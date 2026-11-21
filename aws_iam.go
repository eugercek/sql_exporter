package sql_exporter

import (
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/xo/dburl"
)

const (
	// awsIAMParamAuth is the DSN parameter that enables AWS IAM authentication when set to "true".
	awsIAMParamAuth = "aws-iam-auth"
	// awsIAMParamRegion is the optional DSN parameter for the region of the database, which the token is signed
	// for. It defaults to the region of the AWS environment.
	awsIAMParamRegion = "aws-iam-region"
)

// awsIAMDefaultPorts lists the drivers that support AWS IAM authentication with their default ports.
// The port is needed because it's part of the signed token.
var awsIAMDefaultPorts = map[string]string{
	"mysql":    "3306",
	"pgx":      "5432",
	"postgres": "5432",
}

// openWithAWSIAM opens a DB handle that authenticates with AWS IAM, using the default AWS credential chain.
func openWithAWSIAM(ctx context.Context, driverName string, dbURL *dburl.URL) (*sql.DB, error) {
	dsnRegion := dbURL.Query().Get(awsIAMParamRegion)
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(dsnRegion))
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS config: %w", err)
	}

	c, err := newAWSIAMConnector(driverName, dbURL, cfg)
	if err != nil {
		return nil, err
	}

	return sql.OpenDB(c), nil
}

// newAWSIAMConnector checks the DSN and builds the connector.
// It takes aws.Config as a parameter, so tests don't depend on the AWS environment of the machine.
func newAWSIAMConnector(driverName string, dbURL *dburl.URL, cfg aws.Config) (*awsIAMConnector, error) {
	base, err := prepareAWSIAMBaseURL(driverName, dbURL)
	if err != nil {
		return nil, err
	}

	// database/sql can't look up a driver by name (golang/go#12600), so we open a throwaway handle to get the driver.
	// The handle never connects. It gets the same DSN as the real connections, only without the token.
	parsed, err := dburl.Parse(base.String())
	if err != nil {
		return nil, err
	}
	driverDB, err := sql.Open(driverName, parsed.DSN)
	if err != nil {
		return nil, err
	}
	defer driverDB.Close()

	// The signed token must have a region, either from the DSN or from the AWS environment.
	if cfg.Region == "" {
		return nil, fmt.Errorf("AWS region is not set, use the %s parameter", awsIAMParamRegion)
	}

	return &awsIAMConnector{
		driver:   driverDB.Driver(),
		url:      base,
		endpoint: base.Host,
		region:   cfg.Region,
		user:     base.User.Username(),
		creds:    cfg.Credentials,
	}, nil
}

// prepareAWSIAMBaseURL does the "dirty work" of turning the DSN into the URL for the actual driver.
// It's a separate function to keep newAWSIAMConnector simple.
func prepareAWSIAMBaseURL(driverName string, dbURL *dburl.URL) (url.URL, error) {
	defaultPort, supported := awsIAMDefaultPorts[driverName]
	if !supported {
		return url.URL{}, fmt.Errorf("%s is not supported for the %q driver", awsIAMParamAuth, driverName)
	}

	// Strip the parameters that are only meant for sql_exporter, the drivers would fail to connect with them.
	q := dbURL.Query()
	q.Del(awsIAMParamAuth)
	q.Del(awsIAMParamRegion)

	// A password can also be given as a query parameter (libpq style), which pgx would use instead of the token.
	if _, hasPassword := dbURL.User.Password(); hasPassword || q.Has("password") {
		return url.URL{}, fmt.Errorf("%s cannot be used with a password in the DSN", awsIAMParamAuth)
	}
	// pgx also lets these parameters override the URL, but the token is signed for the user, host and port of the URL.
	if q.Has("user") || q.Has("host") || q.Has("port") {
		return url.URL{}, fmt.Errorf("%s requires the user, host and port in the URL, not as parameters", awsIAMParamAuth)
	}
	// Without a user the drivers fall back to their own default (like the OS user), which the token isn't signed for.
	if dbURL.User.Username() == "" {
		return url.URL{}, fmt.Errorf("%s requires a user in the DSN", awsIAMParamAuth)
	}

	// By default the mysql driver refuses to send cleartext passwords, but RDS expects the token as cleartext, not hashed.
	if driverName == "mysql" && !q.Has("allowCleartextPasswords") {
		q.Set("allowCleartextPasswords", "true")
	}

	base := dbURL.URL
	// The token is signed for the port, so we always write it out for the driver to connect to the same one.
	base.Host = net.JoinHostPort(dbURL.Hostname(), cmp.Or(dbURL.Port(), defaultPort))
	base.RawQuery = q.Encode()
	return base, nil
}

// awsIAMConnector is a driver.Connector that uses a new token as the password for every connection, as tokens expire.
type awsIAMConnector struct {
	driver driver.Driver
	url    url.URL // connection URL without the password

	// The token is signed for the endpoint (host:port), region and user of the database.
	endpoint, region, user string
	creds                  aws.CredentialsProvider
}

func (c *awsIAMConnector) Connect(ctx context.Context) (driver.Conn, error) {
	token, err := auth.BuildAuthToken(ctx, c.endpoint, c.region, c.user, c.creds)
	if err != nil {
		return nil, fmt.Errorf("failed to build AWS IAM auth token: %w", err)
	}

	dsn, err := c.dsn(token)
	if err != nil {
		return nil, err
	}

	// Now the DSN has a fresh token as the password, we only need to call the underlying driver

	// We're mimicking sql.Open() https://github.com/golang/go/blob/e606b442f2fca5c97603449e7ade0100118b10c0/src/database/sql/sql.go#L876-L884
	// If the underlying driver supports context we should use it, otherwise the context  is ignored while connecting
	if dc, ok := c.driver.(driver.DriverContext); ok {
		connector, err := dc.OpenConnector(dsn)
		if err != nil {
			return nil, err
		}
		return connector.Connect(ctx)
	}

	// The driver doesn't support driver.DriverContext, so we use Open()
	return c.driver.Open(dsn)
}

// dsn returns the DSN for the underlying driver, with the token as the password.
func (c *awsIAMConnector) dsn(token string) (string, error) {
	// Can't use safeParse because we already expanded env vars.
	// For example: safeParse("mon%24HOME") -> "mon$HOME", another safeParse(...) would expand $HOME.
	u := c.url
	u.User = url.UserPassword(c.user, token)
	parsed, err := dburl.Parse(u.String())
	if err != nil {
		return "", errors.New("invalid URL") // like safeParse, omit the error because it may contain the token
	}
	return parsed.DSN, nil
}

func (c *awsIAMConnector) Driver() driver.Driver {
	return c.driver
}
