package sql_exporter

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/xo/dburl"
)

// TestPrepareAWSIAMBaseURL makes sure our user contract is always like we say.
// What we say to our users:
//   - Don't put a password in the DSN if you use IAM auth
//   - The DSN must have a user
//   - We only support some drivers, and fail fast on the others
//   - Don't put the user, host, port or password as a query parameter
//   - We remove our own parameters and always set the port, the rest of the DSN goes to the driver as it is
func TestPrepareAWSIAMBaseURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dsn     string
		want    string // URL for the driver
		wantErr string
	}{
		{
			name: "no port",
			dsn:  "postgres://monitoring@db.example.com/postgres?sslmode=require&aws-iam-auth=true&aws-iam-region=eu-west-1",
			want: "postgres://monitoring@db.example.com:5432/postgres?sslmode=require",
		},
		{
			name: "custom port",
			dsn:  "postgres://monitoring@db.example.com:6432/postgres?sslmode=require&aws-iam-auth=true",
			want: "postgres://monitoring@db.example.com:6432/postgres?sslmode=require",
		},
		{
			name: "pgx",
			dsn:  "pgx://monitoring@db.example.com/postgres?aws-iam-auth=true",
			want: "pgx://monitoring@db.example.com:5432/postgres",
		},
		{
			// RDS expects the token as cleartext, which the mysql driver refuses to send by default.
			name: "mysql",
			dsn:  "mysql://monitoring@db.example.com/db?tls=true&aws-iam-auth=true",
			want: "mysql://monitoring@db.example.com:3306/db?allowCleartextPasswords=true&tls=true",
		},
		{
			name: "mysql with allowCleartextPasswords",
			dsn:  "mysql://monitoring@db.example.com:3306/db?allowCleartextPasswords=1&aws-iam-auth=true",
			want: "mysql://monitoring@db.example.com:3306/db?allowCleartextPasswords=1",
		},
		{
			name:    "password",
			dsn:     "postgres://monitoring:pass@db.example.com/postgres?aws-iam-auth=true",
			wantErr: "cannot be used with a password",
		},
		{
			name:    "password as a parameter",
			dsn:     "pgx://monitoring@db.example.com/postgres?password=pass&aws-iam-auth=true",
			wantErr: "cannot be used with a password",
		},
		{
			name:    "user as a parameter",
			dsn:     "pgx://nobody@db.example.com/postgres?user=monitoring&aws-iam-auth=true",
			wantErr: "requires the user, host and port in the URL",
		},
		{
			name:    "host as a parameter",
			dsn:     "pgx://monitoring@db.example.com/postgres?host=db2.example.com&aws-iam-auth=true",
			wantErr: "requires the user, host and port in the URL",
		},
		{
			name:    "port as a parameter",
			dsn:     "pgx://monitoring@db.example.com/postgres?port=5433&aws-iam-auth=true",
			wantErr: "requires the user, host and port in the URL",
		},
		{
			name:    "no user",
			dsn:     "postgres://db.example.com/postgres?aws-iam-auth=true",
			wantErr: "requires a user in the DSN",
		},
		{
			name:    "unsupported driver",
			dsn:     "sqlserver://monitoring@db.example.com:1433?aws-iam-auth=true",
			wantErr: "is not supported",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			u, err := dburl.Parse(tt.dsn)
			if err != nil {
				t.Fatal(err)
			}

			base, err := prepareAWSIAMBaseURL(u.Driver, u)
			if err != nil {
				if tt.wantErr == "" || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("got error %v, want one containing %q", err, tt.wantErr)
				}
				return
			}

			// If an error was expected but didn't come, tt.want is empty and this fails.
			if got := base.String(); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// fakeAWSConfig returns an AWS config with fake credentials, so tests don't read the AWS environment of the machine.
func fakeAWSConfig(region string) aws.Config {
	return aws.Config{
		Region: region,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIAEXAMPLE", SecretAccessKey: "secret"}, nil
		}),
	}
}

// TestAWSIAMConnectorNoRegion tests that the AWS region must be set, as the token is signed for it.
func TestAWSIAMConnectorNoRegion(t *testing.T) {
	t.Parallel()

	u, err := dburl.Parse("postgres://monitoring@db.example.com/postgres?aws-iam-auth=true")
	if err != nil {
		t.Fatal(err)
	}
	_, err = newAWSIAMConnector(u.Driver, u, fakeAWSConfig(""))
	if want := "AWS region is not set"; err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("got error %v, want one containing %q", err, want)
	}
}

// fakeDriver is a db driver that records the DSNs passed to Open().
type fakeDriver struct {
	dsns []string
}

func (d *fakeDriver) Open(dsn string) (driver.Conn, error) {
	d.dsns = append(d.dsns, dsn)
	return nil, errors.New("connection refused")
}

// fakeCtxDriver is a fakeDriver that also implements driver.DriverContext, like pgx and mysql do (lib/pq doesn't).
// It is its own driver.Connector and records the contexts passed to Connect().
type fakeCtxDriver struct {
	fakeDriver
	ctxs []context.Context
}

func (d *fakeCtxDriver) OpenConnector(dsn string) (driver.Connector, error) {
	d.dsns = append(d.dsns, dsn)
	return d, nil
}

func (d *fakeCtxDriver) Connect(ctx context.Context) (driver.Conn, error) {
	d.ctxs = append(d.ctxs, ctx)
	return nil, errors.New("connection refused")
}

func (d *fakeCtxDriver) Driver() driver.Driver {
	return d
}

// newTestConnector builds the real connector for the given sql_exporter DSN.
// Only the underlying driver and the AWS config are fake.
func newTestConnector(t *testing.T, dsn string, drv driver.Driver, cfg aws.Config) *awsIAMConnector {
	t.Helper()

	u, err := dburl.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	c, err := newAWSIAMConnector(u.Driver, u, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.driver = drv
	return c
}

// TestAWSIAMConnectorDollarSign tests that a "$" in the DSN is not expanded as an env var a second time.
// The DSN is already expanded once when it's parsed, and dsn() builds it again to put the token in as the password.
func TestAWSIAMConnectorDollarSign(t *testing.T) {
	t.Parallel()

	c := newTestConnector(t, "postgres://mon%24HOME@db.example.com/postgres?aws-iam-auth=true", &fakeDriver{}, fakeAWSConfig("eu-west-1"))
	got, err := c.dsn("TOKEN")
	if err != nil {
		t.Fatal(err)
	}
	if want := "dbname=postgres host=db.example.com password=TOKEN port=5432 user=mon$HOME"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestAWSIAMConnectorContext tests that a driver with driver.DriverContext gets the context of the caller.
// Without the context pgx and mysql would ignore the scrape timeout while connecting.
func TestAWSIAMConnectorContext(t *testing.T) {
	t.Parallel()

	drv := &fakeCtxDriver{}
	c := newTestConnector(t, "pgx://monitoring@db.example.com/postgres?aws-iam-auth=true", drv, fakeAWSConfig("eu-west-1"))

	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "scrape")

	// The fake driver always refuses to connect, we only need the context it was given.
	_, _ = c.Connect(ctx)
	if len(drv.ctxs) != 1 || drv.ctxs[0] != ctx {
		t.Fatalf("got contexts %v, want only the context of the caller", drv.ctxs)
	}
}

// TestAWSIAMConnectorNewToken tests that each new connection generates a new token and gives it to the driver.
// Connect calls the driver in two different ways, so we test both.
//
// It also tests what the token is signed for, if one of them is wrong RDS rejects the token:
//   - endpoint: host and port from the DSN (here the default port, as the DSN has none)
//   - user: from the DSN
//   - region: from the AWS config
func TestAWSIAMConnectorNewToken(t *testing.T) {
	t.Parallel()

	plain, withCtx := &fakeDriver{}, &fakeCtxDriver{}
	tests := []struct {
		name string
		drv  driver.Driver
		rec  *fakeDriver // records the DSNs that drv gets
	}{
		{name: "driver without context (lib/pq)", drv: plain, rec: plain},
		{name: "driver with context (pgx, mysql)", drv: withCtx, rec: &withCtx.fakeDriver},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// A token is signed with the credentials, so we count how many times they are asked for.
			var tokens int
			cfg := fakeAWSConfig("eu-west-1")
			creds := cfg.Credentials
			cfg.Credentials = aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				tokens++
				return creds.Retrieve(ctx)
			})

			db := sql.OpenDB(newTestConnector(t, "postgres://monitoring@db.example.com/postgres?aws-iam-auth=true", tt.drv, cfg))
			defer db.Close()

			// The driver refuses to connect, so the pool stays empty and every ping has to open a new connection.
			for range 2 {
				_ = db.PingContext(context.Background())
			}
			if tokens != 2 || len(tt.rec.dsns) != 2 {
				t.Fatalf("got %d tokens and %d connection attempts, want 2 of each", tokens, len(tt.rec.dsns))
			}
			// A token starts with the endpoint and the user, the region is in the credential scope.
			wants := []string{
				"password=db.example.com:5432?Action=connect&DBUser=monitoring&",
				"%2Feu-west-1%2Frds-db%2Faws4_request",
			}
			for _, dsn := range tt.rec.dsns {
				for _, want := range wants {
					if !strings.Contains(dsn, want) {
						t.Errorf("got DSN %q, want it to have a token with %q", dsn, want)
					}
				}
			}
		})
	}
}

// TestAWSIAMConnectorNoCredentials tests that we don't try to connect without a token, for example when the AWS
// environment has no credentials or they are expired.
func TestAWSIAMConnectorNoCredentials(t *testing.T) {
	t.Parallel()

	cfg := fakeAWSConfig("eu-west-1")
	cfg.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{}, errors.New("no credentials")
	})

	drv := &fakeDriver{}
	c := newTestConnector(t, "postgres://monitoring@db.example.com/postgres?aws-iam-auth=true", drv, cfg)

	// The fake driver always refuses to connect, so err is never nil here.
	_, err := c.Connect(context.Background())
	if !strings.Contains(err.Error(), "failed to build AWS IAM auth token") {
		t.Fatalf("got error %v, want the token error", err)
	}
	if len(drv.dsns) != 0 {
		t.Fatalf("got %d connection attempts, want 0", len(drv.dsns))
	}
}
