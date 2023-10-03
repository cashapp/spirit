package dbconn

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/squareup/spirit/pkg/utils"
)

const (
	rdsTLSConfigName = "rds"
	maxConnLifetime  = time.Minute * 3
)

// rdsAddr matches Amazon RDS hostnames with optional :port suffix.
// It's used to automatically load the Amazon RDS CA and enable TLS
var (
	rdsAddr = regexp.MustCompile(`rds\.amazonaws\.com(:\d+)?$`)
	once    sync.Once
	// rds-ca-2019-root.pem
	rds2019rootCA = []byte(`-----BEGIN CERTIFICATE-----
MIIEBjCCAu6gAwIBAgIJAMc0ZzaSUK51MA0GCSqGSIb3DQEBCwUAMIGPMQswCQYD
VQQGEwJVUzEQMA4GA1UEBwwHU2VhdHRsZTETMBEGA1UECAwKV2FzaGluZ3RvbjEi
MCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNlcywgSW5jLjETMBEGA1UECwwKQW1h
em9uIFJEUzEgMB4GA1UEAwwXQW1hem9uIFJEUyBSb290IDIwMTkgQ0EwHhcNMTkw
ODIyMTcwODUwWhcNMjQwODIyMTcwODUwWjCBjzELMAkGA1UEBhMCVVMxEDAOBgNV
BAcMB1NlYXR0bGUxEzARBgNVBAgMCldhc2hpbmd0b24xIjAgBgNVBAoMGUFtYXpv
biBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMxIDAeBgNV
BAMMF0FtYXpvbiBSRFMgUm9vdCAyMDE5IENBMIIBIjANBgkqhkiG9w0BAQEFAAOC
AQ8AMIIBCgKCAQEArXnF/E6/Qh+ku3hQTSKPMhQQlCpoWvnIthzX6MK3p5a0eXKZ
oWIjYcNNG6UwJjp4fUXl6glp53Jobn+tWNX88dNH2n8DVbppSwScVE2LpuL+94vY
0EYE/XxN7svKea8YvlrqkUBKyxLxTjh+U/KrGOaHxz9v0l6ZNlDbuaZw3qIWdD/I
6aNbGeRUVtpM6P+bWIoxVl/caQylQS6CEYUk+CpVyJSkopwJlzXT07tMoDL5WgX9
O08KVgDNz9qP/IGtAcRduRcNioH3E9v981QO1zt/Gpb2f8NqAjUUCUZzOnij6mx9
McZ+9cWX88CRzR0vQODWuZscgI08NvM69Fn2SQIDAQABo2MwYTAOBgNVHQ8BAf8E
BAMCAQYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUc19g2LzLA5j0Kxc0LjZa
pmD/vB8wHwYDVR0jBBgwFoAUc19g2LzLA5j0Kxc0LjZapmD/vB8wDQYJKoZIhvcN
AQELBQADggEBAHAG7WTmyjzPRIM85rVj+fWHsLIvqpw6DObIjMWokpliCeMINZFV
ynfgBKsf1ExwbvJNzYFXW6dihnguDG9VMPpi2up/ctQTN8tm9nDKOy08uNZoofMc
NUZxKCEkVKZv+IL4oHoeayt8egtv3ujJM6V14AstMQ6SwvwvA93EP/Ug2e4WAXHu
cbI1NAbUgVDqp+DRdfvZkgYKryjTWd/0+1fS8X1bBZVWzl7eirNVnHbSH2ZDpNuY
0SBd8dj5F6ld3t58ydZbrTHze7JJOd8ijySAp4/kiu9UfZWuTPABzDa/DSdz9Dk/
zPW4CXXvhLmE02TA9/HeCw3KEHIwicNuEfw=
-----END CERTIFICATE-----`)
)

func IsRDSHost(host string) bool {
	return rdsAddr.MatchString(host)
}

func NewTLSConfig() *tls.Config {
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(rds2019rootCA)
	return &tls.Config{RootCAs: caCertPool}
}

func initRDSTLS() error {
	var err error
	once.Do(func() {
		err = mysql.RegisterTLSConfig(rdsTLSConfigName, NewTLSConfig())
	})
	return err
}

// newDSN returns a new DSN to be used to connect to MySQL.
// It accepts a DSN as input and appends TLS configuration
// if the host is an Amazon RDS hostname.
func newDSN(dsn string, config *DBConfig) (string, error) {
	var ops []string
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	if IsRDSHost(cfg.Addr) {
		if err = initRDSTLS(); err != nil {
			return "", err
		}
		ops = append(ops, fmt.Sprintf("%s=%s", "tls", url.QueryEscape(rdsTLSConfigName)))
	}

	// Setting sql_mode looks ill-advised, but unfortunately it's required.
	// A user might have set their SQL mode to empty even if the
	// server has it enabled. After they've inserted data,
	// we need to be able to produce the same when copying.
	// If you look at standard packages like wordpress, drupal etc.
	// they all change the SQL mode. If you look at mysqldump, etc.
	// they all unset the SQL mode just like this.
	ops = append(ops, fmt.Sprintf("%s=%s", "sql_mode", url.QueryEscape(`""`)))
	ops = append(ops, fmt.Sprintf("%s=%s", "time_zone", url.QueryEscape(`"+00:00"`)))
	ops = append(ops, fmt.Sprintf("%s=%s", "innodb_lock_wait_timeout", url.QueryEscape(fmt.Sprint(config.InnodbLockWaitTimeout))))
	ops = append(ops, fmt.Sprintf("%s=%s", "lock_wait_timeout", url.QueryEscape(fmt.Sprint(config.LockWaitTimeout))))
	if config.Aurora20Compatible {
		ops = append(ops, fmt.Sprintf("%s=%s", "tx_isolation", url.QueryEscape(`"read-committed"`))) // Aurora 2.0
	} else {
		ops = append(ops, fmt.Sprintf("%s=%s", "transaction_isolation", url.QueryEscape(`"read-committed"`))) // MySQL 8.0
	}
	// go driver options, should set:
	// character_set_client, character_set_connection, character_set_results
	ops = append(ops, fmt.Sprintf("%s=%s", "charset", "binary"))
	ops = append(ops, fmt.Sprintf("%s=%s", "collation", "binary"))
	dsn = fmt.Sprintf("%s?%s", dsn, strings.Join(ops, "&"))
	return dsn, nil
}

// New is similar to sql.Open except we take the inputDSN and
// append additional options to it to standardize the connection.
// It will also ping the connection to ensure it is valid.
func New(inputDSN string, config *DBConfig) (*sql.DB, error) {
	dsn, err := newDSN(inputDSN, config)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		utils.ErrInErr(db.Close())
		if config.Aurora20Compatible {
			return nil, err // Already tried it, didn't work.
		}
		// This could be because of transaction_isolation vs. tx_isolation.
		// Try changing to Aurora 2.0 compatible mode and retrying.
		// because config is a pointer, it will update future calls too.
		config.Aurora20Compatible = true
		return New(inputDSN, config)
	}
	db.SetMaxOpenConns(config.MaxOpenConnections)
	db.SetConnMaxLifetime(maxConnLifetime)
	return db, nil
}
