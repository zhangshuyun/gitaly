package starter

import (
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v13/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/v13/internal/connectioncounter"
)

const (
	// TCP is the prefix for tcp
	TCP string = "tcp"
	// TLS is the prefix for tls
	TLS string = "tls"
	// Unix is the prefix for unix
	Unix string = "unix"

	separator = "://"
)

var (
	// ErrEmptySchema signals that the address has no schema in it.
	ErrEmptySchema  = errors.New("empty schema can't be used")
	errEmptyAddress = errors.New("empty address can't be used")
)

// ParseEndpoint returns Config based on the passed in address string.
// Returns error only if provided endpoint has no schema or address defined.
func ParseEndpoint(endpoint string) (Config, error) {
	if endpoint == "" {
		return Config{}, errEmptyAddress
	}

	parts := strings.Split(endpoint, separator)
	if len(parts) != 2 {
		return Config{}, fmt.Errorf("unsupported format: %q: %w", endpoint, ErrEmptySchema)
	}

	if err := verifySchema(parts[0]); err != nil {
		return Config{}, err
	}

	if parts[1] == "" {
		return Config{}, errEmptyAddress
	}
	return Config{Name: parts[0], Addr: parts[1]}, nil
}

// ComposeEndpoint returns address string composed from provided schema and schema-less address.
func ComposeEndpoint(schema, address string) (string, error) {
	if address == "" {
		return "", errEmptyAddress
	}

	if err := verifySchema(schema); err != nil {
		return "", err
	}

	return schema + separator + address, nil
}

func verifySchema(schema string) error {
	switch schema {
	case "":
		return ErrEmptySchema
	case TCP, TLS, Unix:
		return nil
	default:
		return fmt.Errorf("unsupported schema: %q", schema)
	}
}

// Config represents a network type, and address
type Config struct {
	Name, Addr string
}

// Endpoint returns fully qualified address.
func (c *Config) Endpoint() (string, error) {
	return ComposeEndpoint(c.Name, c.Addr)
}

func (c *Config) isSecure() bool {
	return c.Name == TLS
}

func (c *Config) family() string {
	if c.isSecure() {
		return TCP
	}

	return c.Name
}

// New creates a new bootstrap.Starter from a config and a GracefulStoppableServer
func New(cfg Config, servers GracefulStoppableServer) bootstrap.Starter {
	return func(listen bootstrap.ListenFunc, errCh chan<- error) error {
		l, err := listen(cfg.family(), cfg.Addr)
		if err != nil {
			return err
		}

		logrus.WithField("address", cfg.Addr).Infof("listening at %s address", cfg.Name)
		l = connectioncounter.New(cfg.Name, l)

		go func() {
			errCh <- servers.Serve(l, cfg.isSecure())
		}()

		return nil
	}
}

// GracefulStoppableServer allows to serve contents on a net.Listener, Stop serving and performing a GracefulStop
type GracefulStoppableServer interface {
	GracefulStop()
	Stop()
	Serve(l net.Listener, secure bool) error
}
