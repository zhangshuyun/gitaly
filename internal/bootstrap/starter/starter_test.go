package starter

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsSecure(t *testing.T) {
	for _, test := range []struct {
		name   string
		secure bool
	}{
		{"tcp", false},
		{"unix", false},
		{"tls", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := Config{Name: test.name}
			require.Equal(t, test.secure, conf.IsSecure())
		})
	}
}

func TestFamily(t *testing.T) {
	for _, test := range []struct {
		name, family string
	}{
		{"tcp", "tcp"},
		{"unix", "unix"},
		{"tls", "tcp"},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := Config{Name: test.name}
			require.Equal(t, test.family, conf.family())
		})
	}
}

func TestComposeEndpoint(t *testing.T) {
	for _, tc := range []struct {
		desc   string
		schema string
		addr   string
		exp    string
		expErr error
	}{
		{
			desc:   "no addresses",
			schema: TCP,
			addr:   "",
			expErr: errors.New("empty address can't be used"),
		},
		{
			desc:   "incorrect schema",
			schema: "bad",
			addr:   "127.0.0.1:2306",
			expErr: errors.New(`unsupported schema: "bad"`),
		},
		{
			desc:   "no schema",
			addr:   "127.0.0.1:2306",
			schema: "",
			expErr: errors.New("empty schema can't be used"),
		},
		{
			desc:   "tcp schema addresses",
			schema: TCP,
			addr:   "127.0.0.1:2306",
			exp:    "tcp://127.0.0.1:2306",
		},
		{
			desc:   "tls schema addresses",
			schema: TLS,
			addr:   "127.0.0.1:2306",
			exp:    "tls://127.0.0.1:2306",
		},
		{
			desc:   "unix schema addresses",
			schema: Unix,
			addr:   "/path/to/socket",
			exp:    "unix:///path/to/socket",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			actual, err := ComposeEndpoint(tc.schema, tc.addr)
			require.Equal(t, tc.expErr, err)
			require.Equal(t, tc.exp, actual)
		})
	}
}

func TestParseEndpoint(t *testing.T) {
	for _, tc := range []struct {
		desc   string
		addr   string
		exp    Config
		expErr error
	}{
		{
			desc:   "no addresses",
			expErr: errEmptyAddress,
		},
		{
			desc:   "incorrect schema",
			addr:   "bad://127.0.0.1:2306",
			expErr: errors.New(`unsupported schema: "bad"`),
		},
		{
			desc:   "no schema",
			addr:   "://127.0.0.1:2306",
			expErr: ErrEmptySchema,
		},
		{
			desc:   "bad format",
			addr:   "127.0.0.1:2306",
			expErr: fmt.Errorf(`unsupported format: "127.0.0.1:2306": %w`, ErrEmptySchema),
		},
		{
			desc: "tcp schema addresses",
			addr: "tcp://127.0.0.1:2306",
			exp:  Config{Name: TCP, Addr: "127.0.0.1:2306"},
		},
		{
			desc: "tls schema addresses",
			addr: "tls://127.0.0.1:2306",
			exp:  Config{Name: TLS, Addr: "127.0.0.1:2306"},
		},
		{
			desc: "unix schema addresses",
			addr: "unix:///path/to/socket",
			exp:  Config{Name: Unix, Addr: "/path/to/socket"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			actual, err := ParseEndpoint(tc.addr)
			require.Equal(t, tc.expErr, err)
			require.Equal(t, tc.exp, actual)
		})
	}
}

func TestConfig_Endpoint(t *testing.T) {
	for _, tc := range []struct {
		desc   string
		conf   Config
		exp    string
		expErr error
	}{
		{
			desc:   "no address",
			conf:   Config{Name: TCP},
			expErr: errors.New("empty address can't be used"),
		},
		{
			desc:   "no schema",
			conf:   Config{Addr: "localhost"},
			expErr: errors.New("empty schema can't be used"),
		},
		{
			desc:   "invalid schema",
			conf:   Config{Name: "invalid", Addr: "localhost"},
			expErr: errors.New(`unsupported schema: "invalid"`),
		},
		{
			desc: "unix",
			conf: Config{Name: Unix, Addr: "/var/opt/some"},
			exp:  "unix:///var/opt/some",
		},
		{
			desc: "tcp",
			conf: Config{Name: TCP, Addr: "localhost:1234"},
			exp:  "tcp://localhost:1234",
		},
		{
			desc: "tls",
			conf: Config{Name: TLS, Addr: "localhost:4321"},
			exp:  "tls://localhost:4321",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			actual, err := tc.conf.Endpoint()
			require.Equal(t, tc.expErr, err)
			require.Equal(t, tc.exp, actual)
		})
	}
}
