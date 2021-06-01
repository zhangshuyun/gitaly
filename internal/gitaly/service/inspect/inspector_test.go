package inspect

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestWrite(t *testing.T) {
	for _, tc := range []struct {
		desc      string
		action    func(io.Reader)
		src       io.Reader
		exp       []byte
		expErrStr string
	}{
		{
			desc: "all data consumed without errors",
			action: func(reader io.Reader) {
				data, err := ioutil.ReadAll(reader)
				require.NoError(t, err)
				require.Equal(t, []byte("somedata"), data)
			},
			src: strings.NewReader("somedata"),
			exp: []byte("somedata"),
		},
		{
			desc: "no data is ok",
			action: func(reader io.Reader) {
				data, err := ioutil.ReadAll(reader)
				require.NoError(t, err)
				require.Empty(t, data)
			},
			src: bytes.NewReader(nil),
			exp: []byte{},
		},
		{
			desc: "consumed by action partially",
			action: func(reader io.Reader) {
				b := make([]byte, 4)
				n, err := reader.Read(b)
				require.NoError(t, err)
				require.Equal(t, 4, n)
				require.Equal(t, []byte("some"), b)
			},
			src: strings.NewReader("somedata"),
			exp: []byte("somedata"),
		},
		{
			desc:      "error on read",
			action:    func(reader io.Reader) {},
			src:       errReader("bad read"),
			exp:       []byte{},
			expErrStr: "bad read",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			mainWriter := &bytes.Buffer{}
			var checked int32

			writer := NewWriter(mainWriter, func(reader io.Reader) {
				tc.action(reader)
				atomic.StoreInt32(&checked, 1)
			})

			_, err := io.Copy(writer, tc.src)
			if tc.expErrStr != "" {
				require.EqualError(t, err, tc.expErrStr)
			} else {
				require.NoError(t, err)
			}

			data, err := ioutil.ReadAll(mainWriter)
			require.NoError(t, err)
			require.Equal(t, tc.exp, data)

			require.NoError(t, writer.Close())
			require.Equal(t, int32(1), atomic.LoadInt32(&checked))
		})
	}
}

type errReader string

func (e errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New(string(e))
}

func TestLogPackInfoStatistic(t *testing.T) {
	dest := &bytes.Buffer{}
	log := &logrus.Logger{
		Out:       dest,
		Formatter: &logrus.JSONFormatter{},
		Level:     logrus.InfoLevel,
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx = ctxlogrus.ToContext(ctx, log.WithField("test", "logging"))

	logging := LogPackInfoStatistic(ctx)
	logging(strings.NewReader("0038\x41ACK 1e292f8fedd741b75372e19097c76d327140c312 ready\n0035\x02Total 1044 (delta 519), reused 1035 (delta 512)\n0038\x41ACK 1e292f8fedd741b75372e19097c76d327140c312 ready\n0000\x01"))

	require.Contains(t, dest.String(), "Total 1044 (delta 519), reused 1035 (delta 512)")
	require.NotContains(t, dest.String(), "ACK 1e292f8fedd741b75372e19097c76d327140c312")
}
