package logsanitizer

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestUrlSanitizerHook(t *testing.T) {
	outBuf := &bytes.Buffer{}

	urlSanitizer := NewURLSanitizerHook()
	urlSanitizer.AddPossibleGrpcMethod(
		"UpdateRemoteMirror",
		"CreateRepositoryFromURL",
		"FetchRemote",
	)

	logger := log.New()
	logger.Out = outBuf
	logger.Hooks.Add(urlSanitizer)

	testCases := []struct {
		desc           string
		logFunc        func()
		expectedString string
	}{
		{
			desc: "with args",
			logFunc: func() {
				logger.WithFields(log.Fields{
					"grpc.method": "CreateRepositoryFromURL",
					"args":        []string{"/usr/bin/git", "clone", "--bare", "--", "https://foo_the_user:hUntEr1@gitlab.com/foo/bar", "/home/git/repositories/foo/bar"},
				}).Info("spawn")
			},
			expectedString: "[/usr/bin/git clone --bare -- https://[FILTERED]@gitlab.com/foo/bar /home/git/repositories/foo/bar]",
		},
		{
			desc: "with error",
			logFunc: func() {
				logger.WithFields(log.Fields{
					"grpc.method": "UpdateRemoteMirror",
					"error":       fmt.Errorf("rpc error: code = Unknown desc = remote: Invalid username or password. fatal: Authentication failed for 'https://foo_the_user:hUntEr1@gitlab.com/foo/bar'"),
				}).Error("ERROR")
			},
			expectedString: "rpc error: code = Unknown desc = remote: Invalid username or password. fatal: Authentication failed for 'https://[FILTERED]@gitlab.com/foo/bar'",
		},
		{
			desc: "with message",
			logFunc: func() {
				logger.WithFields(log.Fields{
					"grpc.method": "CreateRepositoryFromURL",
				}).Info("asked for: https://foo_the_user:hUntEr1@gitlab.com/foo/bar")
			},
			expectedString: "asked for: https://[FILTERED]@gitlab.com/foo/bar",
		},
		{
			desc: "with URL without scheme output",
			logFunc: func() {
				logger.WithFields(log.Fields{
					"grpc.method": "FetchRemote",
				}).Info("fatal: unable to look up foo:bar@non-existent.org (port 9418) (nodename nor servname provided, or not known")
			},
			expectedString: "unable to look up [FILTERED]@non-existent.org (port 9418) (nodename nor servname provided, or not known",
		},
		{
			desc: "with gRPC method not added to the list",
			logFunc: func() {
				logger.WithFields(log.Fields{
					"grpc.method": "UserDeleteTag",
				}).Error("fatal: 'https://foo_the_user:hUntEr1@gitlab.com/foo/bar' is not a valid tag name.")
			},
			expectedString: "fatal: 'https://foo_the_user:hUntEr1@gitlab.com/foo/bar' is not a valid tag name.",
		},
		{
			desc: "log with URL that does not require sanitization",
			logFunc: func() {
				logger.WithFields(log.Fields{
					"grpc.method": "CreateRepositoryFromURL",
				}).Info("asked for: https://gitlab.com/gitlab-org/gitaly/v14")
			},
			expectedString: "asked for: https://gitlab.com/gitlab-org/gitaly/v14",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			testCase.logFunc()
			logOutput := outBuf.String()

			require.Contains(t, logOutput, testCase.expectedString)
		})
	}
}

func BenchmarkUrlSanitizerWithoutSanitization(b *testing.B) {
	urlSanitizer := NewURLSanitizerHook()

	logger := log.New()
	logger.Out = io.Discard
	logger.Hooks.Add(urlSanitizer)

	benchmarkLogging(logger, b)
}

func BenchmarkUrlSanitizerWithSanitization(b *testing.B) {
	urlSanitizer := NewURLSanitizerHook()
	urlSanitizer.AddPossibleGrpcMethod(
		"UpdateRemoteMirror",
		"CreateRepositoryFromURL",
	)

	logger := log.New()
	logger.Out = io.Discard
	logger.Hooks.Add(urlSanitizer)

	benchmarkLogging(logger, b)
}

func benchmarkLogging(logger *log.Logger, b *testing.B) {
	for n := 0; n < b.N; n++ {
		logger.WithFields(log.Fields{
			"grpc.method": "CreateRepositoryFromURL",
			"args":        []string{"/usr/bin/git", "clone", "--bare", "--", "https://foo_the_user:hUntEr1@gitlab.com/foo/bar", "/home/git/repositories/foo/bar"},
		}).Info("spawn")
		logger.WithFields(log.Fields{
			"grpc.method": "UpdateRemoteMirror",
			"error":       fmt.Errorf("rpc error: code = Unknown desc = remote: Invalid username or password. fatal: Authentication failed for 'https://foo_the_user:hUntEr1@gitlab.com/foo/bar'"),
		}).Error("ERROR")
		logger.WithFields(log.Fields{
			"grpc.method": "CreateRepositoryFromURL",
		}).Info("asked for: https://foo_the_user:hUntEr1@gitlab.com/foo/bar")
	}
}
