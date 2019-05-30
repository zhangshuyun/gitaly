package cache_test

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/cache"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	helperProcess = "GO_WANT_HELPER_PROCESS"
	getOp         = "get"
	putOp         = "put"
)

// TestHelper allows us to create dedicated processes for testing
// our file locking strategy. The test process will accept command line args to
// determine the course of action. Depending on the operation, STDIN or STDOUT
// may be used. Upon error, exit status 1 will be returned along with error
// messages on STDERR.
// See original inspiration for this test pattern:
// https://npf.io/2015/06/testing-exec-command/
func TestHelper(_ *testing.T) {
	if os.Getenv(helperProcess) != "1" {
		// wan't invoked by function execTestHelper
		return
	}

	var (
		fset   = flag.NewFlagSet("helper", flag.ExitOnError)
		opFlag = fset.String("op", "", "get|put|invalidate")
		root   = fset.String("root", "", "root dir for stream DBs")
		repo   = fset.String("repo", "", "target repo")
		req    = fset.String("req", "", "request to cache")
		method = fset.String("method", "", "grpc method to inject into context")
	)

	for i, arg := range os.Args {
		if arg == "--" {
			err := fset.Parse(os.Args[i+1:])
			if err != nil {
				log.Fatal(err)
			}
			break
		}
	}

	db, err := cache.OpenStreamDB(*root, cache.NaiveKeyer{})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ctx = setMockMethodCtx(ctx, *method)

	switch *opFlag {
	case getOp:
		stream, err := db.GetStream(ctx, mustDecodeRepo(*repo), mustDecodeReq(*req))
		if err != nil {
			log.Fatal(err)
		}

		_, err = io.Copy(os.Stdout, stream)
		if err != nil {
			log.Fatal(err)
		}
	case putOp:
		err := db.PutStream(ctx, mustDecodeRepo(*repo), mustDecodeReq(*req), os.Stdin)
		if err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("invalid op: %s", *opFlag)
	}
}

func mustDecodeRepo(rawPB string) *gitalypb.Repository {
	repo := new(gitalypb.Repository)
	err := proto.Unmarshal([]byte(rawPB), repo)
	if err != nil {
		log.Fatal(err)
	}
	return repo
}

func mustDecodeReq(rawPB string) *gitalypb.InfoRefsRequest {
	req := new(gitalypb.InfoRefsRequest)
	err := proto.Unmarshal([]byte(rawPB), req)
	if err != nil {
		log.Fatal(err)
	}
	return req
}

func execTestHelper(t testing.TB, root, method, op string, stdin io.Reader, repo *gitalypb.Repository, req *gitalypb.InfoRefsRequest) (io.Reader, <-chan error) {
	args := []string{
		"-test.run=TestHelper",
		"--", // no more "go test" args, now our custom args
		"root=" + root,
		"op=" + op,
		"method=" + method,
	}

	switch op {
	case getOp:
		fallthrough
	case putOp:
		rawReq, err := proto.Marshal(repo)
		assert.NoError(t, err)

		rawRepo, err := proto.Marshal(repo)
		assert.NoError(t, err)

		args = append(args, "req="+string(rawReq), "repo="+string(rawRepo))
	default:
		assert.Fail(t, "🤠") // whoa cow poke, are you lost?
	}

	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = []string{helperProcess + "=1"}
	cmd.Stdin = stdin

	pr, pw := io.Pipe()
	cmd.Stdout = pw

	stderrB := new(bytes.Buffer)
	cmd.Stderr = stderrB

	errQ := make(chan error)
	go func() {
		if err := cmd.Run(); err != nil {
			errQ <- errors.New(stderrB.String())
		}
	}()

	return pr, errQ
}

func tempDB(t testing.TB, ck cache.CacheKeyer) (*cache.StreamDB, func()) {
	root, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)

	oldCfg := config.Config
	config.Config.Storages = []config.Storage{
		{
			Name: "default",
			Path: root,
		},
	}

	cleanup := func() {
		lsOutput, err := exec.Command("find", root).Output()
		require.NoError(t, err)
		log.Print(string(lsOutput))
		config.Config = oldCfg
		require.NoError(t, os.RemoveAll(root))
	}

	db, err := cache.OpenStreamDB(root, ck)
	assert.NoError(t, err)

	return db, cleanup
}

func setMockMethodCtx(ctx context.Context, method string) context.Context {
	return grpc.NewContextWithServerTransportStream(ctx, mockServerTransportStream{method})
}

type mockServerTransportStream struct {
	method string
}

func (msts mockServerTransportStream) Method() string             { return msts.method }
func (mockServerTransportStream) SetHeader(md metadata.MD) error  { return nil }
func (mockServerTransportStream) SendHeader(md metadata.MD) error { return nil }
func (mockServerTransportStream) SetTrailer(md metadata.MD) error { return nil }
