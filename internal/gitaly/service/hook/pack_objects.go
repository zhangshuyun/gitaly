package hook

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"sync"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
)

var packCache *cache
var hello *log.Logger

func init() {
	var err error
	packCache, err = newCache()
	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile("/tmp/hello", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	hello = log.New(f, "", log.LstdFlags)
}

func (s *server) PackObjectsHook(stream gitalypb.HookService_PackObjectsHookServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return helper.ErrInternal(err)
	}

	repoPath, err := helper.GetRepoPath(firstRequest.Repository)
	if err != nil {
		return err
	}

	stdin, err := ioutil.ReadAll(streamio.NewReader(func() ([]byte, error) {
		req, err := stream.Recv()
		return req.GetStdin(), err
	}))
	if err != nil {
		return err
	}

	var m sync.Mutex
	stdout := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.PackObjectsHookResponse{Stdout: p})
	})
	stderr := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.PackObjectsHookResponse{Stderr: p})
	})

	var e *entry
	key := packCache.key(firstRequest.Args, stdin)
	packCache.Lock()
	e = packCache.entries[key]
	if e == nil {
		e = packCache.newEntry(key)
		packCache.entries[key] = e
		go e.generateResponse(repoPath, firstRequest.Args, stdin, stdout, stderr)
	}
	packCache.Unlock()

	var iErr, iOut int
	for done := false; !done; {
		e.Lock()
		for !e.done && iErr == e.NErr() && iOut == e.NOut() {
			e.Wait()
		}
		done = e.done
		e.Unlock()

		if err := e.SendErr(stderr, &iErr); err != nil {
			return err
		}

		if err := e.SendOut(stdout, &iOut); err != nil {
			return err
		}
	}

	return e.err
}

type WriterFunc func([]byte) (int, error)

func (f WriterFunc) Write(p []byte) (int, error) { return f(p) }

type cache struct {
	*blob.Bucket
	cacheID string
	entries map[string]*entry
	sync.Mutex
}

func (c *cache) key(args []string, stdin []byte) string {
	h := sha256.New()
	h.Write([]byte(c.cacheID))
	fmt.Fprintf(h, "%d\x00", len(args))
	for _, a := range args {
		fmt.Fprintf(h, "%s\x00", a)
	}
	h.Write(stdin)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (c *cache) newEntry(key string) *entry {
	e := &entry{c: c, key: key}
	e.Cond = sync.NewCond(e)
	return e
}

type entry struct {
	c   *cache
	key string
	sync.Mutex
	*sync.Cond
	done   bool
	err    error
	stderr []byte
	stdout []byte
}

func (e *entry) delete() {
	e.c.Lock()
	if e.c.entries[e.key] == e {
		delete(e.c.entries, e.key)
	}
	e.c.Unlock()
}

func (e *entry) NOut() int { return len(e.stdout) }
func (e *entry) NErr() int { return len(e.stderr) }

func (e *entry) SendOut(w io.Writer, pos *int) error {
	e.Lock()
	buf := e.stdout
	e.Unlock()
	if *pos > len(buf) {
		return errors.New("stdout: invalid pos")
	}
	n, err := w.Write(buf[*pos:])
	*pos += n
	return err
}

func (e *entry) SendErr(w io.Writer, pos *int) error {
	e.Lock()
	buf := e.stderr
	e.Unlock()
	if *pos > len(buf) {
		return errors.New("stderrt: invalid pos")
	}
	n, err := w.Write(buf[*pos:])
	*pos += n
	return err
}

func newCache() (*cache, error) {
	c := &cache{
		entries: make(map[string]*entry),
	}
	buf := make([]byte, 8)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return nil, err
	}
	c.cacheID = fmt.Sprintf("%x", buf)

	var err error
	c.Bucket, err = blob.OpenBucket(context.Background(), "file:///tmp/gitaly-cache")
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (e *entry) generateResponse(repoPath string, args []string, stdin []byte, stdout io.Writer, stderr io.Writer) {
	err := func() error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cmd, err := command.New(
			ctx,
			exec.Command("git", append([]string{"-C", repoPath}, args...)...),
			bytes.NewReader(stdin),
			WriterFunc(e.WriteStdout),
			WriterFunc(e.WriteStderr),
		)
		if err != nil {
			return err
		}

		return cmd.Wait()
	}()

	e.Lock()
	e.done = true
	e.err = err
	if err != nil {
		e.delete()
	}
	e.Unlock()
	e.Broadcast()
}

func (e *entry) WriteStderr(p []byte) (int, error) {
	e.Lock()
	e.stderr = append(e.stderr, p...)
	e.Unlock()
	e.Broadcast()
	return len(p), nil
}

func (e *entry) WriteStdout(p []byte) (int, error) {
	e.Lock()
	e.stdout = append(e.stdout, p...)
	e.Unlock()
	e.Broadcast()
	return len(p), nil
}
