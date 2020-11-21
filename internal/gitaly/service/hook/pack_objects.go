package hook

import (
	"bufio"
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
	_ "gocloud.dev/blob/gcsblob"
)

var packCache *cache
var hello *log.Logger

const chunkSize = 20 * 1000 * 1000

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
	ctx := stream.Context()
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

		var err error
		iErr, err = e.SendErr(ctx, stderr, iErr)
		if err != nil {
			return err
		}

		iOut, err = e.SendOut(ctx, stdout, iOut)
		if err != nil {
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
	nextEntryID int
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
	e := &entry{
		c:   c,
		key: key,
		id:  c.nextEntryID,
	}
	e.Cond = sync.NewCond(e)
	c.nextEntryID++
	return e
}

type entry struct {
	c   *cache
	key string
	sync.Mutex
	*sync.Cond
	done             bool
	err              error
	stderr           []byte
	nOutChunks       int
	currentChunkSize int
	outWriter        *blob.Writer
	id               int
}

func (e *entry) CloseWriter() error {
	e.Lock()
	defer e.Unlock()

	if e.outWriter == nil {
		return nil
	}

	if err := e.outWriter.Close(); err != nil {
		return err
	}

	e.outWriter = nil
	e.nOutChunks++
	e.currentChunkSize = 0
	e.Broadcast()
	return nil
}

func (e *entry) chunkKey(i int) string {
	chunk := fmt.Sprintf("%02x", i)
	return fmt.Sprintf("%s/%s/%s.%d", e.key, chunk[len(chunk)-2:], chunk, e.id)
}

func (e *entry) delete() {
	e.c.Lock()
	if e.c.entries[e.key] == e {
		delete(e.c.entries, e.key)
	}
	e.c.Unlock()
}

func (e *entry) NOut() int { return e.nOutChunks }
func (e *entry) NErr() int { return len(e.stderr) }

func (e *entry) SendOut(ctx context.Context, w io.Writer, pos int) (int, error) {
	e.Lock()
	nOut := e.NOut()
	e.Unlock()

	for i := pos; i < nOut; i++ {
		r, err := e.c.Bucket.NewReader(ctx, e.chunkKey(i), nil)
		if err != nil {
			return i, err
		}
		defer r.Close()

		if _, err := io.Copy(w, r); err != nil {
			return i, err
		}
	}
	return nOut, nil
}

func (e *entry) SendErr(ctx context.Context, w io.Writer, pos int) (int, error) {
	e.Lock()
	buf := e.stderr
	e.Unlock()
	if pos > len(buf) {
		return 0, errors.New("stderrt: invalid pos")
	}

	n, err := w.Write(buf[pos:])
	return pos + n, err
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

	cacheURL := "file:///tmp/gitaly-cache"
	if v := os.Getenv("GITALY_CACHE_BUCKET"); v != "" {
		cacheURL = v
	}

	var err error
	c.Bucket, err = blob.OpenBucket(context.Background(), cacheURL)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (e *entry) generateResponse(repoPath string, args []string, stdin []byte, stdout io.Writer, stderr io.Writer) {
	err := func() error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stdout := bufio.NewWriter(WriterFunc(e.WriteStdout))
		cmd, err := command.New(
			ctx,
			exec.Command("git", append([]string{"-C", repoPath}, args...)...),
			bytes.NewReader(stdin),
			stdout,
			WriterFunc(e.WriteStderr),
		)
		if err != nil {
			return err
		}

		if err := cmd.Wait(); err != nil {
			return err
		}
		if err := stdout.Flush(); err != nil {
			return err
		}

		return e.CloseWriter()
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
	w, err := e.getOutWriter()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(p)
	if err != nil {
		return n, err
	}

	e.Lock()
	e.currentChunkSize += n
	e.Unlock()

	if e.chunkIsFull() {
		return n, e.CloseWriter()
	}

	return n, nil
}

func (e *entry) getOutWriter() (io.Writer, error) {
	e.Lock()
	defer e.Unlock()
	if e.outWriter == nil {
		var err error
		e.outWriter, err = e.c.Bucket.NewWriter(context.Background(), e.chunkKey(e.nOutChunks), nil)
		if err != nil {
			return nil, err
		}
	}
	return e.outWriter, nil
}

func (e *entry) chunkIsFull() bool {
	e.Lock()
	defer e.Unlock()
	return e.currentChunkSize > chunkSize
}
