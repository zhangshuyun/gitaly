package hook

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
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

const chunkSize = 50 * 1000 * 1000

func init() {
	var err error
	packCache, err = newCache("file:///tmp/gitaly-cache")
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

	stdinHandoff := false
	stdin, err := readPackObjectsStdin(stream)
	if err != nil {
		return err
	}
	defer func() {
		if !stdinHandoff {
			stdin.Close()
		}
	}()

	var m sync.Mutex
	stdout := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.PackObjectsHookResponse{Stdout: p})
	})
	stderr := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.PackObjectsHookResponse{Stderr: p})
	})

	var e *entry
	key, err := packCache.key(repoPath, firstRequest.Args, stdin)
	if err != nil {
		return err
	}

	packCache.Lock()
	e = packCache.entries[key]
	if e == nil {
		e = packCache.newEntry(key)
		packCache.entries[key] = e
		stdinHandoff = true
		go e.fill(repoPath, firstRequest.Args, stdin, stdout, stderr)
	}
	packCache.Unlock()

	var posErr, posOut int
	for done := false; !done; {
		e.Lock()
		for !e.done && posErr == e.stderr.len() && posOut == e.stdout.len() {
			e.Wait()
		}
		done = e.done
		e.Unlock()

		if err := e.stderr.send(ctx, stderr, &posErr); err != nil {
			return err
		}

		if err := e.stdout.send(ctx, stdout, &posOut); err != nil {
			return err
		}
	}

	return e.err
}

func readPackObjectsStdin(stream gitalypb.HookService_PackObjectsHookServer) (f *os.File, err error) {
	defer func() {
		if f != nil && err != nil {
			f.Close()
		}
	}()

	f, err = ioutil.TempFile("", "")
	if err != nil {
		return nil, err
	}
	if err := os.Remove(f.Name()); err != nil {
		return nil, err
	}

	_, err = io.Copy(f, streamio.NewReader(func() ([]byte, error) {
		req, err := stream.Recv()
		return req.GetStdin(), err
	}))

	return f, err
}

type cache struct {
	*blob.Bucket
	cacheID string
	entries map[string]*entry
	sync.Mutex
	nextEntryID int
}

func (c *cache) key(repoPath string, args []string, stdin io.ReadSeeker) (string, error) {
	h := sha256.New()
	if _, err := stdin.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	if _, err := io.Copy(h, stdin); err != nil {
		return "", err
	}

	enc := json.NewEncoder(h)
	for _, v := range []interface{}{c.cacheID, args, repoPath} {
		if err := enc.Encode(v); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func (c *cache) newEntry(key string) *entry {
	e := &entry{
		c:   c,
		key: key,
	}
	e.Cond = sync.NewCond(e)
	e.stderr = &memBuffer{entry: e}
	e.stdout = &blobBuffer{entry: e}
	e.id = c.nextEntryID
	c.nextEntryID++

	return e
}

type entry struct {
	c   *cache
	key string
	sync.Mutex
	*sync.Cond
	done   bool
	err    error
	stderr *memBuffer
	stdout *blobBuffer
	id     int
}

func (e *entry) chunkKey(i int) string {
	chunk := fmt.Sprintf("%02x", i)
	return fmt.Sprintf("%s/%s/%s/%d", e.key, chunk[len(chunk)-2:], chunk, e.id)
}

func (e *entry) delete() {
	e.c.Lock()
	if e.c.entries[e.key] == e {
		delete(e.c.entries, e.key)
	}
	e.c.Unlock()
}

type memBuffer struct {
	buf []byte
	*entry
}

func (mb *memBuffer) Write(p []byte) (int, error) {
	mb.Lock()
	mb.buf = append(mb.buf, p...)
	mb.Unlock()
	mb.Broadcast()
	return len(p), nil
}

// Caller must hold lock when calling len.
func (mb *memBuffer) len() int { return len(mb.buf) }

func (mb *memBuffer) send(ctx context.Context, w io.Writer, pos *int) error {
	mb.Lock()
	buf := mb.buf
	mb.Unlock()
	if *pos > len(buf) {
		return errors.New("memBuffer: invalid pos")
	}

	n, err := w.Write(buf[*pos:])
	*pos += n
	return err
}

func newCache(cacheURL string) (*cache, error) {
	c := &cache{
		entries: make(map[string]*entry),
	}
	buf := make([]byte, 8)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return nil, err
	}
	c.cacheID = fmt.Sprintf("%x", buf)

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

type readSeekCloser interface {
	io.ReadSeeker
	io.Closer
}

type writeFlusher interface {
	io.Writer
	Flush() error
}

func runPackObjects(ctx context.Context, repoPath string, args []string, stdin readSeekCloser, stdout writeFlusher, stderr io.Writer) error {
	defer stdin.Close()
	if _, err := stdin.Seek(0, io.SeekStart); err != nil {
		return err
	}

	cmd, err := command.New(
		ctx,
		exec.Command("git", append([]string{"-C", repoPath}, args...)...),
		stdin,
		stdout,
		stderr,
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	return stdout.Flush()
}

func (e *entry) fill(repoPath string, args []string, stdin readSeekCloser, stdout io.Writer, stderr io.Writer) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := runPackObjects(ctx, repoPath, args, stdin, e.stdout, e.stderr)

	e.Lock()
	e.done = true
	e.err = err
	if err != nil {
		e.delete()
	}
	e.Unlock()

	e.Broadcast()
}

type blobBuffer struct {
	*entry
	w                *blob.Writer
	nChunks          int
	currentChunkSize int
}

func (bb *blobBuffer) Flush() error {
	bb.Lock()
	defer bb.Unlock()

	if bb.w == nil {
		return nil
	}

	if err := bb.w.Close(); err != nil {
		return err
	}

	bb.w = nil
	bb.nChunks++
	bb.currentChunkSize = 0
	bb.Broadcast()
	return nil
}

// Caller must hold lock when calling len.
func (bb *blobBuffer) len() int { return bb.nChunks }

func (bb *blobBuffer) send(ctx context.Context, w io.Writer, pos *int) error {
	bb.Lock()
	nChunks := bb.len()
	bb.Unlock()

	var err error
	for ; *pos < nChunks && err == nil; *pos++ {
		err = func() error {
			r, err := bb.c.Bucket.NewReader(ctx, bb.chunkKey(*pos), nil)
			if err != nil {
				return err
			}
			defer r.Close()

			_, err = io.Copy(w, r)
			return err
		}()
	}

	return err
}

func (bb *blobBuffer) Write(p []byte) (int, error) {
	w, err := bb.writer()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(p)
	if err != nil {
		return n, err
	}

	bb.Lock()
	bb.currentChunkSize += n
	bb.Unlock()

	if bb.chunkIsFull() {
		return n, bb.Flush()
	}

	return n, nil
}

func (bb *blobBuffer) writer() (io.Writer, error) {
	bb.Lock()
	defer bb.Unlock()
	if bb.w == nil {
		var err error
		bb.w, err = bb.entry.c.Bucket.NewWriter(context.Background(), bb.chunkKey(bb.len()), nil)
		if err != nil {
			return nil, err
		}
	}
	return bb.w, nil
}

func (bb *blobBuffer) chunkIsFull() bool {
	bb.Lock()
	defer bb.Unlock()
	return bb.currentChunkSize > chunkSize
}
