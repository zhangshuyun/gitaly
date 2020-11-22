package hook

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"

	"github.com/golang/groupcache/lru"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

var packCache *cache

func init() {
	var err error
	packCache, err = newCache("file:///tmp/gitaly-cache")
	if err != nil {
		panic(err)
	}
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
		// If we have a cache miss and we win the race to spawn a goroutine that
		// fills the cache, that goroutine becomes responsible for closing the
		// stdin tempfile.
		if stdinHandoff {
			return
		}

		stdin.Close()
	}()

	var m sync.Mutex
	stdout := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.PackObjectsHookResponse{Stdout: p})
	})
	stderr := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.PackObjectsHookResponse{Stderr: p})
	})

	key, err := packCache.key(repoPath, firstRequest.Args, stdin)
	if err != nil {
		return err
	}

	var e *entry
	packCache.Lock()
	if lruEntry, ok := packCache.entries.Get(key); ok && !lruEntry.(*entry).isStale() {
		e = lruEntry.(*entry)
	} else {
		e = packCache.createEntry(key)
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

func readPackObjectsStdin(stream gitalypb.HookService_PackObjectsHookServer) (_ *os.File, err error) {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	if err := os.Remove(f.Name()); err != nil {
		return nil, err
	}

	if _, err := io.Copy(f, streamio.NewReader(func() ([]byte, error) {
		req, err := stream.Recv()
		return req.GetStdin(), err
	})); err != nil {
		return nil, err
	}

	return f, nil
}

type cache struct {
	*blob.Bucket
	cacheID string
	entries *lru.Cache
	sync.Mutex
	nextEntryID int
	chunkSize   int
	maxAge      time.Duration
}

func newCache(cacheURL string) (*cache, error) {
	c := &cache{
		entries:   lru.New(10000),
		chunkSize: 50 * 1024 * 1024,
		maxAge:    15 * time.Minute,
	}

	var err error
	c.cacheID, err = text.RandomHex(8)
	if err != nil {
		return nil, err
	}

	if v := os.Getenv("GITALY_CACHE_BUCKET"); v != "" {
		cacheURL = v
	}

	c.Bucket, err = blob.OpenBucket(context.Background(), cacheURL)
	if err != nil {
		return nil, err
	}

	return c, nil
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
	for _, v := range []interface{}{c.cacheID, args, filepath.Clean(repoPath)} {
		if err := enc.Encode(v); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// createEntry expects caller to lock/unlock cache.
func (c *cache) createEntry(key string) *entry {
	e := &entry{
		c:       c,
		key:     key,
		created: time.Now(),
	}
	e.Cond = sync.NewCond(e)
	e.stderr = &memBuffer{entry: e}
	e.stdout = &blobBuffer{entry: e}

	e.id = c.nextEntryID
	c.nextEntryID++

	c.entries.Add(key, e)

	return e
}

type entry struct {
	c   *cache
	key string
	sync.Mutex
	*sync.Cond
	done    bool
	err     error
	stderr  *memBuffer
	stdout  *blobBuffer
	id      int
	created time.Time
}

func (e *entry) isStale() bool { return time.Since(e.created) > e.c.maxAge }

func (e *entry) chunkKey(i int) string {
	chunk := fmt.Sprintf("%02x", i)
	return fmt.Sprintf("%s/%s/%s/%d", e.key, chunk[len(chunk)-2:], chunk, e.id)
}

func (e *entry) delete() {
	e.c.Lock()
	defer e.c.Unlock()

	if lruEntry, ok := e.c.entries.Get(e.key); ok && lruEntry.(*entry) == e {
		e.c.entries.Remove(e.key)
	}
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

// Flush finishes the current chunk.
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

	if bb.w != nil {
		return bb.w, nil
	}

	var err error
	if bb.w, err = bb.entry.c.Bucket.NewWriter(
		context.Background(),
		bb.chunkKey(bb.len()),
		&blob.WriterOptions{ContentType: "application/octet-stream"},
	); err != nil {
		return nil, err
	}

	return bb.w, nil
}

func (bb *blobBuffer) chunkIsFull() bool {
	bb.Lock()
	defer bb.Unlock()
	return bb.currentChunkSize > bb.c.chunkSize
}
