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

	var posErr, posOut int
	for done := false; !done; {
		e.Lock()
		for !e.done && posErr == e.stderr.SizeLocked() && posOut == e.stdout.SizeLocked() {
			e.Wait()
		}
		done = e.done
		e.Unlock()

		if err := e.stderr.Send(ctx, stderr, &posErr); err != nil {
			return err
		}

		if err := e.stdout.Send(ctx, stdout, &posOut); err != nil {
			return err
		}
	}

	return e.err
}

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
	e.stderr = &memBuffer{entry: e}
	e.stdout = &blobBuffer{entry: e}
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
	return fmt.Sprintf("%s/%s/%s.%d", e.key, chunk[len(chunk)-2:], chunk, e.id)
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

func (mb *memBuffer) SizeLocked() int { return len(mb.buf) }

func (mb *memBuffer) Send(ctx context.Context, w io.Writer, pos *int) error {
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

		stdout := bufio.NewWriter(e.stdout)
		cmd, err := command.New(
			ctx,
			exec.Command("git", append([]string{"-C", repoPath}, args...)...),
			bytes.NewReader(stdin),
			stdout,
			e.stderr,
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

		return e.stdout.FlushChunk()
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

type blobBuffer struct {
	*entry
	w                *blob.Writer
	nChunks          int
	currentChunkSize int
}

func (bb *blobBuffer) FlushChunk() error {
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

func (bb *blobBuffer) SizeLocked() int { return bb.nChunks }

func (bb *blobBuffer) Send(ctx context.Context, w io.Writer, pos *int) error {
	bb.Lock()
	nChunks := bb.SizeLocked()
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
		return n, bb.FlushChunk()
	}

	return n, nil
}

func (bb *blobBuffer) writer() (io.Writer, error) {
	bb.Lock()
	defer bb.Unlock()
	if bb.w == nil {
		var err error
		bb.w, err = bb.entry.c.Bucket.NewWriter(context.Background(), bb.chunkKey(bb.nChunks), nil)
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
