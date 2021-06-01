package stats

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
)

type Clone struct {
	URL         string
	Interactive bool
	User        string
	Password    string

	wants []string // all branch and tag pointers
	Get
	Post
}

func (cl *Clone) RefsWanted() int { return len(cl.wants) }

// Perform does a Git HTTP clone, discarding cloned data to /dev/null.
func (cl *Clone) Perform(ctx context.Context) error {
	if err := cl.doGet(ctx); err != nil {
		return ctxErr(ctx, err)
	}

	if err := cl.doPost(ctx); err != nil {
		return ctxErr(ctx, err)
	}

	return nil
}

func ctxErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

type Get struct {
	start          time.Time
	responseHeader time.Duration
	httpStatus     int
	ReferenceDiscovery
}

func (g *Get) ResponseHeader() time.Duration { return g.responseHeader }
func (g *Get) HTTPStatus() int               { return g.httpStatus }
func (g *Get) FirstGitPacket() time.Duration { return g.FirstPacket.Sub(g.start) }
func (g *Get) ResponseBody() time.Duration   { return g.LastPacket.Sub(g.start) }

func (cl *Clone) doGet(ctx context.Context) error {
	req, err := http.NewRequest("GET", cl.URL+"/info/refs?service=git-upload-pack", nil)
	if err != nil {
		return err
	}

	req = req.WithContext(ctx)
	if cl.User != "" {
		req.SetBasicAuth(cl.User, cl.Password)
	}

	for k, v := range map[string]string{
		"User-Agent":      "gitaly-debug",
		"Accept":          "*/*",
		"Accept-Encoding": "deflate, gzip",
		"Pragma":          "no-cache",
	} {
		req.Header.Set(k, v)
	}

	cl.Get.start = time.Now()
	cl.printInteractive("---")
	cl.printInteractive("--- GET %v", req.URL)
	cl.printInteractive("---")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	if code := resp.StatusCode; code < 200 || code >= 400 {
		return fmt.Errorf("git http get: unexpected http status: %d", code)
	}

	cl.Get.responseHeader = time.Since(cl.Get.start)
	cl.Get.httpStatus = resp.StatusCode
	cl.printInteractive("response code: %d", resp.StatusCode)
	cl.printInteractive("response header: %v", resp.Header)

	body := resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		body, err = gzip.NewReader(body)
		if err != nil {
			return err
		}
	}

	if err := cl.Get.Parse(body); err != nil {
		return err
	}

	for _, ref := range cl.Get.Refs {
		if strings.HasPrefix(ref.Name, "refs/heads/") || strings.HasPrefix(ref.Name, "refs/tags/") {
			cl.wants = append(cl.wants, ref.Oid)
		}
	}

	return nil
}

type Post struct {
	start             time.Time
	responseHeader    time.Duration
	httpStatus        int
	nak               time.Duration
	multiband         map[string]*bandInfo
	responseBody      time.Duration
	packets           int
	largestPacketSize int
}

func (p *Post) ResponseHeader() time.Duration { return p.responseHeader }
func (p *Post) HTTPStatus() int               { return p.httpStatus }
func (p *Post) NAK() time.Duration            { return p.nak }
func (p *Post) ResponseBody() time.Duration   { return p.responseBody }
func (p *Post) Packets() int                  { return p.packets }
func (p *Post) LargestPacketSize() int        { return p.largestPacketSize }

func (p *Post) BandPackets(b string) int               { return p.multiband[b].packets }
func (p *Post) BandPayloadSize(b string) int64         { return p.multiband[b].size }
func (p *Post) BandFirstPacket(b string) time.Duration { return p.multiband[b].firstPacket }

type bandInfo struct {
	firstPacket time.Duration
	size        int64
	packets     int
}

func (bi *bandInfo) consume(start time.Time, data []byte) {
	if bi.packets == 0 {
		bi.firstPacket = time.Since(start)
	}
	bi.size += int64(len(data))
	bi.packets++
}

// See
// https://github.com/git/git/blob/v2.25.0/Documentation/technical/http-protocol.txt#L351
// for background information.
func (cl *Clone) buildPost(ctx context.Context) (*http.Request, error) {
	reqBodyRaw := &bytes.Buffer{}
	reqBodyGzip := gzip.NewWriter(reqBodyRaw)
	for i, oid := range cl.wants {
		if i == 0 {
			oid += " multi_ack_detailed no-done side-band-64k thin-pack ofs-delta deepen-since deepen-not agent=git/2.21.0"
		}
		if _, err := pktline.WriteString(reqBodyGzip, "want "+oid+"\n"); err != nil {
			return nil, err
		}
	}
	if err := pktline.WriteFlush(reqBodyGzip); err != nil {
		return nil, err
	}
	if _, err := pktline.WriteString(reqBodyGzip, "done\n"); err != nil {
		return nil, err
	}
	if err := reqBodyGzip.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", cl.URL+"/git-upload-pack", reqBodyRaw)
	if err != nil {
		return nil, err
	}

	req = req.WithContext(ctx)
	if cl.User != "" {
		req.SetBasicAuth(cl.User, cl.Password)
	}

	for k, v := range map[string]string{
		"User-Agent":       "gitaly-debug",
		"Content-Type":     "application/x-git-upload-pack-request",
		"Accept":           "application/x-git-upload-pack-result",
		"Content-Encoding": "gzip",
	} {
		req.Header.Set(k, v)
	}

	return req, nil
}

func (cl *Clone) doPost(ctx context.Context) error {
	req, err := cl.buildPost(ctx)
	if err != nil {
		return err
	}

	cl.Post.start = time.Now()
	cl.printInteractive("---")
	cl.printInteractive("--- POST %v", req.URL)
	cl.printInteractive("---")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if code := resp.StatusCode; code < 200 || code >= 400 {
		return fmt.Errorf("git http post: unexpected http status: %d", code)
	}

	cl.Post.responseHeader = time.Since(cl.Post.start)
	cl.Post.httpStatus = resp.StatusCode
	cl.printInteractive("response code: %d", resp.StatusCode)
	cl.printInteractive("response header: %v", resp.Header)

	// Expected response:
	// - "NAK\n"
	// - "<side band byte><pack or progress or error data>
	// - ...
	// - FLUSH
	//

	cl.Post.multiband = make(map[string]*bandInfo)
	for _, band := range Bands() {
		cl.Post.multiband[band] = &bandInfo{}
	}

	seenFlush := false

	scanner := pktline.NewScanner(resp.Body)
	for ; scanner.Scan(); cl.Post.packets++ {
		if seenFlush {
			return errors.New("received extra packet after flush")
		}

		if n := len(scanner.Bytes()); n > cl.Post.largestPacketSize {
			cl.Post.largestPacketSize = n
		}

		data := pktline.Data(scanner.Bytes())

		if cl.Post.packets == 0 {
			// We're now looking at the first git packet sent by the server. The
			// server must conclude the ref negotiation. Because we have not sent any
			// "have" messages there is nothing to negotiate and the server should
			// send a single NAK.
			if !bytes.Equal([]byte("NAK\n"), data) {
				return fmt.Errorf("expected NAK, got %q", data)
			}
			cl.Post.nak = time.Since(cl.Post.start)
			continue
		}

		if pktline.IsFlush(scanner.Bytes()) {
			seenFlush = true
			continue
		}

		if len(data) == 0 {
			return errors.New("empty packet in PACK data")
		}

		band, err := bandToHuman(data[0])
		if err != nil {
			return err
		}

		cl.Post.multiband[band].consume(cl.Post.start, data[1:])

		// Print progress data as-is
		if cl.Interactive && band == bandProgress {
			if _, err := os.Stdout.Write(data[1:]); err != nil {
				return err
			}
		}

		if cl.Interactive && cl.Post.packets%500 == 0 && cl.Post.packets > 0 && band == bandPack {
			// Print dots to have some sort of progress meter for the user in
			// interactive mode. It's not accurate progress, but it shows that
			// something is happening.
			if _, err := fmt.Print("."); err != nil {
				return err
			}
		}
	}

	if cl.Interactive {
		// Trailing newline for progress dots.
		if _, err := fmt.Println(""); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	if !seenFlush {
		return errors.New("POST response did not end in flush")
	}

	cl.Post.responseBody = time.Since(cl.Post.start)
	return nil
}

func (cl *Clone) printInteractive(format string, a ...interface{}) error {
	if !cl.Interactive {
		return nil
	}

	if _, err := fmt.Println(fmt.Sprintf(format, a...)); err != nil {
		return err
	}

	return nil
}

const (
	bandPack     = "pack"
	bandProgress = "progress"
	bandError    = "error"
)

// Bands returns the slice of bands which git uses to transport different kinds
// of data in a multiplexed way. See
// https://git-scm.com/docs/protocol-capabilities/2.24.0#_side_band_side_band_64k
// for more information about the different bands.
func Bands() []string { return []string{bandPack, bandProgress, bandError} }

func bandToHuman(b byte) (string, error) {
	bands := Bands()

	// Band index bytes are 1-indexed.
	if b < 1 || int(b) > len(bands) {
		return "", fmt.Errorf("invalid band index: %d", b)
	}

	return bands[b-1], nil
}
