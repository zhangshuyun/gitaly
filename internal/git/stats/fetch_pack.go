package stats

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
)

// FetchPack is used to parse the response body of a git-fetch-pack(1) request.
type FetchPack struct {
	// ReportProgress is an optional callback set by the caller. If set, this function
	// will be called for all received progress packets.
	ReportProgress func([]byte)

	packets           int
	largestPacketSize int
	multiband         map[string]*bandInfo
	nak               time.Time
	responseBody      time.Time
}

// Parse parses the server response from a git-fetch-pack(1) request.
func (f *FetchPack) Parse(body io.Reader) error {
	// Expected response:
	// - "NAK\n"
	// - "<side band byte><pack or progress or error data>
	// - ...
	// - FLUSH

	f.multiband = make(map[string]*bandInfo)
	for _, band := range Bands() {
		f.multiband[band] = &bandInfo{}
	}

	seenFlush := false

	scanner := pktline.NewScanner(body)
	for ; scanner.Scan(); f.packets++ {
		if seenFlush {
			return errors.New("received extra packet after flush")
		}

		if n := len(scanner.Bytes()); n > f.largestPacketSize {
			f.largestPacketSize = n
		}

		data := pktline.Data(scanner.Bytes())

		if f.packets == 0 {
			// We're now looking at the first git packet sent by the server. The
			// server must conclude the ref negotiation. Because we have not sent any
			// "have" messages there is nothing to negotiate and the server should
			// send a single NAK.
			if !bytes.Equal([]byte("NAK\n"), data) {
				return fmt.Errorf("expected NAK, got %q", data)
			}
			f.nak = time.Now()
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

		f.multiband[band].consume(data[1:])

		// Print progress data as-is.
		if f.ReportProgress != nil && band == bandProgress {
			f.ReportProgress(data[1:])
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	if !seenFlush {
		return errors.New("POST response did not end in flush")
	}

	f.responseBody = time.Now()
	return nil
}

type bandInfo struct {
	firstPacket time.Time
	size        int64
	packets     int
}

func (bi *bandInfo) consume(data []byte) {
	if bi.packets == 0 {
		bi.firstPacket = time.Now()
	}
	bi.size += int64(len(data))
	bi.packets++
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
