package stats

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
)

// SendPack is used to parse the response body of a git-send-pack(1) request.
type SendPack struct {
	// ReportProgress is an optional callback set by the caller. If set, this function
	// will be called for all received progress packets.
	ReportProgress func([]byte)

	updatedRefs       int
	packets           int
	largestPacketSize int
	unpackOK          time.Time
	responseBody      time.Time
	multiband         map[string]*bandInfo
}

// Parse parses the server response from a git-send-pack(1) request. The expected response is
// documented in git.git:Documentation/techincal/pack-protocol.txt. We expect that the report-status
// capability is active and thus don't support the report-status-v2 protocol.
func (s *SendPack) Parse(body io.Reader) error {
	s.multiband = make(map[string]*bandInfo)
	for _, band := range Bands() {
		s.multiband[band] = &bandInfo{}
	}

	scanner := pktline.NewScanner(body)
	seenFlush := false
	seenPack := false

	// We're requesting the side-band-64k capability and thus receive a multiplexed stream from
	// git-receive-pack(1). The first byte of each pktline will identify the multiplexing band.
	for ; scanner.Scan(); s.packets++ {
		if seenFlush {
			return errors.New("received extra packet after flush")
		}
		if pktline.IsFlush(scanner.Bytes()) {
			seenFlush = true
			continue
		}

		data := pktline.Data(scanner.Bytes())
		if len(data) == 0 {
			return errors.New("empty packet in PACK data")
		}
		if len(data) > s.largestPacketSize {
			s.largestPacketSize = len(data)
		}

		band, err := bandToHuman(data[0])
		if err != nil {
			return fmt.Errorf("converting band: %w", err)
		}

		s.multiband[band].consume(data)
		data = data[1:]

		switch band {
		case bandPack:
			// The pack band contains our "normal" pktlines which are nested into the
			// outer pktline.
			scanner := pktline.NewScanner(bytes.NewReader(data))
			for scanner.Scan() {
				if pktline.IsFlush(scanner.Bytes()) {
					break
				}

				data := pktline.Data(scanner.Bytes())
				if !seenPack {
					if !bytes.Equal(data, []byte("unpack ok\n")) {
						return fmt.Errorf("expected unpack ok, got %q", data)
					}
					s.unpackOK = time.Now()
					seenPack = true
				} else {
					if bytes.HasPrefix(data, []byte("ok ")) {
						s.updatedRefs++
					} else if bytes.HasPrefix(data, []byte("ng ")) {
						return fmt.Errorf("reference update failed: %q", data)
					} else {
						return fmt.Errorf("unsupported packet line: %q", data)
					}
				}
			}
		case bandProgress:
			if s.ReportProgress != nil {
				s.ReportProgress(data)
			}
		case bandError:
			return fmt.Errorf("received error: %q", data)
		default:
			return fmt.Errorf("unsupported band: %q", band)
		}
	}

	s.responseBody = time.Now()

	return nil
}
