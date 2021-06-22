package stats

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
)

func TestSendPack_Parse(t *testing.T) {
	t.Run("valid input", func(t *testing.T) {
		var progress []byte
		sendPack := &SendPack{
			ReportProgress: func(p []byte) {
				progress = append(progress, p...)
			},
		}

		var response bytes.Buffer

		startTime := time.Now()

		gittest.WritePktlineString(t, &response, "\x01000eunpack ok\n0015ok refs/heads/branch\n0000")
		gittest.WritePktlineString(t, &response, "\x010013ok refs/heads/other0000\n")
		gittest.WritePktlineString(t, &response, "\x02progress-bytes")
		gittest.WritePktlineFlush(t, &response)

		err := sendPack.Parse(&response)
		require.NoError(t, err)

		endTime := time.Now()

		require.Equal(t, 4, sendPack.packets)
		require.Equal(t, 44, sendPack.largestPacketSize)

		for _, band := range []string{"pack", "progress"} {
			require.True(t, startTime.Before(sendPack.multiband[band].firstPacket))
			require.True(t, endTime.After(sendPack.multiband[band].firstPacket))
			sendPack.multiband[band].firstPacket = startTime
		}

		require.Equal(t, &bandInfo{
			firstPacket: startTime,
			size:        73,
			packets:     2,
		}, sendPack.multiband["pack"])

		require.Equal(t, &bandInfo{
			firstPacket: startTime,
			size:        15,
			packets:     1,
		}, sendPack.multiband["progress"])

		require.Equal(t, &bandInfo{}, sendPack.multiband["error"])

		require.Equal(t, "progress-bytes", string(progress))
	})

	t.Run("data after flush", func(t *testing.T) {
		var response bytes.Buffer
		gittest.WritePktlineString(t, &response, "\x01000eunpack ok\n0000")
		gittest.WritePktlineFlush(t, &response)
		gittest.WritePktlineString(t, &response, "\x01somethingsomething")

		err := (&SendPack{}).Parse(&response)
		require.Equal(t, errors.New("received extra packet after flush"), err)
	})

	t.Run("unpack error", func(t *testing.T) {
		var response bytes.Buffer
		gittest.WritePktlineString(t, &response, "\x010011unpack error\n0000")

		err := (&SendPack{}).Parse(&response)
		require.Equal(t, errors.New("expected unpack ok, got \"unpack error\\n\""), err)
	})

	t.Run("error sideband", func(t *testing.T) {
		var response bytes.Buffer
		gittest.WritePktlineString(t, &response, "\x01000eunpack ok\n0000")
		gittest.WritePktlineString(t, &response, "\x03error-bytes")
		gittest.WritePktlineFlush(t, &response)

		err := (&SendPack{}).Parse(&response)
		require.Equal(t, errors.New("received error: \"error-bytes\""), err)
	})

	t.Run("failed reference update", func(t *testing.T) {
		var response bytes.Buffer
		gittest.WritePktlineString(t, &response, "\x01000eunpack ok\n0000")
		gittest.WritePktlineString(t, &response, "\x010014ok refs/heads/branch0000")
		gittest.WritePktlineString(t, &response, "\x010021ng refs/heads/feature failure0000")
		gittest.WritePktlineFlush(t, &response)

		err := (&SendPack{}).Parse(&response)
		require.Equal(t, errors.New("reference update failed: \"ng refs/heads/feature failure\""), err)
	})
}
