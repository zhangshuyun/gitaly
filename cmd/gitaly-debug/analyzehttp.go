package main

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
)

func analyzeHTTPClone(cloneURL string) {
	st := &stats.Clone{
		URL:         cloneURL,
		Interactive: true,
	}

	noError(st.Perform(context.Background()))

	fmt.Println("\n--- GET metrics:")
	for _, entry := range []metric{
		{"response header time", st.Get.ResponseHeader()},
		{"first Git packet", st.Get.FirstGitPacket()},
		{"response body time", st.Get.ResponseBody()},
		{"payload size", st.Get.PayloadSize},
		{"Git packets received", st.Get.Packets},
		{"refs advertised", len(st.Get.Refs)},
		{"wanted refs", st.RefsWanted()},
	} {
		entry.print()
	}

	fmt.Println("\n--- POST metrics:")
	for _, entry := range []metric{
		{"response header time", st.Post.ResponseHeader()},
		{"time to server NAK", st.Post.NAK()},
		{"response body time", st.Post.ResponseBody()},
		{"largest single Git packet", st.Post.LargestPacketSize()},
		{"Git packets received", st.Post.Packets()},
	} {
		entry.print()
	}

	for _, band := range stats.Bands() {
		numPackets := st.Post.BandPackets(band)
		if numPackets == 0 {
			continue
		}

		fmt.Printf("\n--- POST %s band\n", band)
		for _, entry := range []metric{
			{"time to first packet", st.Post.BandFirstPacket(band)},
			{"packets", numPackets},
			{"total payload size", st.Post.BandPayloadSize(band)},
		} {
			entry.print()
		}
	}
}

type metric struct {
	key   string
	value interface{}
}

func (m metric) print() { fmt.Printf("%-40s %v\n", m.key, m.value) }
