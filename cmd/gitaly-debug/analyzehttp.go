package main

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
)

func analyzeHTTPClone(cloneURL string) {
	st, err := stats.PerformHTTPClone(context.Background(), cloneURL, "", "", true)
	noError(err)

	fmt.Println("\n--- Reference discovery metrics:")
	for _, entry := range []metric{
		{"response header time", st.ReferenceDiscovery.ResponseHeader()},
		{"first Git packet", st.ReferenceDiscovery.FirstGitPacket()},
		{"response body time", st.ReferenceDiscovery.ResponseBody()},
		{"payload size", st.ReferenceDiscovery.PayloadSize()},
		{"Git packets received", st.ReferenceDiscovery.Packets()},
		{"refs advertised", len(st.ReferenceDiscovery.Refs())},
	} {
		entry.print()
	}

	fmt.Println("\n--- Fetch pack metrics:")
	for _, entry := range []metric{
		{"response header time", st.FetchPack.ResponseHeader()},
		{"time to server NAK", st.FetchPack.NAK()},
		{"response body time", st.FetchPack.ResponseBody()},
		{"largest single Git packet", st.FetchPack.LargestPacketSize()},
		{"Git packets received", st.FetchPack.Packets()},
		{"wanted refs", st.FetchPack.RefsWanted()},
	} {
		entry.print()
	}

	for _, band := range stats.Bands() {
		numPackets := st.FetchPack.BandPackets(band)
		if numPackets == 0 {
			continue
		}

		fmt.Printf("\n--- FetchPack %s band\n", band)
		for _, entry := range []metric{
			{"time to first packet", st.FetchPack.BandFirstPacket(band)},
			{"packets", numPackets},
			{"total payload size", st.FetchPack.BandPayloadSize(band)},
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
