package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
)

func simulateHTTPClone(gitDir string) {
	msg("Generating server response for HTTP clone. Data goes to /dev/null.")
	infoRefs := exec.Command("git", "upload-pack", "--stateless-rpc", "--advertise-refs", gitDir)
	infoRefs.Stderr = os.Stderr
	out, err := infoRefs.StdoutPipe()
	noError(err)

	start := time.Now()
	noError(infoRefs.Start())

	infoScanner := bufio.NewScanner(out)
	var infoLines []string
	for infoScanner.Scan() {
		infoLines = append(infoLines, infoScanner.Text())
	}
	noError(infoScanner.Err())

	noError(infoRefs.Wait())

	msg("simulated GET \"/info/refs?service=git-upload-pack\" returned %d lines, took %v", len(infoLines), time.Since(start))

	if len(infoLines) == 0 {
		fatal("no refs were advertised")
	}

	request := &bytes.Buffer{}
	refsHeads := regexp.MustCompile(`^[a-f0-9]{44} refs/(heads|tags)/`)
	firstLine := true
	for _, line := range infoLines {
		if !refsHeads.MatchString(line) {
			continue
		}

		commitID := line[4:44]

		if firstLine {
			firstLine = false
			fmt.Fprintf(request, "0098want %s multi_ack_detailed no-done side-band-64k thin-pack ofs-delta deepen-since deepen-not agent=git/2.19.1\n", commitID)
			continue
		}

		fmt.Fprintf(request, "0032want %s\n", commitID)
	}
	fmt.Fprint(request, "00000009done\n")

	uploadPack := exec.Command("git", "upload-pack", "--stateless-rpc", gitDir)
	uploadPack.Stdin = request
	uploadPack.Stderr = os.Stderr
	out, err = uploadPack.StdoutPipe()
	noError(err)

	start = time.Now()
	noError(uploadPack.Start())

	var n int64
	scanner := pktline.NewScanner(out)
	for scanner.Scan() {
		n += int64(len(scanner.Bytes()))
		data := pktline.Data(scanner.Bytes())
		if len(data) == 0 {
			continue
		}

		// Print progress data
		if data[0] == 2 {
			_, err := os.Stdout.Write(data[1:])
			noError(err)
		}
	}

	noError(scanner.Err())

	msg("simulated POST \"/git-upload-pack\" returned %s, took %v", humanBytes(n), time.Since(start))
}
