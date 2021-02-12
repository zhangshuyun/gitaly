package stats

import (
	"bufio"
	"context"
	"io"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
)

// LogObjectsInfo read statistics of the git repo objects
// and logs it under 'count-objects' key as structured entry.
func LogObjectsInfo(ctx context.Context, gitCmdFactory git.CommandFactory, repo repository.GitRepo) {
	logger := ctxlogrus.Extract(ctx)

	cmd, err := gitCmdFactory.New(ctx, repo, nil, git.SubCmd{
		Name:  "count-objects",
		Flags: []git.Option{git.Flag{Name: "--verbose"}},
	})

	if err != nil {
		logger.WithError(err).Warn("failed on bootstrapping to gather object statistic")
		return
	}

	stats, err := readObjectInfoStatistic(cmd)
	if err != nil {
		logger.WithError(err).Warn("failed on reading to gather object statistic")
	}

	if err := cmd.Wait(); err != nil {
		logger.WithError(err).Warn("failed on waiting to gather object statistic")
		return
	}

	if len(stats) > 0 {
		logger.WithField("count_objects", stats).Info("git repo statistic")
	}
}

/* readObjectInfoStatistic parses output of 'git count-objects -v' command and represents it as dictionary
current supported format is:
  count: 12
  packs: 2
  size-garbage: 934
  alternate: /some/path/to/.git/objects
  alternate: "/some/other path/to/.git/objects"
will result in:
  {
    "count": 12,
    "packs": 2,
    "size-garbage": 934,
    "alternate": ["/some/path/to/.git/objects", "/some/other path/to/.git/objects"]
  }
*/
func readObjectInfoStatistic(reader io.Reader) (map[string]interface{}, error) {
	stats := map[string]interface{}{}

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) != 2 {
			continue
		}

		// one of: count, size, in-pack, packs, size-pack, prune-packable, garbage, size-garbage, alternate (repeatable)
		key := parts[0]
		rawVal := strings.TrimPrefix(parts[1], ": ")

		switch key {
		case "alternate":
			addMultiString(stats, key, rawVal)
		default:
			addInt(stats, key, rawVal)
		}
	}

	return stats, scanner.Err()
}

func addMultiString(stats map[string]interface{}, key, rawVal string) {
	val := strings.Trim(rawVal, "\" \t\n")

	statVal, found := stats[key]
	if !found {
		stats[key] = val
		return
	}

	statAggr, ok := statVal.([]string) // 'alternate' is only repeatable key and it is a string type
	if ok {
		statAggr = append(statAggr, val)
	} else {
		delete(stats, key) // remove single string value of 'alternate' to replace it with slice
		statAggr = []string{statVal.(string), val}
	}
	stats[key] = statAggr
}

func addInt(stats map[string]interface{}, key, rawVal string) {
	val, err := strconv.ParseInt(rawVal, 10, 64)
	if err != nil {
		return
	}

	stats[key] = val
}
