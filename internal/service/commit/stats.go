package commit

import (
	"bufio"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

var (
	commitRegex = regexp.MustCompile("([a-f0-9]{40})")
)

func (server) CommitStats(ctx context.Context, in *pb.CommitStatsRequest) (*pb.CommitStatsResponse, error) {
	repoPath, err := helper.GetRepoPath(in.GetRepository())
	if err != nil {
		return nil, err
	}

	if len(in.GetRevision()) == 0 {
		return nil, grpc.Errorf(codes.InvalidArgument, "no revision given")
	}

	// NOTE: append `^{commit}` so we only fetch the commit...
	revision := fmt.Sprintf("%s^{commit}", in.GetRevision())
	cmd, err := helper.GitCommandReader("--git-dir", repoPath, "show", "--format='%H'", "--numstat", revision)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, err.Error())
	}

	resp := &pb.CommitStatsResponse{}
	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		line := scanner.Text()

		// Don't try to match OID if it's already set
		if len(resp.Oid) == 0 {
			// Use Regex since we wanna be _really_ sure that this is a commit SHA
			if matches := commitRegex.FindStringSubmatch(line); len(matches) >= 2 {
				resp.Oid = matches[1]
			}
			// if OID isn't set, or just been set, continue
			continue
		}
		if stats := strings.SplitN(line, "\t", 3); len(stats) == 3 {
			var a, d int64
			a, err = strconv.ParseInt(stats[0], 10, 32)
			if err != nil {
				return nil, grpc.Errorf(codes.Internal, err.Error())
			}
			d, err = strconv.ParseInt(stats[1], 10, 32)
			if err != nil {
				return nil, grpc.Errorf(codes.Internal, err.Error())
			}
			resp.Additions += int32(a)
			resp.Deletions += int32(d)
		}
	}

	if err = cmd.Wait(); err != nil {
		return nil, grpc.Errorf(codes.Internal, err.Error())
	}

	return resp, nil
}
