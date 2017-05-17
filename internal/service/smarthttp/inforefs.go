package smarthttp

import (
	"fmt"
	"io"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	pbhelper "gitlab.com/gitlab-org/gitaly-proto/go/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *server) InfoRefsUploadPack(in *pb.InfoRefsRequest, stream pb.SmartHTTP_InfoRefsUploadPackServer) error {
	w := pbhelper.NewSendWriter(func(p []byte) error {
		return stream.Send(&pb.InfoRefsResponse{Data: p})
	})
	return handleInfoRefs("upload-pack", in.Repository, w)
}

func (s *server) InfoRefsReceivePack(in *pb.InfoRefsRequest, stream pb.SmartHTTP_InfoRefsReceivePackServer) error {
	w := pbhelper.NewSendWriter(func(p []byte) error {
		return stream.Send(&pb.InfoRefsResponse{Data: p})
	})
	return handleInfoRefs("receive-pack", in.Repository, w)
}

func handleInfoRefs(service string, repo *pb.Repository, w io.Writer) error {
	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return err
	}

	cmd, err := helper.GitCommandReader(service, "--stateless-rpc", "--advertise-refs", repoPath)
	if err != nil {
		return grpc.Errorf(codes.Internal, "GetInfoRefs: cmd: %v", err)
	}
	defer cmd.Kill()

	helper.Debugf("handleInfoRefs: service=%q RepoPath=%q", service, repoPath)

	if err := pktLine(w, fmt.Sprintf("# service=git-%s\n", service)); err != nil {
		return grpc.Errorf(codes.Internal, "GetInfoRefs: pktLine: %v", err)
	}

	if err := pktFlush(w); err != nil {
		return grpc.Errorf(codes.Internal, "GetInfoRefs: pktFlush: %v", err)
	}

	if _, err := io.Copy(w, cmd); err != nil {
		return grpc.Errorf(codes.Internal, "GetInfoRefs: copy output of %v: %v", cmd.Args, err)
	}

	if err := cmd.Wait(); err != nil {
		return grpc.Errorf(codes.Internal, "GetInfoRefs: wait for %v: %v", cmd.Args, err)
	}

	return nil
}

func pktLine(w io.Writer, s string) error {
	_, err := fmt.Fprintf(w, "%04x%s", len(s)+4, s)
	return err
}

func pktFlush(w io.Writer) error {
	_, err := fmt.Fprint(w, "0000")
	return err
}
