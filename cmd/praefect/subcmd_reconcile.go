package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type nodeReconciler struct {
	conf             config.Config
	virtualStorage   string
	targetStorage    string
	referenceStorage string
}

type reconcileSubcommand struct {
	virtual   string
	target    string
	reference string
}

func (s *reconcileSubcommand) FlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("reconcile", flag.ExitOnError)
	fs.StringVar(&s.virtual, "virtual", "", "virtual storage for target storage")
	fs.StringVar(&s.target, "target", "", "target storage to reconcile")
	fs.StringVar(&s.reference, "reference", "", "reference storage to reconcile (optional)")
	return fs
}

func (s *reconcileSubcommand) Exec(flags *flag.FlagSet, conf config.Config) error {
	nr := nodeReconciler{
		conf:             conf,
		virtualStorage:   s.virtual,
		targetStorage:    s.target,
		referenceStorage: s.reference,
	}

	if err := nr.reconcile(); err != nil {
		return fmt.Errorf("unable to reconcile: %s", err)
	}

	return nil
}

func (nr nodeReconciler) reconcile() error {
	if err := nr.validateArgs(); err != nil {
		return err
	}

	var nodeAddr string
	switch {
	case nr.conf.SocketPath != "":
		nodeAddr = "unix://" + nr.conf.SocketPath
	case nr.conf.ListenAddr != "":
		nodeAddr = "tcp://" + nr.conf.ListenAddr
	default:
		return errors.New("no Praefect address configured")
	}

	cc, err := subCmdDial(nodeAddr, nr.conf.Auth.Token)
	if err != nil {
		return err
	}

	pCli := gitalypb.NewPraefectInfoServiceClient(cc)

	request := &gitalypb.ConsistencyCheckRequest{
		VirtualStorage:   nr.virtualStorage,
		TargetStorage:    nr.targetStorage,
		ReferenceStorage: nr.referenceStorage,
	}
	stream, err := pCli.ConsistencyCheck(context.TODO(), request)
	if err != nil {
		return err
	}

	if err := nr.consumeStream(stream); err != nil {
		return err
	}

	return nil
}

func (nr nodeReconciler) validateArgs() error {
	var vsFound, tFound, rFound bool

	for _, vs := range nr.conf.VirtualStorages {
		if vs.Name != nr.virtualStorage {
			continue
		}
		vsFound = true

		for _, n := range vs.Nodes {
			if n.Storage == nr.targetStorage {
				tFound = true
			}
			if n.Storage == nr.referenceStorage {
				rFound = true
			}
		}
	}

	if !vsFound {
		return fmt.Errorf(
			"cannot find virtual storage %s in config", nr.virtualStorage,
		)
	}
	if !tFound {
		return fmt.Errorf(
			"cannot find target storage %s in virtual storage %q in config",
			nr.targetStorage, nr.virtualStorage,
		)
	}
	if nr.referenceStorage != "" && !rFound {
		return fmt.Errorf(
			"cannot find reference storage %q in virtual storage %q in config",
			nr.referenceStorage, nr.virtualStorage,
		)
	}

	return nil
}

func (nr nodeReconciler) consumeStream(stream gitalypb.PraefectInfoService_ConsistencyCheckClient) error {
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if resp.GetReferenceChecksum() != resp.GetTargetChecksum() {
			log.Printf(
				"INCONSISTENT: %s - replication scheduled: #%d",
				resp.GetRepoRelativePath(),
				resp.GetReplJobId(),
			)
		}
	}
	return nil
}
