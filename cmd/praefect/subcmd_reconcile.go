package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type nodeReconciler struct {
	conf                  config.Config
	virtualStorage        string
	targetStorage         string
	referenceStorage      string
	disableReconciliation bool
}

type reconcileSubcommand struct {
	virtual   string
	target    string
	reference string
	force     bool
}

func (s *reconcileSubcommand) FlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("reconcile", flag.ExitOnError)
	fs.StringVar(&s.virtual, "virtual", "", "virtual storage for target storage")
	fs.StringVar(&s.target, "target", "", "target storage to reconcile")
	fs.StringVar(&s.reference, "reference", "", "reference storage to reconcile (optional)")
	fs.BoolVar(&s.force, "f", false, "actually schedule replications")
	return fs
}

func (s *reconcileSubcommand) Exec(flags *flag.FlagSet, conf config.Config) error {
	logger.Warn("The reconcile sub-command has been deprecated in GitLab 13.12 and is scheduled for removal in GitLab 14.0. Use the automatic reconciler instead: https://docs.gitlab.com/ee/administration/gitaly/praefect.html#automatic-reconciliation")

	nr := nodeReconciler{
		conf:                  conf,
		virtualStorage:        s.virtual,
		targetStorage:         s.target,
		referenceStorage:      s.reference,
		disableReconciliation: !s.force,
	}

	if err := nr.reconcile(); err != nil {
		return fmt.Errorf("unable to reconcile: %s", err)
	}

	return nil
}

func getNodeAddress(cfg config.Config) (string, error) {
	switch {
	case cfg.SocketPath != "":
		return "unix://" + cfg.SocketPath, nil
	case cfg.ListenAddr != "":
		return "tcp://" + cfg.ListenAddr, nil
	default:
		return "", errors.New("no Praefect address configured")
	}
}

func (nr nodeReconciler) reconcile() error {
	if err := nr.validateArgs(); err != nil {
		return err
	}

	nodeAddr, err := getNodeAddress(nr.conf)
	if err != nil {
		return err
	}

	cc, err := subCmdDial(nodeAddr, nr.conf.Auth.Token)
	if err != nil {
		return err
	}

	pCli := gitalypb.NewPraefectInfoServiceClient(cc)

	if nr.disableReconciliation {
		log.Print("Performing a DRY RUN - no changes will be made until '-f' flag is provided")
	} else {
		log.Print("Performing a LIVE RUN - any repositories on target that are inconsistent with reference will be overwritten with the version present on reference")
	}

	request := &gitalypb.ConsistencyCheckRequest{
		VirtualStorage:         nr.virtualStorage,
		TargetStorage:          nr.targetStorage,
		ReferenceStorage:       nr.referenceStorage,
		DisableReconcilliation: nr.disableReconciliation,
	}
	stream, err := pCli.ConsistencyCheck(context.TODO(), request)
	if err != nil {
		return err
	}

	log.Print("Checking consistency...")
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
	var rStorage string
	var i uint

	for ; ; i++ {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if resp.ReferenceStorage != rStorage {
			rStorage = resp.ReferenceStorage
			log.Print("Reference storage being used: " + rStorage)
		}

		if len(resp.Errors) > 0 {
			var composedErrMsg bytes.Buffer
			for _, errMsg := range resp.Errors {
				composedErrMsg.WriteString("\t")
				composedErrMsg.WriteString(errMsg)
				composedErrMsg.WriteString("\n")
			}
			log.Printf("FAILURE: Internal error(s) occurred for the repo %s: %s", resp.GetRepoRelativePath(), composedErrMsg.String())
			continue
		}

		if resp.GetReferenceChecksum() == resp.GetTargetChecksum() {
			log.Print("CONSISTENT: " + resp.GetRepoRelativePath())
			continue
		}

		checksumPrint := func(checksum string) string {
			if checksum == "" {
				return "null"
			}
			return checksum
		}

		log.Printf(
			"INCONSISTENT: Repo %s has checksum %s on target but checksum %s on reference storage %s",
			resp.GetRepoRelativePath(),
			checksumPrint(resp.GetTargetChecksum()),
			checksumPrint(resp.GetReferenceChecksum()),
			resp.GetReferenceStorage(),
		)
		if resp.GetReplJobId() != 0 {
			log.Printf(
				"SCHEDULED: Replication job %d will update repo %s",
				resp.GetReplJobId(),
				resp.GetRepoRelativePath(),
			)
		}
	}

	log.Printf("FINISHED: %d repos were checked for consistency", i)
	return nil
}
