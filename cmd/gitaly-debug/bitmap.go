package main

import (
	"bufio"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/packfile"
)

func listBitmapPack(idxFile string) {
	idx, err := packfile.ReadIndex(idxFile)
	noError(err)

	noError(idx.LabelObjectTypes())

	out := bufio.NewWriter(os.Stdout)
	defer flush(out)
	fprintf(out, "# pack-%s\n", idx.ID)

	for _, o := range idx.PackfileOrder {
		fprintln(out, o)
	}
}

func mapBitmapPack(idxFile string) {
	idx, err := packfile.ReadIndex(idxFile)
	noError(err)

	noError(idx.LabelObjectTypes())

	out := bufio.NewWriter(os.Stdout)
	defer flush(out)
	fprintf(out, "# pack-%s\n", idx.ID)
	// Use a mix of lower and upper case that is easier to distinguish than all upper / all lower.
	fprintln(out, "# b: blob, C: commit, e: tree, T: tag")

	for _, o := range idx.PackfileOrder {
		c := ""
		switch o.Type {
		case packfile.TBlob:
			c = "b"
		case packfile.TCommit:
			c = "C"
		case packfile.TTree:
			c = "e"
		case packfile.TTag:
			c = "T"
		}

		fprint(out, c+" ")
	}
}

func listBitmapCommits(idxFile string) {
	idx, err := packfile.ReadIndex(idxFile)
	noError(err)

	noError(idx.LabelObjectTypes())

	out := bufio.NewWriter(os.Stdout)
	defer flush(out)
	fprintf(out, "# pack-%s\n", idx.ID)

	for i := 0; i < idx.IndexBitmap.NumBitmapCommits(); i++ {
		bc, err := idx.IndexBitmap.BitmapCommit(i)
		noError(err)
		fprintln(out, bc.OID)
	}
}

func listBitmapReachable(idxFile string, commitID string) {
	idx, err := packfile.ReadIndex(idxFile)
	noError(err)

	noError(idx.LabelObjectTypes())

	var bc *packfile.BitmapCommit
	for i := 0; i < idx.IndexBitmap.NumBitmapCommits(); i++ {
		var err error
		bc, err = idx.BitmapCommit(i)
		noError(err)
		if bc.OID == commitID {
			break
		}
	}

	if bc == nil || bc.OID != commitID {
		fatal(fmt.Errorf("bitmap commit not found: %s", commitID))
	}

	out := bufio.NewWriter(os.Stdout)
	defer flush(out)
	fprintf(out, "# pack-%s\n", idx.ID)
	fprintf(out, "# bitmap commit %s\n", bc.OID)

	noError(bc.Bitmap.Scan(func(i int) error {
		_, err := fmt.Fprintln(out, idx.PackfileOrder[i])
		return err
	}))
}
