package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime/pprof"
	"sort"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/git/packfile"
)

var (
	idxFileRegex = regexp.MustCompile(`\A(.*/pack-)([0-9a-f]{40})\.idx\z`)
	printMap     = flag.Bool("map", false, "output map visualization of packfile")
)

func main() {
	flag.Parse()

	if len(flag.Args()) != 1 {
		log.Fatal("usage: parse-bitmap [-map] PACK_IDX")
	}

	if err := _main(flag.Arg(0)); err != nil {
		log.Fatal(err)
	}
}

func _main(packIdx string) error {
	if pprofOut := os.Getenv("PPROF_OUT"); len(pprofOut) > 0 {
		f, err := os.Create(pprofOut)
		if err != nil {
			return err
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	start := time.Now()
	idx, err := packfile.ReadIndex(packIdx)
	if err != nil {
		return err
	}
	log.Printf("loaded idx with %d objects in %v", len(idx.Objects), time.Since(start))

	start = time.Now()
	if err := idx.LoadBitmap(); err != nil {
		return err
	}
	log.Printf("loaded bitmap in %v", time.Since(start))

	start = time.Now()
	// Sort objects by pack offset, because the object type bitmaps use
	// packfile order.
	packObjects := make([]*packfile.Object, len(idx.Objects))
	copy(packObjects, idx.Objects)

	sort.Sort(packfile.PackfileOrder(packObjects))
	log.Printf("sorted object list in %v", time.Since(start))

	start = time.Now()
	for _, t := range []struct {
		objectType packfile.ObjectType
		ewah       *packfile.EWAH
	}{
		{packfile.TCommit, idx.Bitmap.Commits},
		{packfile.TTree, idx.Bitmap.Trees},
		{packfile.TBlob, idx.Bitmap.Blobs},
		{packfile.TTag, idx.Bitmap.Tags},
	} {
		setFunc := func(i int) error {
			obj := packObjects[i]
			if obj.Type != packfile.TUnknown {
				return fmt.Errorf("type already set for object %v", obj)
			}

			obj.Type = t.objectType

			return nil
		}

		if err := t.ewah.Scan(setFunc); err != nil {
			return err
		}
	}
	log.Printf("labeled objects in %v", time.Since(start))

	for _, obj := range packObjects {
		if obj.Type == packfile.TUnknown {
			return fmt.Errorf("object missing type label: %v", obj)
		}
	}

	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	fmt.Fprintf(out, "# pack-%s\n", idx.ID)

	for _, o := range packObjects {
		if *printMap {
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

			fmt.Fprint(out, c+" ")
		} else {
			fmt.Fprintln(out, o)
		}
	}

	return nil
}
