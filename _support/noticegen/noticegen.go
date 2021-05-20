package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"text/template"
)

var (
	sourcePath = flag.String("source", "", "directory path containing license files")
	tmplPath   = flag.String("template", "", "file path to notice template")
)

type license struct {
	Filename string
	Path     string
	Text     string
}

func main() {
	flag.Parse()

	if *sourcePath == "" || *tmplPath == "" {
		log.Fatal("must provide flags 'source' and 'template'")
	}

	tmpl, err := template.ParseFiles(*tmplPath)
	if err != nil {
		log.Fatal(err)
	}

	var licenses []license

	data, err := exec.Command("go", "mod", "edit", "-json").Output()
	if err != nil {
		log.Fatal(err)
	}
	var modInfo = struct {
		Module struct {
			Path string
		}
	}{}
	if err := json.Unmarshal(data, &modInfo); err != nil {
		log.Fatal(err)
	}

	if err := filepath.Walk(*sourcePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		p, err := filepath.Rel(*sourcePath, filepath.Dir(path))
		if err != nil {
			log.Fatal(err)
		}

		if p == modInfo.Module.Path {
			return nil
		}

		t, err := ioutil.ReadFile(path)
		if err != nil {
			log.Fatal(err)
		}

		licenses = append(licenses, license{
			Filename: filepath.Base(path),
			Path:     p,
			Text:     string(t),
		})

		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err := tmpl.Execute(os.Stdout, licenses); err != nil {
		log.Fatal(err)
	}
}
