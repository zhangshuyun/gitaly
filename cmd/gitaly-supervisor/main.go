package main

import (
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

func main() {

	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		log.Fatal(err)
	}
	cmd := exec.Command(os.Args[1], os.Args[2])
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	cmd.ExtraFiles = []*os.File{pipeR}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	if err := pipeR.Close(); err != nil {
		log.Fatal(err)
	}
	go func() {
		time.Sleep(time.Second)
		for range time.NewTicker(100 * time.Millisecond).C {
			if _, err := pipeW.Write([]byte{0}); err != nil {
				log.Fatal("heartbeat write failed: %v", err)
			}
		}
	}()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM)
	go func() {
		<-ch
		process := cmd.Process
		if process == nil {
			return
		}
		syscall.Kill(process.Pid, syscall.SIGHUP)
	}()
	log.Print(cmd.Wait())
}
