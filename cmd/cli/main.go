package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"HM3/internal/cli"
)

func main() {
	st := cli.NewState()

	in := bufio.NewScanner(os.Stdin)
	fmt.Println("hm3-cli (type 'help' or 'exit')")

	for {
		fmt.Print("> ")
		if !in.Scan() {
			return
		}

		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		if line == "exit" {
			return
		}

		if err := cli.ExecuteLine(st, line, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
	}
}
