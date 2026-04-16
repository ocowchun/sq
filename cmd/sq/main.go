package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/ocowchun/sq/queryexec"
	"github.com/ocowchun/sq/shell"
)

func main() {
	s, err := shell.New()
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	engine := queryexec.New()
	for {
		input, err := s.Read()
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Fatal(err)
		}
		fmt.Printf("input %s \n", input)
		if strings.HasPrefix(input, ".") {
			fmt.Printf("command: %s\n", input)
			HandleCommand(input, s)
			continue
		}

		iter, err := engine.Query(context.Background(), input)
		if err != nil {
			fmt.Println("query error:", err)
			continue
		}
		s.PrintResult(iter)
		err = iter.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func HandleCommand(command string, s *shell.Shell) {
	strs := strings.Split(command, " ")
	switch strs[0] {
	case ".mode":
		switch strs[1] {
		case "table":
			s.SetPrintMode(shell.PrintModeTable)
		case "line":
			s.SetPrintMode(shell.PrintModeLine)
		default:
			fmt.Printf("Unknown mode: `%s`\n", strs[1])
		}
	default:
		fmt.Printf("unknown command: `%s`", strs[0])
	}
}
