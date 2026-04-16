package shell

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
)

const (
	primaryPrompt   = "sq> "
	secondaryPrompt = "...> "
)

type Shell struct {
	instance  *readline.Instance
	printMode PrintMode
}

func New() (*Shell, error) {
	historyFile, err := ensureHistoryFile()
	if err != nil {
		return nil, err
	}

	instance, err := readline.NewEx(&readline.Config{
		Prompt:                 primaryPrompt,
		HistoryFile:            historyFile,
		DisableAutoSaveHistory: true,
		InterruptPrompt:        "\n",
		EOFPrompt:              "\n",
	})
	if err != nil {
		return nil, err
	}
	return &Shell{instance: instance}, nil
}

func ensureHistoryFile() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}

	historyFile := filepath.Join(homeDir, ".sq_history")
	file, err := os.OpenFile(historyFile, os.O_CREATE, 0o600)
	if err != nil {
		return "", fmt.Errorf("create history file: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close history file: %w", err)
	}

	return historyFile, nil
}

func isInterrupt(err error) bool {
	var interrupt *readline.InterruptError
	return errors.As(err, &interrupt) || errors.Is(err, readline.ErrInterrupt)
}

func shouldSubmit(input string) bool {
	input = strings.TrimSpace(input)
	if input == "" {
		return true
	}

	if strings.HasPrefix(input, ".") {
		return true
	}

	return strings.HasSuffix(input, ";")
}

func (s *Shell) Read() (string, error) {

	var sb strings.Builder
	for {
		line, err := s.instance.Readline()
		switch {
		case err == nil:
			trimmed := strings.TrimSpace(line)
			if trimmed == "" && sb.Len() == 0 {
				s.instance.SetPrompt(primaryPrompt)
				return sb.String(), nil
			}

			sb.WriteString(line)
			sb.WriteByte('\n')
			command := strings.TrimSpace(sb.String())

			if !shouldSubmit(command) {
				s.instance.SetPrompt(secondaryPrompt)
			} else {
				s.instance.SetPrompt(primaryPrompt)

				if err = s.instance.SaveHistory(command); err != nil {
					fmt.Printf("save history error: %v\n", err)
				}
				return command, nil
			}

		case isInterrupt(err):
			sb.Reset()
			s.instance.SetPrompt(primaryPrompt)
			continue
		case errors.Is(err, io.EOF):
			return "", io.EOF
		default:
			fmt.Fprintf(os.Stderr, "read input: %v\n", err)
			os.Exit(1)
		}
	}
}

func (s *Shell) SetPrintMode(mode PrintMode) {
	s.printMode = mode
}

func (s *Shell) PrintResult(headers []string, rows [][]string) {
	if len(headers) == 0 {
		fmt.Println("no rows found")
		return
	}

	var printer Printer
	switch s.printMode {
	case PrintModeTable:
		printer = NewTablePrinter()
	case PrintModeLine:
		printer = NewLinePrinter()
	case PrintModeCsv:
		printer = NewCsvPrinter()
	default:
		panic("unknown PrintMode")
	}
	defer printer.Close()

	printer.SetHeader(headers)
	err := printer.SetData(rows)
	if err != nil {
		fmt.Println("set data error:", err)
	}
	err = printer.Print()
	if err != nil {
		fmt.Println("print query error:", err)
	}
}

func (s *Shell) Close() error {
	return s.instance.Close()
}
