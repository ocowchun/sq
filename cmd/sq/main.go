package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/ocowchun/sq/queryexec"
	"github.com/ocowchun/sq/shell"
)

const VERSION = "v0.0.4"

func main() {
	versionFlag := flag.Bool("version", false, "print version and exit")
	query := flag.String("e", "", "execute one SQL statement and exit")
	profile := flag.String("profile", "", "aws profile")
	flag.Parse()

	if *versionFlag {
		fmt.Printf("sq %s\n", VERSION)
		return
	}

	loadOptions := make([]func(*config.LoadOptions) error, 0)
	ctx := context.Background()
	if profile != nil && *profile != "" {
		loadOptions = append(loadOptions, config.WithSharedConfigProfile(*profile))
	}
	awsConfig, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		fmt.Printf("failed to load aws config: %v\n", err)
		os.Exit(1)
	}

	engine := queryexec.New(awsConfig)

	if query != nil {
		res := runQuery(engine, *query)
		if res.Error != nil {
			fmt.Println(res.Error)
			os.Exit(1)
		}
		printer := shell.NewCsvPrinter()
		defer printer.Close()
		printer.SetHeader(res.Header)
		err := printer.SetData(res.Rows)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		err = printer.Print()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		return
	}

	s, err := shell.New()
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	for {
		input, err := s.Read()
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Fatal(err)
		}
		if strings.HasPrefix(input, ".") {
			HandleCommand(input, s)
			continue
		}

		res := runQuery(engine, input)
		if res.Error != nil {
			fmt.Println(res.Error)
			continue
		}

		s.PrintResult(res.Header, res.Rows)
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
		case "csv":
			s.SetPrintMode(shell.PrintModeCsv)
		default:
			fmt.Printf("Unknown mode: `%s`\n", strs[1])
		}
	default:
		fmt.Printf("unknown command: `%s`", strs[0])
	}
}

type Response struct {
	Header []string
	Rows   [][]string
	Error  error
}

func runQuery(engine *queryexec.QueryExec, sql string) Response {
	iter, err := engine.Query(context.Background(), sql)
	if err != nil {
		return Response{
			Error: err,
		}
	}
	defer iter.Close()

	headers := make([]string, 0)
	rows := make([][]string, 0)
	for {
		ctx := context.Background()
		batch, err := iter.Next(ctx)
		if err != nil {
			return Response{
				Error: err,
			}
		}

		if len(headers) == 0 {
			for _, f := range batch.Schema().Fields() {
				headers = append(headers, f.Name)
			}
		}

		for i := 0; i < int(batch.NumRows()); i++ {
			row := make([]string, len(batch.Columns()))
			for j, column := range batch.Columns() {
				row[j] = column.ValueStr(i)
			}
			rows = append(rows, row)
		}

		if !iter.HasNext() {
			break
		}
	}

	return Response{
		Header: headers,
		Rows:   rows,
	}
}
