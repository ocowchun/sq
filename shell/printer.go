package shell

import (
	"fmt"
	"os"

	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
)

type PrintMode uint8

const (
	PrintModeTable PrintMode = iota
	PrintModeLine
)

type Printer interface {
	SetHeader([]string)
	SetData([][]string) error
	Print() error
	Close() error
}

type TablePrinter struct {
	table *tablewriter.Table
}

func NewTablePrinter() *TablePrinter {
	table := tablewriter.NewTable(
		os.Stdout,
		tablewriter.WithConfig(tablewriter.Config{
			Row: tw.CellConfig{
				Formatting:   tw.CellFormatting{AutoWrap: tw.WrapTruncate}, // Wrap long content
				Alignment:    tw.CellAlignment{Global: tw.AlignLeft},       // Left-align rows
				ColMaxWidths: tw.CellWidth{Global: 100},
			},
		}),
	)
	return &TablePrinter{table}
}
func (p *TablePrinter) SetHeader(header []string) {
	p.table.Header(header)
}
func (p *TablePrinter) SetData(data [][]string) error {
	return p.table.Bulk(data)
}

func (p *TablePrinter) Print() error {
	return p.table.Render()
}
func (p *TablePrinter) Close() error {
	return p.table.Close()
}

type LinePrinter struct {
	header []string
	data   [][]string
}

func NewLinePrinter() *LinePrinter {
	return &LinePrinter{}
}
func (p *LinePrinter) SetHeader(header []string) {
	p.header = header
}
func (p *LinePrinter) SetData(data [][]string) error {
	if len(data) == 0 {
		return nil
	}

	if len(data[0]) != len(p.header) {
		message := fmt.Sprintf("header length mismatch: header has %d columns while has %d columns", len(p.header), len(data[0]))
		return fmt.Errorf(message)
	}
	p.data = data
	return nil
}

func (p *LinePrinter) Print() error {
	for _, row := range p.data {
		for i, cell := range row {
			fmt.Printf("%s = %s\n", p.header[i], cell)
		}
		fmt.Println()
	}
	return nil
}

func (p *LinePrinter) Close() error {
	return nil
}
