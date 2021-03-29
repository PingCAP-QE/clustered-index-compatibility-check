package main

import (
	"context"
	"database/sql"
	"flag"
	"io"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

var global struct {
	Action     string
	Threads    int
	Output     io.Writer
	Input      io.Reader
	DB         *sql.DB
	AdminCheck bool
}

func initGlobal() (teardown func() error) {
	flag.IntVar(&global.Threads, "threads", 16, "number of worker threads")
	flag.BoolVar(&global.AdminCheck, "admin-check", false, "do admin check table")
	input := flag.String("i", "a.out", "input file for check")
	output := flag.String("o", "a.out", "output file for setup")
	dsn := flag.String("dsn", "root:@tcp(127.0.0.1:4000)/test", "target data source name")
	flag.Parse()

	if flag.NArg() != 1 || !(flag.Arg(0) == "setup" || flag.Arg(0) == "check") {
		log.Fatalf("usage: %s [options] <setup|check>", os.Args[0])
	}
	global.Action = flag.Arg(0)

	if global.Action == "setup" {
		f, err := os.OpenFile(*output, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		if err != nil {
			log.Panic(err)
		}
		global.Output = f
		teardown = f.Close
	} else {
		f, err := os.Open(*input)
		if err != nil {
			log.Panic(err)
		}
		global.Input = f
		teardown = f.Close
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Panic(err)
	}
	global.DB = db
	return
}

func main() {
	initGlobal()
	ctx := context.Background()
	f := setup
	if global.Action == "check" {
		f = check
	}
	if err := f(ctx); err != nil {
		log.Fatal(err)
	}
}
