package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/pkg/errors"
	"github.com/zyguan/sqlz/resultset"
	"golang.org/x/sync/errgroup"
)

type TestTable struct {
	Name   string
	Create []string
	Insert []string
}

func check(ctx context.Context) error {
	threads := global.Threads
	if threads <= 0 {
		threads = 1
	}
	g, ctx := errgroup.WithContext(ctx)
	digests := make(chan [2]string)
	for i := 0; i < threads; i++ {
		t := checkTask{ctx: ctx, db: global.DB, digests: digests}
		g.Go(t.run)
	}
	cnt := 0
	in := bufio.NewScanner(global.Input)
loop:
	for in.Scan() {
		line := in.Text()
		tuple := strings.SplitN(line, " ", 2)
		if len(tuple) != 2 {
			return errors.Errorf("malformed input line: %q", line)
		}
		select {
		case digests <- [2]string{tuple[0], tuple[1]}:
			cnt += 1
			if cnt%100 == 0 {
				log.Printf("%6d tables checked", cnt)
			}
		case <-ctx.Done():
			break loop
		}
	}
	close(digests)
	return g.Wait()
}

func setup(ctx context.Context) error {
	threads := global.Threads
	if threads <= 0 {
		threads = 1
	}
	g, ctx := errgroup.WithContext(ctx)
	tests := genTests()
	tasks := make([]setupTask, threads)
	results := make(chan [2]string)
	done := make(chan struct{})
	for i := 0; i < threads; i++ {
		tasks[i] = setupTask{ctx: ctx, db: global.DB, tests: tests, results: results}
		g.Go(tasks[i].run)
	}
	go func() {
		defer close(done)
		cnt := 0
		for r := range results {
			fmt.Fprintln(global.Output, r[0], r[1])
			cnt += 1
			if cnt%100 == 0 {
				log.Printf("%6d tables created", cnt)
			}
		}
	}()
	if err := g.Wait(); err != nil {
		return err
	}
	close(results)
	<-done
	return nil
}

type checkTask struct {
	ctx     context.Context
	db      *sql.DB
	digests <-chan [2]string
}

func (t *checkTask) run() error {
	var (
		digest [2]string
		ok     bool
	)
	for {
		select {
		case <-t.ctx.Done():
			return t.ctx.Err()
		case digest, ok = <-t.digests:
			if !ok {
				return nil
			}
		}
		name, actual := digest[0], digest[1]
		if global.AdminCheck {
			_, err := t.db.ExecContext(t.ctx, "admin check table "+name)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		rs, err := readTable(t.ctx, t.db, name)
		if err != nil {
			return errors.WithStack(err)
		}
		expect := rs.DataDigest(resultset.DigestOptions{Sort: true})
		if expect != actual {
			return errors.Errorf("data digest of table %s changed: %s -> %s", name, expect, actual)
		}
	}
}

type setupTask struct {
	ctx     context.Context
	db      *sql.DB
	tests   <-chan TestTable
	results chan [2]string
}

func (t *setupTask) run() error {
	var (
		tt TestTable
		ok bool
	)
	for {
		select {
		case <-t.ctx.Done():
			return t.ctx.Err()
		case tt, ok = <-t.tests:
			if !ok {
				return nil
			}
		}
		for _, q := range tt.Create {
			if _, err := t.db.ExecContext(t.ctx, q); err != nil {
				return errors.Wrap(err, "create table "+tt.Name)
			}
		}
		for _, q := range tt.Insert {
			if _, err := t.db.ExecContext(t.ctx, q); err != nil {
				return errors.Wrap(err, "insert data to "+tt.Name)
			}
		}
		if global.AdminCheck {
			_, err := t.db.ExecContext(t.ctx, "admin check table "+tt.Name)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		rs, err := readTable(t.ctx, t.db, tt.Name)
		if err != nil {
			return errors.WithStack(err)
		}
		t.results <- [2]string{tt.Name, rs.DataDigest(resultset.DigestOptions{Sort: true})}
	}
}

func readTable(ctx context.Context, db *sql.DB, name string) (*resultset.ResultSet, error) {
	rows, err := db.QueryContext(ctx, "select * from "+name)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()
	rs, err := resultset.ReadFromRows(rows)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rs, nil
}
