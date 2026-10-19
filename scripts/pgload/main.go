// pgload sends requests from N concurrent clients to the proxy. Each client
// keeps one connection and sends its requests over it, or with --pool the
// clients share a pool of connections the way an application does. Every few
// seconds it logs how many requests each client has sent and the average req/s.
//
//	go run -C scripts/pgload . --clients=100 --requests=10000
//	go run -C scripts/pgload . --clients=100 --requests=10000 --pool=10
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// A connection or a pool: each request runs on one of them.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

type client struct {
	id     int
	sent   atomic.Int64
	failed atomic.Int64
	done   atomic.Bool
	err    atomic.Value // string
	last   int64        // sent at the previous report
}

func main() {
	clients := flag.Int("clients", 1, "number of concurrent clients")
	requests := flag.Int("requests", 100, "requests per client")
	host := flag.String("host", "", "proxy host (default: this machine's 192.168.1.x address)")
	port := flag.Int("port", 0, "proxy port (default: proxy.listen_port in config.json)")
	pool := flag.Int("pool", 0, "share a pool of this many connections between the clients (default: one connection per client)")
	query := flag.String("query", "SELECT 1", "query each request runs")
	every := flag.Duration("every", 5*time.Second, "how often to log")
	user := flag.String("user", "irp", "postgres user")
	password := flag.String("password", "irp", "postgres password")
	db := flag.String("db", "irp", "postgres database")
	flag.Parse()

	if *clients < 1 || *requests < 1 {
		fail("--clients and --requests must be positive")
	}
	if *pool < 0 {
		fail("--pool can't be negative")
	}
	if *host == "" {
		*host = lanAddress()
	}
	if *port == 0 {
		*port = configPort()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("proxy:    %s:%d\n", *host, *port)
	fmt.Printf("clients:  %d\n", *clients)
	fmt.Printf("requests: %d (%d per client)\n", *clients**requests, *requests)
	if *pool > 0 {
		fmt.Printf("pool:     %d connections shared by the clients\n", *pool)
	} else {
		fmt.Printf("pool:     none, one connection per client\n")
	}
	fmt.Println()

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", *user, *password, *host, *port, *db)
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		fail(err.Error())
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	var shared *pgxpool.Pool
	if *pool > 0 {
		pcfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			fail(err.Error())
		}
		pcfg.ConnConfig = cfg
		pcfg.MaxConns = int32(*pool)
		shared, err = pgxpool.NewWithConfig(ctx, pcfg)
		if err != nil {
			fail(err.Error())
		}
		defer shared.Close()
	}

	all := make([]*client, *clients)
	var wg sync.WaitGroup
	start := time.Now()
	for i := range all {
		c := &client{id: i}
		all[i] = c
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer c.done.Store(true)
			if shared != nil {
				c.send(ctx, shared, *query, *requests)
				return
			}
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				c.err.Store("connect: " + err.Error())
				return
			}
			defer conn.Close(context.Background())
			c.send(ctx, conn, *query, *requests)
		}()
	}

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

	tick := time.NewTicker(*every)
	defer tick.Stop()
	prevTotal, prevAt := int64(0), start
	for {
		select {
		case <-finished:
			summary(all, start)
			return
		case <-ctx.Done():
			<-finished
			summary(all, start)
			return
		case now := <-tick.C:
			prevTotal = report(all, start, now, prevAt, prevTotal)
			prevAt = now
		}
	}
}

// Runs the query requests times. A pool takes a free connection for each
// request, waiting when all of them are busy.
func (c *client) send(ctx context.Context, db execer, query string, requests int) {
	for i := 0; i < requests; i++ {
		if _, err := db.Exec(ctx, query); err != nil {
			if ctx.Err() != nil {
				return
			}
			c.failed.Add(1)
			c.err.Store(err.Error())
			if conn, ok := db.(*pgx.Conn); ok && conn.IsClosed() {
				return
			}
			continue
		}
		c.sent.Add(1)
	}
}

// Logs each client's count and the req/s since the last report and overall.
// Gives the total so the next report can work out its rate.
func report(all []*client, start, now, prevAt time.Time, prevTotal int64) int64 {
	elapsed := now.Sub(start).Seconds()
	window := now.Sub(prevAt).Seconds()
	var total, failed int64
	running := 0
	fmt.Printf("--- %s (%.0fs)\n", now.Format("15:04:05"), elapsed)
	for _, c := range all {
		sent := c.sent.Load()
		total += sent
		failed += c.failed.Load()
		state := "running"
		if c.done.Load() {
			state = "done"
		} else {
			running++
		}
		line := fmt.Sprintf("client %3d  %8d sent  +%-6d %7.0f req/s  %s", c.id, sent, sent-c.last, float64(sent-c.last)/window, state)
		if e, ok := c.err.Load().(string); ok {
			line += "  last error: " + e
		}
		fmt.Println(line)
		c.last = sent
	}
	fmt.Printf("total %d sent, %d failed, %d/%d clients running | now %.0f req/s | average %.0f req/s\n\n",
		total, failed, running, len(all), float64(total-prevTotal)/window, float64(total)/elapsed)
	return total
}

func summary(all []*client, start time.Time) {
	elapsed := time.Since(start)
	var total, failed int64
	broken := 0
	for _, c := range all {
		total += c.sent.Load()
		failed += c.failed.Load()
		if e, ok := c.err.Load().(string); ok {
			broken++
			fmt.Printf("client %d: %s\n", c.id, e)
		}
	}
	fmt.Printf("\ndone in %s: %d sent, %d failed, %d clients with errors, average %.0f req/s\n",
		elapsed.Round(time.Millisecond), total, failed, broken, float64(total)/elapsed.Seconds())
}

// This machine's 192.168.1.x address, where the proxy listens.
func lanAddress() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fail(err.Error())
	}
	for _, a := range addrs {
		if ip, ok := a.(*net.IPNet); ok && ip.IP.To4() != nil && strings.HasPrefix(ip.IP.String(), "192.168.1.") {
			return ip.IP.String()
		}
	}
	fail("no 192.168.1.x address found on this machine, pass --host")
	return ""
}

// proxy.listen_port from the repo's config.json, found by walking up from here.
func configPort() int {
	dir, _ := os.Getwd()
	for {
		data, err := os.ReadFile(filepath.Join(dir, "config.json"))
		if err == nil {
			var cfg struct {
				Proxy struct {
					ListenPort int `json:"listen_port"`
				} `json:"proxy"`
			}
			if err := json.Unmarshal(data, &cfg); err != nil || cfg.Proxy.ListenPort == 0 {
				fail("config.json has no proxy.listen_port, pass --port")
			}
			return cfg.Proxy.ListenPort
		} else if !errors.Is(err, os.ErrNotExist) {
			fail(err.Error())
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			fail("config.json not found, pass --port")
		}
		dir = parent
	}
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
