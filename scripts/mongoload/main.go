// mongoload sends requests from N concurrent clients to the proxy. Each client
// keeps one connection and sends its requests over it, or with --pool the
// clients share a pool of connections the way an application does. Every few
// seconds it logs how many requests each client has sent and the average req/s.
//
//	./scripts/mongoload.sh --clients=100 --requests=10000
//	./scripts/mongoload.sh --clients=100 --requests=10000 --pool=10
//	./scripts/mongoload.sh --op=insert
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

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

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
	port := flag.Int("port", 0, "proxy port (default: proxy.listen_port in mongo-config.json)")
	pool := flag.Int("pool", 0, "share a pool of this many connections between the clients (default: one connection per client)")
	op := flag.String("op", "ping", "what each request does: ping, find or insert")
	every := flag.Duration("every", 5*time.Second, "how often to log")
	db := flag.String("db", "irp", "mongo database")
	flag.Parse()

	if *clients < 1 || *requests < 1 {
		fail("--clients and --requests must be positive")
	}
	if *pool < 0 {
		fail("--pool can't be negative")
	}
	if *op != "ping" && *op != "find" && *op != "insert" {
		fail("--op must be ping, find or insert")
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
	fmt.Printf("requests: %d (%d per client, %s)\n", *clients**requests, *requests, *op)
	if *pool > 0 {
		fmt.Printf("pool:     %d connections shared by the clients\n", *pool)
	} else {
		fmt.Printf("pool:     none, one connection per client\n")
	}
	fmt.Println()

	// Direct, so the driver never goes around the proxy to the address the
	// server reports. Polling keeps it to one monitoring connection per
	// mongo.Client instead of two. No retries, so every failure is counted.
	uri := fmt.Sprintf("mongodb://%s/?directConnection=true", net.JoinHostPort(*host, fmt.Sprint(*port)))
	newMongo := func(size int) (*mongo.Client, error) {
		return mongo.Connect(options.Client().
			ApplyURI(uri).
			SetMaxPoolSize(uint64(size)).
			SetServerMonitoringMode(options.ServerMonitoringModePoll).
			SetRetryReads(false).
			SetRetryWrites(false))
	}

	// With --pool the clients share one mongo.Client, otherwise each client
	// gets its own with a single connection.
	var shared *mongo.Client
	if *pool > 0 {
		var err error
		if shared, err = newMongo(*pool); err != nil {
			fail(err.Error())
		}
		defer shared.Disconnect(context.Background())
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
			m := shared
			if m == nil {
				var err error
				if m, err = newMongo(1); err != nil {
					c.err.Store("connect: " + err.Error())
					return
				}
				defer m.Disconnect(context.Background())
				if err := m.Ping(ctx, nil); err != nil {
					c.err.Store("connect: " + err.Error())
					return
				}
			}
			c.send(ctx, m.Database(*db), *op, *requests)
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

// Runs the op requests times. A pool takes a free connection for each
// request, waiting when all of them are busy.
func (c *client) send(ctx context.Context, db *mongo.Database, op string, requests int) {
	coll := db.Collection("load")
	for i := 0; i < requests; i++ {
		var err error
		switch op {
		case "ping":
			err = db.RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Err()
		case "find":
			err = coll.FindOne(ctx, bson.D{{Key: "_id", Value: 42}}).Err()
			if errors.Is(err, mongo.ErrNoDocuments) {
				err = nil
			}
		case "insert":
			_, err = coll.InsertOne(ctx, bson.D{{Key: "client", Value: c.id}, {Key: "n", Value: i}})
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.failed.Add(1)
			c.err.Store(err.Error())
			if errors.Is(err, mongo.ErrClientDisconnected) {
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

// proxy.listen_port from the repo's mongo-config.json, found by walking up from here.
func configPort() int {
	dir, _ := os.Getwd()
	for {
		data, err := os.ReadFile(filepath.Join(dir, "mongo-config.json"))
		if err == nil {
			var cfg struct {
				Proxy struct {
					ListenPort int `json:"listen_port"`
				} `json:"proxy"`
			}
			if err := json.Unmarshal(data, &cfg); err != nil || cfg.Proxy.ListenPort == 0 {
				fail("mongo-config.json has no proxy.listen_port, pass --port")
			}
			return cfg.Proxy.ListenPort
		} else if !errors.Is(err, os.ErrNotExist) {
			fail(err.Error())
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			fail("mongo-config.json not found, pass --port")
		}
		dir = parent
	}
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
