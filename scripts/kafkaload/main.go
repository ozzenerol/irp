// kafkaload sends records from N concurrent producers through the proxy. Each
// client is its own producer and sends one record at a time, waiting for the
// broker to ack it before the next. Every few seconds it logs how many records
// each client has sent and the average msg/s.
//
// The brokers must advertise the proxy's address (scripts/backends.sh does),
// otherwise the client connects to them directly after bootstrapping. Each
// client opens two connections, one for metadata and one for producing, and
// the proxy may send them to different brokers.
//
//	./scripts/kafkaload.sh --clients=100 --requests=10000
//	./scripts/kafkaload.sh --clients=10 --requests=1000 --size=65536
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

	"github.com/twmb/franz-go/pkg/kgo"
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
	clients := flag.Int("clients", 1, "number of concurrent producers")
	requests := flag.Int("requests", 100, "records per producer")
	host := flag.String("host", "", "proxy host (default: this machine's 192.168.1.x address)")
	port := flag.Int("port", 0, "proxy port (default: proxy.listen_port in kafka-config.json)")
	topic := flag.String("topic", "load", "topic to produce to")
	size := flag.Int("size", 64, "record value size in bytes")
	timeout := flag.Duration("timeout", 10*time.Second, "how long to wait for each ack")
	every := flag.Duration("every", 5*time.Second, "how often to log")
	flag.Parse()

	if *clients < 1 || *requests < 1 || *size < 1 {
		fail("--clients, --requests and --size must be positive")
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
	fmt.Printf("records:  %d (%d per client, %d bytes, topic %s)\n", *clients**requests, *requests, *size, *topic)
	fmt.Println()

	addr := net.JoinHostPort(*host, fmt.Sprint(*port))
	value := []byte(strings.Repeat("x", *size))

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
			if err := c.run(ctx, addr, *topic, value, *requests, *timeout); err != nil && ctx.Err() == nil {
				c.err.Store(err.Error())
			}
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

// Produces the records one at a time with acks from all in-sync replicas.
// Idempotent writes are off: the producer ID comes from whichever broker the
// proxy picked for that request, and the others would reject it.
func (c *client) run(ctx context.Context, addr, topic string, value []byte, requests int, timeout time.Duration) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerLinger(0),
		kgo.RecordDeliveryTimeout(timeout),
		kgo.ClientID(fmt.Sprintf("kafkaload-%d", c.id)),
	)
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}
	defer cl.Close()

	for i := 0; i < requests; i++ {
		if ctx.Err() != nil {
			return nil
		}
		if err := cl.ProduceSync(ctx, &kgo.Record{Value: value}).FirstErr(); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.failed.Add(1)
			c.err.Store(err.Error())
			continue
		}
		c.sent.Add(1)
	}
	return nil
}

// Logs each client's count and the msg/s since the last report and overall.
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
		line := fmt.Sprintf("client %3d  %8d sent  +%-6d %7.0f msg/s  %s", c.id, sent, sent-c.last, float64(sent-c.last)/window, state)
		if e, ok := c.err.Load().(string); ok {
			line += "  last error: " + e
		}
		fmt.Println(line)
		c.last = sent
	}
	fmt.Printf("total %d sent, %d failed, %d/%d clients running | now %.0f msg/s | average %.0f msg/s\n\n",
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
	fmt.Printf("\ndone in %s: %d sent, %d failed, %d clients with errors, average %.0f msg/s\n",
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

// proxy.listen_port from the repo's kafka-config.json, found by walking up from here.
func configPort() int {
	dir, _ := os.Getwd()
	for {
		data, err := os.ReadFile(filepath.Join(dir, "kafka-config.json"))
		if err == nil {
			var cfg struct {
				Proxy struct {
					ListenPort int `json:"listen_port"`
				} `json:"proxy"`
			}
			if err := json.Unmarshal(data, &cfg); err != nil || cfg.Proxy.ListenPort == 0 {
				fail("kafka-config.json has no proxy.listen_port, pass --port")
			}
			return cfg.Proxy.ListenPort
		} else if !errors.Is(err, os.ErrNotExist) {
			fail(err.Error())
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			fail("kafka-config.json not found, pass --port")
		}
		dir = parent
	}
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
