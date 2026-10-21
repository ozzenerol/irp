// rabbitload sends messages from N concurrent clients through the proxy. Each
// client opens one AMQP connection, declares its own queue, consumes from it
// and publishes to it with confirms. A request is one round trip: publish,
// broker confirm, and the broker pushing the message back to the consumer.
// Every few seconds it logs how many messages each client has sent and the
// average msg/s.
//
//	./scripts/rabbitload.sh --clients=100 --requests=10000
//	./scripts/rabbitload.sh --clients=10 --requests=1000 --size=65536
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

	amqp "github.com/rabbitmq/amqp091-go"
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
	requests := flag.Int("requests", 100, "messages per client")
	host := flag.String("host", "", "proxy host (default: this machine's 192.168.1.x address)")
	port := flag.Int("port", 0, "proxy port (default: proxy.listen_port in rabbitmq-config.json)")
	size := flag.Int("size", 64, "message body size in bytes")
	timeout := flag.Duration("timeout", 10*time.Second, "how long to wait for each confirm and delivery")
	every := flag.Duration("every", 5*time.Second, "how often to log")
	user := flag.String("user", "irp", "rabbitmq user")
	password := flag.String("password", "irp", "rabbitmq password")
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
	fmt.Printf("messages: %d (%d per client, %d bytes)\n", *clients**requests, *requests, *size)
	fmt.Println()

	url := fmt.Sprintf("amqp://%s:%s@%s/", *user, *password, net.JoinHostPort(*host, fmt.Sprint(*port)))
	body := []byte(strings.Repeat("x", *size))

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
			if err := c.run(ctx, url, body, *requests, *timeout); err != nil && ctx.Err() == nil {
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

// Connects and sends the messages one at a time, waiting for the confirm and
// the delivery of each before the next. Gives up on the first broken
// connection, since AMQP has no way to carry on without reconnecting.
func (c *client) run(ctx context.Context, url string, body []byte, requests int, timeout time.Duration) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("confirm: %w", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	// Exclusive: the broker deletes it when the connection closes.
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return fmt.Errorf("declare: %w", err)
	}
	deliveries, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	for i := 0; i < requests; i++ {
		if ctx.Err() != nil {
			return nil
		}
		if err := ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{Body: body}); err != nil {
			return fmt.Errorf("publish: %w", err)
		}

		wait := time.NewTimer(timeout)
		select {
		case confirm, ok := <-confirms:
			if !ok {
				wait.Stop()
				return errors.New("connection closed waiting for confirm")
			}
			if !confirm.Ack {
				c.failed.Add(1)
				c.err.Store("broker nacked a message")
				wait.Stop()
				continue
			}
		case <-wait.C:
			return errors.New("timed out waiting for confirm")
		case <-ctx.Done():
			wait.Stop()
			return nil
		}

		select {
		case d, ok := <-deliveries:
			wait.Stop()
			if !ok {
				return errors.New("connection closed waiting for delivery")
			}
			if len(d.Body) != len(body) {
				c.failed.Add(1)
				c.err.Store(fmt.Sprintf("got %d bytes back, sent %d", len(d.Body), len(body)))
				continue
			}
		case <-wait.C:
			return errors.New("timed out waiting for delivery")
		case <-ctx.Done():
			wait.Stop()
			return nil
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

// proxy.listen_port from the repo's rabbitmq-config.json, found by walking up from here.
func configPort() int {
	dir, _ := os.Getwd()
	for {
		data, err := os.ReadFile(filepath.Join(dir, "rabbitmq-config.json"))
		if err == nil {
			var cfg struct {
				Proxy struct {
					ListenPort int `json:"listen_port"`
				} `json:"proxy"`
			}
			if err := json.Unmarshal(data, &cfg); err != nil || cfg.Proxy.ListenPort == 0 {
				fail("rabbitmq-config.json has no proxy.listen_port, pass --port")
			}
			return cfg.Proxy.ListenPort
		} else if !errors.Is(err, os.ErrNotExist) {
			fail(err.Error())
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			fail("rabbitmq-config.json not found, pass --port")
		}
		dir = parent
	}
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
