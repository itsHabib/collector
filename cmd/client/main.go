package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/itsHabib/collector/internal/api"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cactus/go-statsd-client/v5/statsd"
)

func main() {
	// Initialize a statsd client
	config := &statsd.ClientConfig{
		Address: "127.0.0.1:8125",
	}
	client, err := statsd.NewClientWithConfig(config)
	if err != nil {
		log.Fatalf("Could not initialize statsd client: %v", err)
	}
	defer client.Close()

	const numWorkers = 25
	var failedQueries int
	var doneWorkers int
	g := new(errgroup.Group)
	c := make(chan string)
	q := make(chan struct{})
	counter := make(map[string]int)
	d := make(chan struct{})
	doneW := make(chan struct{})
	go func() {
		for {
			select {
			case s := <-c:
				counter[s]++
			case <-d:
				return
			case <-q:
				failedQueries++
			case _, ok := <-doneW:
				if !ok {
					return
				}
				doneWorkers++
				if doneWorkers == numWorkers {
					close(d)
					return
				}
			}
		}
	}()

	n := time.Now()
	for i := 0; i < numWorkers; i++ {
		g.Go(func() error {
			return worker(client, c, doneW)
		})
	}

	const numQueryWorkers = 20
	for i := 0; i < numQueryWorkers; i++ {
		var do func()
		if i%2 == 0 {
			do = query
		} else {
			do = queryRange
		}
		g.Go(func() error {
			queryWorker(d, do)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		log.Fatalf("Could not run workers: %v", err)
	}

	var sum int
	for k, v := range counter {
		fmt.Printf("%s: %d\n", k, v)
		sum += v
	}
	fmt.Println("Unique Metrics sent!, len:", len(counter))
	fmt.Println("failed queries:", failedQueries)
	fmt.Println("sum:", sum)
	fmt.Println("time:", time.Since(n).Seconds(), "seconds")
}

func worker(client statsd.Statter, c chan<- string, done chan<- struct{}) error {
	count := 200000
	uniqueMetrics := 1000
	for i := 0; i < count; i++ {
		metricID := rand.Intn(uniqueMetrics)

		// Create metric name and tags
		metricName := fmt.Sprintf("metric.%d", metricID)
		tag1 := [2]string{"region", fmt.Sprintf("region%d", metricID%10)}
		tag2 := [2]string{"version", fmt.Sprintf("v%d", metricID%5)}

		// Increment counter and set gauge
		counterName := metricName + ".counter"
		gaugeName := metricName + ".gauge"

		err := client.Inc(counterName, 1, 1.0, tag1, tag2)
		if err == nil {
			c <- counterName + "&" + tag1[0] + "=" + tag1[1] + "," + tag2[0] + "=" + tag2[1] + "@" + "c"
		}
		err = client.Gauge(gaugeName, int64(i%2), 1.0, tag1, tag2)
		if err == nil {
			c <- gaugeName + "&" + tag1[0] + "=" + tag1[1] + "," + tag2[0] + "=" + tag2[1] + "@" + "g"
		}

		time.Sleep(time.Duration(rand.Intn(10)) + time.Millisecond)
	}
	done <- struct{}{}

	return nil
}

func queryWorker(d <-chan struct{}, do func()) {
	timer := time.NewTimer(time.Duration(rand.Intn(1000)) * time.Millisecond)
	const interval = time.Second * 5
	for {
		select {
		case <-d:
			return
		case <-timer.C:
			do()
			timer.Reset(interval)
		}
	}
}

func query() {
	id := rand.Intn(1000)
	metricType := "guage"
	var agg string
	if id%2 == 0 {
		metricType = "counter"
	}
	if id%5 == 0 {
		agg = "true"
	}

	qr := api.QueryRequest{
		Name: "metric." + strconv.Itoa(id) + "." + metricType,
		Tags: map[string]string{
			"region":  "region" + strconv.Itoa(id%10),
			"version": "v" + strconv.Itoa(id%5),
		},
	}
	b, err := json.Marshal(qr)
	if err != nil {
		log.Printf("Could not marshal request: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	c := new(http.Client)
	req, err := http.NewRequestWithContext(ctx, "GET", "http://localhost:8080/query", bytes.NewReader(b))
	if err != nil {
		log.Printf("Could not create request: %v", err)
		return
	}
	qv := req.URL.Query()
	qv.Set("agg", agg)
	req.URL.RawQuery = qv.Encode()

	resp, err := c.Do(req)
	if err != nil {
		log.Printf("Could not send request: %v", err)
		return
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("Bad status code: %d", resp.StatusCode)
		return
	}

}

func queryRange() {
	id := rand.Intn(1000)
	metricType := "guage"
	var agg string
	if id%2 == 0 {
		metricType = "counter"
	}
	if id%5 == 0 {
		agg = "true"
	}

	qr := api.QueryRequestRange{
		Name: "metric." + strconv.Itoa(id) + "." + metricType,
		Tags: map[string]string{
			"region":  "region" + strconv.Itoa(id%10),
			"version": "v" + strconv.Itoa(id%5),
		},
		Range: api.QueryRangeRequestOptions{
			Duration: "15m",
		},
	}
	b, err := json.Marshal(qr)
	if err != nil {
		log.Printf("Could not marshal request: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	c := new(http.Client)
	req, err := http.NewRequestWithContext(ctx, "GET", "http://localhost:8080/query_range", bytes.NewReader(b))
	if err != nil {
		log.Printf("Could not create request: %v", err)
		return
	}
	qv := req.URL.Query()
	qv.Set("agg", agg)
	req.URL.RawQuery = qv.Encode()

	resp, err := c.Do(req)
	if err != nil {
		log.Printf("Could not send request: %v", err)
		return
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("Bad status code: %d", resp.StatusCode)
		return
	}
}
