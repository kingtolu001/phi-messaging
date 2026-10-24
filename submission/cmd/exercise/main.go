package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"phi-messaging/internal/exercise"
	"time"
)

func main() {
	mode := flag.String("mode", "load", "load, failure or audit")
	input := flag.String("input", "", "existing load report for a separate durable-history audit")
	url := flag.String("url", "http://127.0.0.1:8080", "load balancer URL")
	connections := flag.Int("connections", 10, "simultaneous recipient devices")
	rate := flag.Int("rate", 2, "aggregate logical sends per second across the entire workload, not per device")
	warmup := flag.Duration("warmup", 3*time.Second, "excluded warm-up")
	duration := flag.Duration("duration", 15*time.Second, "measurement window")
	ramp := flag.Duration("ramp", 10*time.Millisecond, "pause between connection starts")
	drain := flag.Duration("drain", 15*time.Second, "delivery deadline after sends finish")
	out := flag.String("out", "report.json", "JSON report path")
	flag.Parse()
	c := exercise.LoadConfig{URL: *url, Connections: *connections, Rate: *rate, Warmup: *warmup, Duration: *duration, Ramp: *ramp, Drain: *drain}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	var passed bool
	var value any
	if *mode == "failure" {
		c.Connections = 3
		c.KillAfter = c.Warmup + 3*time.Second
		result := exercise.RunFailure(ctx, c)
		value = result
		passed = result.Passed
	} else if *mode == "audit" {
		raw, e := os.ReadFile(*input)
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(2)
		}
		var report exercise.LoadReport
		if e = json.Unmarshal(raw, &report); e != nil {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(2)
		}
		audit := exercise.Audit(ctx, report)
		value = audit
		passed = audit.Error == "" && audit.MissingAcknowledged == 0
	} else if *mode == "load" {
		result := exercise.RunLoad(ctx, c)
		value = result
		passed = result.Passed
	} else {
		fmt.Fprintln(os.Stderr, "invalid mode")
		os.Exit(2)
	}
	if e := exercise.Save(*out, value); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(2)
	}
	if !passed {
		fmt.Println("FAIL:", *out)
		os.Exit(1)
	}
	fmt.Println("PASS:", *out)
}
