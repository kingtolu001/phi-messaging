package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"phi-messaging/internal/exercise"
	"runtime/pprof"
	"syscall"
	"time"
)

func main() {
	mode := flag.String("mode", "load", "load, failure, audit or fixtures")
	userOffset := flag.Int("user-offset", 0, "first distinct-user fixture index for a disjoint generator shard")
	readyFile := flag.String("ready-file", "", "write accepted connection counts before waiting for the start barrier")
	startBarrier := flag.String("start-barrier", "", "wait for this shared file before starting warm-up")
	cpuProfile := flag.String("cpu-profile", "", "optional diagnostic CPU profile; changes measurement overhead")
	distinct := flag.Bool("distinct-users", false, "one distinct seeded identity per recipient, in groups of at most 100")
	fixtureDir := flag.String("fixture-dir", ".artifacts/scale", "fresh directory for local mock identity fixtures")
	recovery := flag.Duration("recovery-interval", 0, "periodic HTTP repair interval; zero retains the original one-second client default")
	readyTimeout := flag.Duration("ready-timeout", 30*time.Second, "bounded wait for the full cohort after connection ramp")
	sharedHTTP := flag.Bool("shared-http", false, "bound generator HTTP connections to 256 shared across devices")
	killAfter := flag.Duration("kill-after", 0, "kill app1 after this time from warm-up start; caller must restore app1")
	firstSocket := flag.String("first-websocket-url", "", "pin first device to this instance for the node-failure test")
	survivor := flag.String("survivor-url", "", "surviving instance URL for the legacy multi-device presence check")
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
	if *cpuProfile != "" {
		profile, err := os.OpenFile(*cpuProfile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if err = pprof.StartCPUProfile(profile); err != nil {
			profile.Close()
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		defer func() { pprof.StopCPUProfile(); profile.Close() }()
	}
	c := exercise.LoadConfig{URL: *url, Connections: *connections, Rate: *rate, Warmup: *warmup, Duration: *duration, Ramp: *ramp, Drain: *drain, DistinctUsers: *distinct, RecoveryInterval: *recovery, SharedHTTP: *sharedHTTP}
	if *readyTimeout <= 0 || *readyTimeout > 2*time.Minute {
		fmt.Fprintln(os.Stderr, "ready-timeout must be positive and at most 2m")
		os.Exit(2)
	}
	c.ReadyTimeout = *readyTimeout
	c.KillAfter, c.FirstWebSocketURL, c.SurvivorURL = *killAfter, *firstSocket, *survivor
	c.UserOffset, c.ReadyFile, c.StartBarrier = *userOffset, *readyFile, *startBarrier
	if *mode == "fixtures" {
		if err := exercise.WriteUserFixtures(*fixtureDir, *connections); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Println("Created local user fixtures in", *fixtureDir)
		return
	}
	signalContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ctx, cancel := context.WithTimeout(signalContext, 10*time.Minute)
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
		// os.Exit bypasses defers; flush diagnostic samples before returning failure.
		if *cpuProfile != "" {
			pprof.StopCPUProfile()
		}
		os.Exit(1)
	}
	fmt.Println("PASS:", *out)
}
