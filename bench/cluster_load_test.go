// Cluster-level throughput/latency measurement: unlike engine_bench_test.go
// (which benchmarks the storage engine directly, in-process), this drives
// a real 3-node cluster — real TCP for Raft, real HTTP for the client
// API — with many concurrent HTTP clients, and measures true end-to-end
// latency: network + Raft consensus + storage, not just local disk I/O.
//
// Run with:
//
//	go test ./bench/... -run TestClusterThroughputAndLatency -v
package bench

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PedrowDias/key-value-store/engine"
	"github.com/PedrowDias/key-value-store/raft"
	"github.com/PedrowDias/key-value-store/server"
	"github.com/PedrowDias/key-value-store/transport"
)

const (
	loadTestTickInterval  = 50 * time.Millisecond
	loadTestElectionTicks = 10
	loadTestHeartbeatTick = 1
)

type loadTestNode struct {
	id      uint64
	httpURL string

	server  *server.Server
	tr      *transport.Transport
	raft    *raft.Node
	eng     *engine.Engine
	httpSrv *http.Server
	httpLn  net.Listener
}

func startLoadTestCluster(t *testing.T, n int, raftBasePort, httpBasePort int) []*loadTestNode {
	return startLoadTestClusterWithBatchWindow(t, n, raftBasePort, httpBasePort, -1)
}

// startLoadTestClusterWithBatchWindow is startLoadTestCluster with
// control over the group-commit batch window; batchWindow < 0 leaves
// each Server's own default in place. Used by TestBatchWindowSweep to
// stand up a cluster per candidate window value.
func startLoadTestClusterWithBatchWindow(t *testing.T, n int, raftBasePort, httpBasePort int, batchWindow time.Duration) []*loadTestNode {
	t.Helper()
	ids := make([]uint64, n)
	raftAddrs := make(map[uint64]string, n)
	for i := 0; i < n; i++ {
		ids[i] = uint64(i + 1)
		raftAddrs[ids[i]] = fmt.Sprintf("127.0.0.1:%d", raftBasePort+i)
	}

	nodes := make([]*loadTestNode, n)
	for i, id := range ids {
		var peers []uint64
		peerAddrs := make(map[uint64]string)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
				peerAddrs[other] = raftAddrs[other]
			}
		}

		dir := t.TempDir()
		rn, err := raft.OpenNode(raft.Config{
			ID: id, Peers: peers,
			ElectionTick: loadTestElectionTicks, HeartbeatTick: loadTestHeartbeatTick,
		}, filepath.Join(dir, "raft.wal"))
		if err != nil {
			t.Fatalf("OpenNode(%d): %v", id, err)
		}
		tr, err := transport.Listen(id, raftAddrs[id], peerAddrs)
		if err != nil {
			t.Fatalf("Listen(%d): %v", id, err)
		}
		eng, err := engine.Open(engine.Options{Dir: filepath.Join(dir, "kv")})
		if err != nil {
			t.Fatalf("engine.Open(%d): %v", id, err)
		}

		srv := server.New(rn, tr, eng, loadTestTickInterval)
		if batchWindow >= 0 {
			srv.SetBatchWindow(batchWindow)
		}
		go srv.Run()

		httpLn, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", httpBasePort+i))
		if err != nil {
			t.Fatalf("http Listen(%d): %v", id, err)
		}
		httpSrv := &http.Server{Handler: server.NewHTTPAPI(srv).Handler()}
		go httpSrv.Serve(httpLn)

		nodes[i] = &loadTestNode{
			id: id, httpURL: "http://" + httpLn.Addr().String(),
			server: srv, tr: tr, raft: rn, eng: eng,
			httpSrv: httpSrv, httpLn: httpLn,
		}
	}
	return nodes
}

func stopLoadTestCluster(nodes []*loadTestNode) {
	for _, n := range nodes {
		n.httpSrv.Close()
		n.server.Stop()
		n.tr.Close()
		n.raft.Close()
		n.eng.Close()
	}
}

func waitForLoadTestLeader(t *testing.T, nodes []*loadTestNode, timeout time.Duration) *loadTestNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *loadTestNode
		count := 0
		var maxTerm uint64
		for _, n := range nodes {
			s := n.server.Status()
			if s.Role == raft.Leader {
				if s.Term > maxTerm {
					maxTerm = s.Term
					leader = n
					count = 1
				} else if s.Term == maxTerm {
					count++
				}
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timed out waiting for a single leader")
	return nil
}

// latencyStats summarizes a slice of measured request latencies.
type latencyStats struct {
	count      int
	errors     int
	p50, p90   time.Duration
	p99, p999  time.Duration
	min, max   time.Duration
	mean       time.Duration
	throughput float64 // ops/sec, over the wall-clock duration of the run
}

// percentileFine returns the (thousandths/10)-th percentile — e.g.
// thousandths=999 gives the 99.9th percentile — using nearest-rank.
// A separate helper from failover_test.go's integer percentile(), which
// only supports whole-percent granularity; load test latency
// distributions benefit from a finer p99.9 given the larger sample size.
func percentileFine(sorted []time.Duration, thousandths int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (thousandths * len(sorted)) / 1000
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func computeLatencyStats(latencies []time.Duration, errors int, wallClock time.Duration) latencyStats {
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	stats := latencyStats{
		count:      len(sorted),
		errors:     errors,
		throughput: float64(len(sorted)) / wallClock.Seconds(),
	}
	if len(sorted) == 0 {
		return stats
	}
	stats.min = sorted[0]
	stats.max = sorted[len(sorted)-1]
	stats.mean = sum / time.Duration(len(sorted))
	stats.p50 = percentile(sorted, 50)
	stats.p90 = percentile(sorted, 90)
	stats.p99 = percentile(sorted, 99)
	stats.p999 = percentileFine(sorted, 999)
	return stats
}

func (s latencyStats) log(t *testing.T, label string) {
	t.Logf("=== %s ===", label)
	t.Logf("requests: %d (errors: %d)", s.count, s.errors)
	t.Logf("throughput: %.1f ops/sec", s.throughput)
	t.Logf("latency: min=%v mean=%v p50=%v p90=%v p99=%v p99.9=%v max=%v",
		s.min, s.mean, s.p50, s.p90, s.p99, s.p999, s.max)
}

// loadTestClient issues Put/Get requests against the cluster over real
// HTTP and records each request's latency.
type loadTestClient struct {
	http       *http.Client
	writeURL   string   // the leader's base URL, for writes
	readURLs   []string // all nodes' base URLs, for reads (round-robin)
	readCursor int64
}

func (c *loadTestClient) put(key, value []byte) (time.Duration, error) {
	start := time.Now()
	req, err := http.NewRequest(http.MethodPut, c.writeURL+"/kv/"+string(key), bytes.NewReader(value))
	if err != nil {
		return 0, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return time.Since(start), err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	elapsed := time.Since(start)
	if resp.StatusCode != http.StatusNoContent {
		return elapsed, fmt.Errorf("put: unexpected status %d", resp.StatusCode)
	}
	return elapsed, nil
}

func (c *loadTestClient) get(key []byte) (time.Duration, error) {
	idx := atomic.AddInt64(&c.readCursor, 1)
	url := c.readURLs[int(idx)%len(c.readURLs)]

	start := time.Now()
	resp, err := c.http.Get(url + "/kv/" + string(key))
	if err != nil {
		return time.Since(start), err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	elapsed := time.Since(start)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return elapsed, fmt.Errorf("get: unexpected status %d", resp.StatusCode)
	}
	return elapsed, nil
}

// runLoadTest drives numWorkers concurrent goroutines, each issuing
// requestsPerWorker requests at the given read percentage, against a
// pre-populated cluster, and returns aggregate latency/throughput stats.
func runLoadTest(t *testing.T, client *loadTestClient, numWorkers, requestsPerWorker, readPct, keySpace int) latencyStats {
	var mu sync.Mutex
	var allLatencies []time.Duration
	var errorCount int64

	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localLatencies := make([]time.Duration, 0, requestsPerWorker)
			for i := 0; i < requestsPerWorker; i++ {
				key := []byte(fmt.Sprintf("k-%d", (workerID*requestsPerWorker+i)%keySpace))
				var d time.Duration
				var err error
				if (workerID*requestsPerWorker+i)%100 < readPct {
					d, err = client.get(key)
				} else {
					d, err = client.put(key, []byte("v"))
				}
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					continue
				}
				localLatencies = append(localLatencies, d)
			}
			mu.Lock()
			allLatencies = append(allLatencies, localLatencies...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	wallClock := time.Since(start)

	return computeLatencyStats(allLatencies, int(errorCount), wallClock)
}

// newLoadTestHTTPClient builds an http.Client configured for the
// concurrency these load tests actually generate.
//
// Go's http.DefaultTransport (what an &http.Client{Timeout: ...} with no
// Transport field gets, implicitly) caps MaxIdleConnsPerHost at just 2.
// With dozens of concurrent goroutines all sending requests to the same
// leader URL, that's a severe, unintentional bottleneck baked into the
// BENCHMARK TOOL itself: most requests can't reuse a pooled connection
// and pay a full TCP handshake, or queue behind the tiny pool — entirely
// separate from anything the server under test is actually doing. This
// was discovered, not designed around from the start: an early version
// of this file used the zero-value client, and a batch-window sweep at
// 50 concurrent workers produced results that didn't make sense (0
// window performing far worse than even a no-batching baseline) until
// tracing it back to this.
func newLoadTestHTTPClient(maxConns int) *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        maxConns,
			MaxIdleConnsPerHost: maxConns,
			MaxConnsPerHost:     0, // unlimited — let the OS/kernel be the real limit, not this client
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

func TestClusterThroughputAndLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster load test in -short mode")
	}

	nodes := startLoadTestCluster(t, 3, 26001, 26101)
	defer stopLoadTestCluster(nodes)

	leader := waitForLoadTestLeader(t, nodes, 3*time.Second)
	var readURLs []string
	for _, n := range nodes {
		readURLs = append(readURLs, n.httpURL)
	}
	client := &loadTestClient{
		http:     newLoadTestHTTPClient(128),
		writeURL: leader.httpURL,
		readURLs: readURLs,
	}

	// Pre-populate a modest key space so reads have real data to find.
	const keySpace = 200
	for i := 0; i < keySpace; i++ {
		if _, err := client.put([]byte(fmt.Sprintf("k-%d", i)), []byte("v")); err != nil {
			t.Fatalf("populate: %v", err)
		}
	}

	t.Run("ReadHeavy_90pct", func(t *testing.T) {
		stats := runLoadTest(t, client, 20, 50, 90, keySpace)
		stats.log(t, "Read-heavy (90% reads), 20 workers x 50 requests")
	})

	t.Run("WriteHeavy_10pctReads", func(t *testing.T) {
		stats := runLoadTest(t, client, 20, 50, 10, keySpace)
		stats.log(t, "Write-heavy (10% reads), 20 workers x 50 requests")
	})

	t.Run("WriteHeavy_HighConcurrency", func(t *testing.T) {
		stats := runLoadTest(t, client, 100, 20, 10, keySpace)
		stats.log(t, "Write-heavy (10% reads), 100 workers x 20 requests")
	})
}

// TestBatchWindowSweep empirically finds a good group-commit batch
// window rather than assuming the package default is optimal: it stands
// up a fresh cluster per candidate window (so one run's warm state can't
// bias the next), applies the same write-heavy workload at a fixed
// concurrency, and reports throughput/latency for each. Run with:
//
//	go test ./bench/... -run TestBatchWindowSweep -v
func TestBatchWindowSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping batch window sweep in -short mode")
	}

	windows := []time.Duration{
		0,
		100 * time.Microsecond,
		250 * time.Microsecond,
		500 * time.Microsecond,
		1 * time.Millisecond,
		2 * time.Millisecond,
		5 * time.Millisecond,
	}

	for i, window := range windows {
		window := window
		t.Run(window.String(), func(t *testing.T) {
			basePort := 26500 + i*10
			nodes := startLoadTestClusterWithBatchWindow(t, 3, basePort, basePort+100, window)
			defer stopLoadTestCluster(nodes)

			leader := waitForLoadTestLeader(t, nodes, 3*time.Second)
			var readURLs []string
			for _, n := range nodes {
				readURLs = append(readURLs, n.httpURL)
			}
			client := &loadTestClient{
				http:     newLoadTestHTTPClient(128),
				writeURL: leader.httpURL,
				readURLs: readURLs,
			}

			const keySpace = 200
			for k := 0; k < keySpace; k++ {
				if _, err := client.put([]byte(fmt.Sprintf("k-%d", k)), []byte("v")); err != nil {
					t.Fatalf("populate: %v", err)
				}
			}

			stats := runLoadTest(t, client, 50, 30, 10, keySpace)
			stats.log(t, fmt.Sprintf("batchWindow=%s, write-heavy, 50 workers x 30 requests", window))
		})
	}
}
