// Command poolworker supervises the mining-pool process.
//
// It is the pool's counterpart to blockdag-corechain's nodeworker: it runs as
// PID 1 with mining-pool as its child, gates startup on the node's own health,
// probes the running pool, and restarts it when it wedges.
//
// It replaces the watchdog's pool-start lease and pool restarts. Both lived
// outside the pool and needed the Docker socket; neither could tell the pool to
// drain its miners before stopping. Gating here also breaks the coupling that
// made mining availability depend on the watchdog being alive.
package main

import (
	"asicPool/internal/buildinfo"
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	var (
		poolBinary = flag.String("pool-binary", "mining-pool", "path to the mining-pool binary")
		poolArgs   = flag.String("pool-args", "", "space-separated arguments passed to mining-pool")

		nodeRPCURL = flag.String("node-rpc-url", envOr("NODE_RPC_URL", "http://127.0.0.1:38131"), "node DAG RPC endpoint")
		nodeUser   = flag.String("node-rpc-user", os.Getenv("NODE_RPC_USER"), "node RPC basic-auth user")
		nodePass   = flag.String("node-rpc-pass", os.Getenv("NODE_RPC_PASS"), "node RPC basic-auth password")

		metricsAddr = flag.String("pool-metrics-addr", envOr("METRICS_ADDR", ":9090"), "pool metrics/health listen address")
		stratumAddr = flag.String("pool-bind-addr", envOr("POOL_BIND_ADDR", ":3334"), "pool stratum listen address")

		startGateTimeout   = flag.Duration("start-gate-timeout", 0, "how long to wait for the node to become mineable before giving up (0 waits indefinitely)")
		probeInterval      = flag.Duration("probe-interval", 10*time.Second, "interval between health probes")
		rpcTimeout         = flag.Duration("rpc-timeout", 5*time.Second, "timeout for a single node or pool HTTP call")
		dialTimeout        = flag.Duration("dial-timeout", 3*time.Second, "timeout for the stratum TCP probe")
		stopGrace          = flag.Duration("stop-grace", 20*time.Second, "how long the pool may take to drain after SIGTERM before SIGKILL")
		restartCooldown    = flag.Duration("restart-cooldown", 60*time.Second, "quiet period after a restart before probes can trigger another")
		maxRestartCooldown = flag.Duration("max-restart-cooldown", 30*time.Minute, "ceiling for the exponential backoff between consecutive restarts")

		driftTimeout           = flag.Duration("drift-timeout", 45*time.Second, "restart the pool if its template parent has been absent from the node's recent parents for this long")
		driftWindow            = flag.Duration("drift-window", 90*time.Second, "how long a node parent stays acceptable for the drift probe")
		driftMaxParents        = flag.Int("drift-max-parents", 512, "cap on remembered node parents")
		poolUnreachableTimeout = flag.Duration("pool-unreachable-timeout", 60*time.Second, "restart the pool if its health endpoint has been unreachable for this long")
		stratumDownTimeout     = flag.Duration("stratum-down-timeout", 60*time.Second, "restart the pool if its stratum listener has refused connections for this long")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.LUTC)
	log.Printf("[poolworker] BlockDAG Pool Worker %s source=%s", buildinfo.Version, buildinfo.SourceCommit)

	// An evicted parent reports its age as the window. If the window were below
	// the drift timeout, a pool stuck on an ancient parent would report an age
	// under the threshold and never be restarted — the exact failure this worker
	// exists to catch.
	if *driftWindow < *driftTimeout {
		log.Printf("[poolworker] drift-window (%s) must be >= drift-timeout (%s)", *driftWindow, *driftTimeout)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sup := &supervisor{
		node:               newNodeClient(*nodeRPCURL, *nodeUser, *nodePass, *rpcTimeout),
		pool:               newPoolClient(*metricsAddr, *rpcTimeout),
		child:              newChild(*poolBinary, splitArgs(*poolArgs)),
		drift:              newTemplateDrift(*driftWindow, *driftMaxParents),
		stratumAddr:        *stratumAddr,
		probeInterval:      *probeInterval,
		dialTimeout:        *dialTimeout,
		stopGrace:          *stopGrace,
		restartCooldown:    *restartCooldown,
		maxRestartCooldown: *maxRestartCooldown,
		thresholds: thresholds{
			driftTimeout:           *driftTimeout,
			poolUnreachableTimeout: *poolUnreachableTimeout,
			stratumDownTimeout:     *stratumDownTimeout,
		},
	}

	if err := sup.awaitNodeReady(ctx, *startGateTimeout); err != nil {
		log.Printf("[poolworker] not starting pool: %v", err)
		os.Exit(1)
	}

	if err := sup.child.start(); err != nil {
		log.Printf("[poolworker] %v", err)
		os.Exit(1)
	}

	err := sup.run(ctx)
	switch {
	case err == nil, ctx.Err() != nil:
		log.Print("[poolworker] shutdown complete")
	default:
		log.Printf("[poolworker] %v", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func splitArgs(raw string) []string {
	return strings.Fields(raw)
}
