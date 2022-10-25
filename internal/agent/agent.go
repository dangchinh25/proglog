package agent

import (
	"bytes"
	"crypto/tls"
	"dangchinh25/proglog/internal/auth"
	"dangchinh25/proglog/internal/discovery"
	"dangchinh25/proglog/internal/log"
	"dangchinh25/proglog/internal/server"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Agent runs on every service instance, setting and connecting all the different components.
type Agent struct {
	Config AgentConfig // Wrap around all the component's config (CommitLog, Server, Membership, Replicator)

	mux        cmux.CMux
	log        *log.DistributedLog   // CommitLog instance
	server     *grpc.Server          // GRPC server instance
	membership *discovery.Membership // Serf's membership instance

	isShutdown   bool          // Indicate whether the current Agent cluster is shutdown
	shutdowns    chan struct{} //
	shutdownLock sync.Mutex
}

type AgentConfig struct {
	// Config for server

	ServerTLSConfig *tls.Config // Configuration that's served to clients
	PeerTLSConfig   *tls.Config // Configuration for peer client to communicate internally between server

	// Config for CommitLog

	DataDir string // Location(path) to save commit log

	// Config for Serf

	NodeName       string   // Acts as the node's unique identifier across the Serf cluster
	BindAddr       string   // Serf listens on this address for gossiping
	RPCPort        int      // Combine with BindAdd to form a complete RPC address to create a connection to
	StartJoinAddrs []string // Addresses of nodes in the cluster so that when new node join the cluster, it can auto discovery and connect with the other nodes

	// Config for Authorizer

	ACLModelFile  string // Path to model file to pass into Authorizer (Casbin)
	ACLPolicyFile string // Path to policy file to pass into Authorizer (Casbin)

	Bootstrap bool
}

func (c AgentConfig) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

// New creates an Agent and runs a set of methods to set up and run the agent's components
func NewAgent(config AgentConfig) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	go a.serve()

	return a, nil
}

// creates a listener on RPC address that'll accept both Raft and gRPC connections and then creates a mux with the listener
func (a *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(":%d", a.Config.RPCPort)
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	a.mux = cmux.New(ln)

	return nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

// configure the DistributedLog's Raft to use multiplexer listenr and create the distributed log
func (a *Agent) setupLog() error {
	// Identify Raft connections by reading 1 byte and check that the byte matches the one we setup in Dial() method in StreamLayer
	// if the mux matches this rule, it will pass the connection the listener to handle the connection
	raftLn := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Compare(b, []byte{byte(log.RaftRPC)}) == 0
	})

	logConfig := log.LogConfig{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftLn,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap
	var err error
	a.log, err = log.NewDistributedLog(
		a.Config.DataDir,
		logConfig,
	)
	if err != nil {
		return err
	}
	if a.Config.Bootstrap {
		err = a.log.WaitForLeader(3 * time.Second)
	}
	return err
}

func (a *Agent) setupServer() error {
	authorizer := auth.NewAuthorizer(a.Config.ACLModelFile, a.Config.ACLPolicyFile)
	serverConfig := &server.ServerConfig{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}
	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	var err error
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}
	grpcLn := a.mux.Match(cmux.Any())
	go func() {
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()

	return err
}

// sets up a Replicator with the gRPC dial options needed to connect to other servers
// and a client so the replicator can connect to other servers
func (a *Agent) setupMembership() error {
	// setup a peer client for Replicator
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}

	a.membership, err = discovery.NewMembership(a.log, discovery.MembershipConfig{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})
	return err
}

// Shutdown the agent and its component by
// leaving the membership so that others will see this server has left the cluster so that this server doesn't receive discovery events anymore,
// closing the replicator so it doesn't continue to replicate,
// gracefully stopping the server,
// closing the log.
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.isShutdown {
		return nil
	}
	a.isShutdown = true
	close(a.shutdowns)

	shutdown := []func() error{
		a.membership.Leave,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}

	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

// serve the connection by mux
func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}
