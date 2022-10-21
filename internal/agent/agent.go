package agent

import (
	"crypto/tls"
	api "dangchinh25/proglog/api/v1"
	"dangchinh25/proglog/internal/auth"
	"dangchinh25/proglog/internal/discovery"
	"dangchinh25/proglog/internal/log"
	"dangchinh25/proglog/internal/server"
	"fmt"
	"net"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Agent runs on every service instance, setting and connecting all the different components.
type Agent struct {
	Config AgentConfig // Wrap around all the component's config (CommitLog, Server, Membership, Replicator)

	log        *log.Log              // CommitLog instance
	server     *grpc.Server          // GRPC server instance
	membership *discovery.Membership // Serf's membership instance
	replicator *log.Replicator       // Replicator management instance

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
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	return a, nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

func (a *Agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(a.Config.DataDir, log.Config{})

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
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	httpListener, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	go func() {
		if err := a.server.Serve(httpListener); err != nil {
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
	var opts []grpc.DialOption
	if a.Config.PeerTLSConfig != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(a.Config.PeerTLSConfig)))
	}
	conn, err := grpc.Dial(rpcAddr, opts...)
	if err != nil {
		return err
	}
	client := api.NewLogClient(conn)

	a.replicator = &log.Replicator{
		DialOptions: opts,
		LocalServer: client,
	}
	a.membership, err = discovery.NewMembership(a.replicator, discovery.MembershipConfig{
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
		a.replicator.Close,
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
