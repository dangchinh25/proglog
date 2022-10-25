package loadbalance

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	api "dangchinh25/proglog/api/v1"
)

type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn // user's client connection and gRPC passes it to the resolver to connect with the servers it discovers
	resolverConn  *grpc.ClientConn    // resolver's own connection to the server so it can call GetServers()
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

const Name = "proglog"

// enforce Resolver to resolver.Builder interface
var _ resolver.Builder = (*Resolver)(nil)
var _ resolver.Resolver = (*Resolver)(nil)

// Build method to satify the resolver.Builder interface.
// Build recieves data and setup connections that will be used to setup connections
func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc

	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(opts.DialCreds))
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(fmt.Sprintf(`{"loadBalancingConfig":[{"%s": {}}]}`, Name))
	var err error
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})

	return r, nil
}

// Sche method to satify the resolver.Builder interface
func (r *Resolver) Scheme() string {
	return Name
}

// ResolveNow method to satify the resolver.Interface interface
// ResolveNow resolve the target, discover the servers, and update the client connection with the servers
func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	client := api.NewLogClient(r.resolverConn)
	// get cluster and then set on cc attributes
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error("failed to resolve server", zap.Error(err))
		return
	}
	// Update the state with a list of server address to inform the load balance what servers it can choose from
	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr:       server.RpcAddr,
			Attributes: attributes.New("is_leader", server.IsLeader),
		})
	}
	r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

// Close method to satify the resolver.Resolver interface
func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error("failed to close conn", zap.Error(err))
	}
}

func init() {
	resolver.Register(&Resolver{})
}
