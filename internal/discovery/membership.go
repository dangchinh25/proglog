package discovery

import (
	"net"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

// Config used for Serf setup
type MembershipConfig struct {
	NodeName       string            // Acts as the node's unique identifier across the Serf cluster
	BindAddr       string            // Serf listens on this address for gossiping
	Tags           map[string]string // Serf share these tags to other nodes inside the cluster, we sgare each node's user-configured RPC address so the nodes know which addresses to send their RPC
	StartJoinAddrs []string          // Addresses of nodes in the cluster so that when new node join the cluster, it can auto discovery and connect with the other nodes
}

// Membership wraps around Serf to provide discovery and cluster membership to our service1
type Membership struct {
	Config  MembershipConfig // Config to create a Serf cluster
	handler Handler          // Handle that implement custom behavior when a node leaves or joins the cluster
	serf    *serf.Serf
	events  chan serf.Event // A channel to receive Serf's events when a node joins or leaves the cluster.
	logger  *zap.Logger
}

// NewMembership create a Membership with the required configuration and event handler
func NewMembership(handler Handler, config MembershipConfig) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}

	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	return c, nil
}

func (m *Membership) setupSerf() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", m.Config.BindAddr)
	if err != nil {
		return err
	}
	// Setup config
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	config.EventCh = m.events
	config.Tags = m.Config.Tags
	config.NodeName = m.Config.NodeName
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}
	// Run eventHandler in the background
	go m.eventHandler()
	if m.Config.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.Config.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return nil
}

//handle event sent by Serf to all nodes
func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(member.Name, member.Tags["rpc_addr"]); err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(member.Name); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// returns whether the given Serf member is the local member
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// returns a snapshot of the current cluster's Serf members
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// tell this member to leave the Serf cluster
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

func (m *Membership) logError(err error, msg string, member serf.Member) {
	log := m.logger.Error
	if err == raft.ErrNotLeader {
		log = m.logger.Debug
	}
	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr",
			member.Tags["rpc_addr"]),
	)
}
