package aerospike

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"net"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
)

// I've split the connection fields into two, which allows us to put tls and
// auth further down the fields stack. This is literally just polish for the
// docs.
func connectionHeadFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField("hosts").
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"10.211.55.101", "10.211.55.102", "10.211.103"}).
			Example([]string{"10.211.55.101:3000", "10.211.55.102:3000", "10.211.103:3000"}),
		service.NewIntField("queue_size").
			Description("The maximum number of connections to maintain in the connection pool.").
			Default(100).
			Optional(),
		service.NewIntField("min_connections_per_node").
			Description("The minimum number of connections to maintain per node.").
			Default(10).
			Optional(),
	}
}

func connectionTailFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewTLSToggledField("tls"),
		authFieldSpec(),
	}
}

type connectionDetails struct {
	label                string
	logger               *service.Logger
	tlsConf              *tls.Config
	authConf             authConfig
	fs                   *service.FS
	hosts                []*as.Host
	asClient             *as.Client
	queueSize            int
	minConnectionPerNode int
}

func connectionDetailsFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (c connectionDetails, err error) {
	c.label = mgr.Label()
	c.fs = mgr.FS()
	c.logger = mgr.Logger()

	if conf.Contains("hosts") {
		var hostList []string
		if hostList, err = conf.FieldStringList("hosts"); err != nil {
			err = errors.New("seed host list is empty")
			return c, err
		}
		for _, host := range hostList {
			addr, err2 := net.ResolveTCPAddr("tcp", host)
			if err2 != nil {
				err2 = fmt.Errorf("unable to parse host %s: %v", host, err2)
				return c, err
			}
			if addr.Port == 0 {
				// Use default port
				addr.Port = 3000
			}
			asHost := as.NewHost(addr.IP.String(), addr.Port)
			//asHost.TLSName = "benthos pipeline"
			c.hosts = append(c.hosts, asHost)

		}
	}

	var tlsEnabled bool
	if c.tlsConf, tlsEnabled, err = conf.FieldTLSToggled("tls"); err != nil {
		return
	}
	if !tlsEnabled {
		c.tlsConf = nil
	}

	if c.authConf, err = AuthFromParsedConfig(conf.Namespace("auth")); err != nil {
		return
	}
	return
}

func (c *connectionDetails) get(_ context.Context) (*as.Client, error) {
	if c.asClient != nil {
		return c.asClient, nil
	}

	policy := as.NewClientPolicy()
	policy.MinConnectionsPerNode = c.minConnectionPerNode
	if c.queueSize > 0 {
		policy.ConnectionQueueSize = c.queueSize
	}

	//policy.ConnectionQueueSize = 5
	policy.LimitConnectionsToQueueSize = true
	policy.OpeningConnectionThreshold = 0 //No limit
	policy.TendInterval = 1 * time.Second // Default
	policy.IdleTimeout = 1 * time.Minute
	policy.LoginTimeout = 60 * time.Second

	//authVars = config.ASAuth
	policy.User = c.authConf.Username
	policy.Password = c.authConf.UserPassword
	c.logger.Debugf("new connection with policy to nodes(%v) ", c.hosts)
	asClient, err := as.NewClientWithPolicyAndHost(policy, c.hosts...)
	if err != nil {
		return nil, err
	}

	return asClient, nil
}
