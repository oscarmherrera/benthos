package aerospike

import (
	"context"
	"errors"
	as "github.com/aerospike/aerospike-client-go/v6"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/benthosdev/benthos/v4/internal/component/input/span"
	"github.com/benthosdev/benthos/v4/public/service"
)

func aerospikeInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`Subscribe to a Aerospike subject=<namespace>.<set>`).
		Description(`
### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- aerospike_subject
- aerospike_reply_subject
- All message headers (when supported by the connection)
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).

` + connectionNameDescription() + authDescription()).
		Fields(connectionHeadFields()...).
		Field(service.NewStringField("subject").
			Description("A subject to consume from. Supports wildcards for consuming multiple subjects. Either a subject or stream must be specified.").
			Example("foo.bar.baz").Example("foo.*.baz").Example("foo.bar.*").Example("foo.>")).
		Field(service.NewStringField("queue").
			Description("An optional queue group to consume as.").
			Optional()).
		Field(service.NewAutoRetryNacksToggleField()).
		Field(service.NewDurationField("nak_delay").
			Description("An optional delay duration on redelivering a message when negatively acknowledged.").
			Example("1m").
			Advanced().
			Optional()).
		Field(service.NewIntField("prefetch_count").
			Description("The maximum number of messages to pull at a time.").
			Advanced().
			Default(nats.DefaultSubPendingMsgsLimit).
			LintRule(`root = if this < 0 { ["prefetch count must be greater than or equal to zero"] }`)).
		Fields(connectionTailFields()...).
		Field(inputTracingDocs())
}

func init() {
	err := service.RegisterInput(
		"aerospike", aerospikeInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			input, err := newAerospikeReader(conf, mgr)
			if err != nil {
				return nil, err
			}

			r, err := service.AutoRetryNacksToggled(conf, input)
			if err != nil {
				return nil, err
			}
			return span.NewInput("nats", conf, r, mgr)
		},
	)
	if err != nil {
		panic(err)
	}
}

type aerospikeInput struct {
	connDetails   connectionDetails
	subject       string
	queue         string
	prefetchCount int
	nakDelay      time.Duration

	log *service.Logger

	cMut sync.Mutex

	interruptChan   chan struct{}
	interruptOnce   sync.Once
	currentAsClient *as.Client
	namespace       string
	set             string
	result          <-chan *as.Result
	recordSet       *as.Recordset
}

func newAerospikeReader(conf *service.ParsedConfig, mgr *service.Resources) (*aerospikeInput, error) {
	n := aerospikeInput{
		log:           mgr.Logger(),
		interruptChan: make(chan struct{}),
	}

	var err error
	if n.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	if n.subject, err = conf.FieldString("subject"); err != nil {
		return nil, err
	}

	if n.prefetchCount, err = conf.FieldInt("prefetch_count"); err != nil {
		return nil, err
	}

	if n.prefetchCount < 0 {
		return nil, errors.New("prefetch count must be greater than or equal to zero")
	}

	if conf.Contains("nak_delay") {
		if n.nakDelay, err = conf.FieldDuration("nak_delay"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("queue") {
		if n.queue, err = conf.FieldString("queue"); err != nil {
			return nil, err
		}
	}

	if strings.Count(n.subject, ".") != 1 {
		return nil, errors.New("subject must be in the form of <namespace>.<set>")
	}

	n.namespace = strings.Split(n.subject, ".")[0]
	n.set = strings.Split(n.subject, ".")[1]

	return &n, nil
}

func (a *aerospikeInput) Connect(ctx context.Context) error {
	a.cMut.Lock()
	defer a.cMut.Unlock()

	//if n.natsConn != nil {
	//	return nil
	//}

	var asClient *as.Client
	//var natsSub *nats.Subscription
	var err error

	if asClient, err = a.connDetails.get(ctx); err != nil {
		return err
	}

	a.currentAsClient = asClient

	recordset, err := asClient.ScanAll(nil, a.namespace, a.set)
	if err != nil {
		return err
	}

	a.recordSet = recordset

	return nil
}

func (a *aerospikeInput) disconnect() {
	a.cMut.Lock()
	defer a.cMut.Unlock()

	if a.recordSet != nil {
		a.recordSet.Close()
		a.recordSet = nil
	}
	if a.currentAsClient != nil {
		a.currentAsClient = nil
	}
	a.currentAsClient = nil
}

func (a *aerospikeInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {

	var record *as.Record
	for res := range a.recordSet.Results() {
		if res.Err != nil {
			return nil, nil, res.Err
		}
		record = res.Record
		break
	}

	msg := service.NewMessage(nil)
	msg.SetStructuredMut(record.Bins)
	msg.MetaSet("aerospike_subject", a.subject)
	msg.MetaSet("aerospike_reply_subject", "")
	msg.MetaSetMut("generation", record.Generation)
	msg.MetaSetMut("expiration", record.Expiration)
	msg.MetaSetMut("key", record.Key)
	return msg, func(ctx context.Context, err error) error {
		return nil
	}, nil
}

func (n *aerospikeInput) Close(ctx context.Context) (err error) {
	go func() {
		n.disconnect()
	}()
	n.interruptOnce.Do(func() {
		close(n.interruptChan)
	})
	return
}
