package aerospike

import (
	"context"
	"errors"
	as "github.com/aerospike/aerospike-client-go/v6"
	"strings"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component/input/span"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	asFieldQuery = "query"
	asBins       = "bins"
	asSet        = "set"
)

func inputConfigSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Categories("Services").
		Summary("Executes a query on an aerospike set and creates a message for each row received.").
		Description("A query to execute.").
		Fields(
			//service.NewStringField(asFieldQuery),
			service.NewObjectField(asFieldQuery,
				service.NewStringField(asSet).
					Description("The set to execute the query on"),
				service.NewStringListField(asBins).
					Description("The bins to retrieve from the set, if there is no bin list all bins will be retrieved").
					Optional(),
			)).
		Example("Minimal query for Aerospike",
			`
Let's presume that we have 3 Aerospike nodes:
`,
			`
input:
  aerospike:
    addresses:
      - 172.17.0.2
    query:
		bins:
		  - foo
		  - bar
		  - zoo
`,
		)

	return spec
}

func init() {
	err := service.RegisterInput(
		"aerospike", inputConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			input, err := newAerospikeInput(conf, mgr)
			if err != nil {
				return nil, err
			}

			r, err := service.AutoRetryNacksToggled(conf, input)
			if err != nil {
				return nil, err
			}
			return span.NewInput("aerospike", conf, r, mgr)
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

func newAerospikeInput(conf *service.ParsedConfig, mgr *service.Resources) (*aerospikeInput, error) {
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
		err := a.recordSet.Close()
		if err != nil {
			a.log.Errorf("Failed to close recordset: %v", err)
		}
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

func (a *aerospikeInput) Close(ctx context.Context) (err error) {
	go func() {
		a.disconnect()
	}()
	a.interruptOnce.Do(func() {
		close(a.interruptChan)
	})
	return
}
