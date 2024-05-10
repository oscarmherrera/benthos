package aerospike

import (
	"context"
	"errors"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"

	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	coFieldQuery       = "query"
	coFieldArgsMapping = "args_mapping"
	coFieldConsistency = "consistency"
	coFieldLoggedBatch = "logged_batch"
	coFieldBatching    = "batching"
)

func outputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Publish to an aerospike set.").
		Description(`This output will interpolate functions within the aerospike set).

` + connectionNameDescription() + authDescription()).
		Fields(connectionHeadFields()...).
		Field(service.NewInterpolatedStringField("subject").
			Description("The set to publish to.").
			Example("<namespace>.<set>")).
		Field(service.NewInterpolatedStringMapField("headers").
			Description("Explicit message headers to add to messages.").
			Default(map[string]any{}).
			Example(map[string]any{
				"Timestamp": `${!meta("Timestamp")}`,
			})).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of messages to have in flight at a given time. Increase this to improve throughput.").
			Default(64)).
		Fields(connectionTailFields()...).
		Field(outputTracingDocs())

}

func init() {
	err := service.RegisterBatchOutput(
		"aerospike", outputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(coFieldBatching); err != nil {
				return
			}
			out, err = newAerospikeWriter(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

type aerospikeWriter struct {
	connDetails        connectionDetails
	log                *service.Logger
	policyHeader       map[string]interface{}
	maxInFlight        int
	subject            string
	namespace          string
	set                string
	currentAsClient    *as.Client
	recordExistsAction as.RecordExistsAction
	commitLevel        as.CommitLevel
	generationPolicy   as.GenerationPolicy
	durableDelete      bool
	sendKey            bool
	interruptChan      chan struct{}
	interruptOnce      sync.Once

	argsMapping *bloblang.Executor
	connLock    sync.RWMutex
}

func newAerospikeWriter(conf *service.ParsedConfig, mgr *service.Resources) (c *aerospikeWriter, err error) {
	c = &aerospikeWriter{
		log: mgr.Logger(),
	}
	//var err error
	if c.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	//if c.query, err = conf.FieldString(coFieldQuery); err != nil {
	//	return
	//}

	c.maxInFlight, err = conf.FieldMaxInFlight()
	if err != nil {
		return nil, err
	}

	if c.subject, err = conf.FieldString("subject"); err != nil {
		return nil, err
	}

	if strings.Count(c.subject, ".") != 1 {
		return nil, errors.New("subject must be in the form of <namespace>.<set>")
	}

	c.namespace = strings.Split(c.subject, ".")[0]
	c.set = strings.Split(c.subject, ".")[1]

	headers, err := conf.FieldInterpolatedStringMap("headers")
	if err != nil {
		return nil, err
	}

	//UPDATE RecordExistsAction = iota
	//
	//// UPDATE_ONLY means: Update record only. Fail if record does not exist.
	//// Merge write command bins with existing bins.
	//UPDATE_ONLY
	//
	//// REPLACE means: Create or replace record.
	//// Delete existing bins not referenced by write command bins.
	//// Supported by Aerospike 2 server versions >= 2.7.5 and
	//// Aerospike 3 server versions >= 3.1.6 and later.
	//REPLACE
	//
	//// REPLACE_ONLY means: Replace record only. Fail if record does not exist.
	//// Delete existing bins not referenced by write command bins.
	//// Supported by Aerospike 2 server versions >= 2.7.5 and
	//// Aerospike 3 server versions >= 3.1.6 and later.
	//REPLACE_ONLY
	//
	//// CREATE_ONLY means: Create only. Fail if record exists.
	//CREATE_ONLY

	val, ok := headers["record_exists_action"]
	if ok {
		s, isStatic := val.Static()
		if !isStatic {

		}
		valInt, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse record_exists_action: %w", err)
		}
		c.recordExistsAction = as.RecordExistsAction(valInt)
	}

	val, ok = headers["commit_level"]
	if ok {
		s, isStatic := val.Static()
		if !isStatic {

		}
		valInt, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse commit_level: %w", err)
		}
		c.commitLevel = as.CommitLevel(valInt)
	}

	val, ok = headers["generation_policy"]
	if ok {
		s, isStatic := val.Static()
		if !isStatic {

		}
		valInt, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse generation_policy: %w", err)
		}
		c.generationPolicy = as.GenerationPolicy(valInt)
	}

	val, ok = headers["durabe_delete"]
	if ok {
		s, isStatic := val.Static()
		if !isStatic {

		}
		if strings.ToLower(s) == "true" {
			c.durableDelete = true
		} else {
			c.durableDelete = false
		}
	}

	val, ok = headers["send_key"]
	if ok {
		s, isStatic := val.Static()
		if !isStatic {

		}
		if strings.ToLower(s) == "true" {
			c.sendKey = true
		} else {
			c.sendKey = false
		}
	}

	if aStr, _ := conf.FieldString(coFieldArgsMapping); aStr != "" {
		if c.argsMapping, err = conf.FieldBloblang(coFieldArgsMapping); err != nil {
			return
		}
	}

	return
}

func (c *aerospikeWriter) Connect(ctx context.Context) error {
	c.connLock.Lock()
	defer c.connLock.Unlock()

	//if n.natsConn != nil {
	//	return nil
	//}

	var asClient *as.Client
	//var natsSub *nats.Subscription
	var err error

	if asClient, err = c.connDetails.get(ctx); err != nil {
		return err
	}

	c.currentAsClient = asClient

	return nil
}

func (c *aerospikeWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	c.connLock.RLock()

	c.connLock.RUnlock()

	if c.currentAsClient == nil {
		return service.ErrNotConnected
	}

	if len(batch) == 1 {
		return c.writeRow(batch)
	}
	return c.writeBatch(c.currentAsClient, batch)
}

func (c *aerospikeWriter) writeRow(b service.MessageBatch) error {
	values, err := c.mapArgs(b, 0)
	if err != nil {
		return fmt.Errorf("parsing args: %w", err)
	}

	msg := values[0].(*service.Message)
	return c.writeAerospikeRecordFromMessage(msg)
}

func (c *aerospikeWriter) writeBatch(client *as.Client, b service.MessageBatch) error {

	for i := range b {
		values, err := c.mapArgs(b, i)
		msg := values[0].(*service.Message)
		err = c.writeAerospikeRecordFromMessage(msg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *aerospikeWriter) mapArgs(b service.MessageBatch, index int) ([]any, error) {
	if c.argsMapping != nil {
		// We've got an "args_mapping" field, extract values from there.
		part, err := b.BloblangQuery(index, c.argsMapping)
		if err != nil {
			return nil, fmt.Errorf("executing bloblang mapping: %w", err)
		}

		jraw, err := part.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("parsing bloblang mapping result as json: %w", err)
		}

		j, ok := jraw.([]any)
		if !ok {
			return nil, fmt.Errorf("expected bloblang mapping result to be []interface{} but was %T", jraw)
		}

		for i, v := range j {
			j[i] = genericValue{v: v}
		}
		return j, nil
	}
	return nil, nil
}

func (c *aerospikeWriter) Close(context.Context) error {
	c.connLock.Lock()
	go func() {
		c.disconnect()
	}()
	c.interruptOnce.Do(func() {
		close(c.interruptChan)
	})
	c.connLock.Unlock()
	return nil
}

type genericValue struct {
	v any
}

type decorator struct {
	NumRetries int
	Min, Max   time.Duration
}

func (d *decorator) Attempt(q gocql.RetryableQuery) bool {
	if q.Attempts() > d.NumRetries {
		return false
	}
	time.Sleep(getExponentialTime(d.Min, d.Max, q.Attempts()))
	return true
}

func getExponentialTime(min, max time.Duration, attempts int) time.Duration {
	minFloat := float64(min)
	napDuration := minFloat * math.Pow(2, float64(attempts-1))

	// Add some jitter
	napDuration += rand.Float64()*minFloat - (minFloat / 2)
	if napDuration > float64(max) {
		return max
	}
	return time.Duration(napDuration)
}

func (c *aerospikeWriter) writeAerospikeRecordFromMessage(msg *service.Message) error {
	var binMap as.BinMap
	var key *as.Key
	err := msg.MetaWalkMut(func(k string, v any) error {
		binMap[k] = v
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to walk: %w", err)
	}

	gen, bFound := msg.MetaGetMut("generation")
	if bFound == false {
		gen = 0
	}

	exp, bFound := msg.MetaGetMut("expiration")
	if bFound == false {
		exp = 0
	}

	id, bFound := msg.MetaGetMut("id")

	policy := as.NewWritePolicy(gen.(uint32), exp.(uint32))
	policy.SendKey = c.sendKey
	policy.RecordExistsAction = c.recordExistsAction
	policy.CommitLevel = c.commitLevel
	policy.GenerationPolicy = c.generationPolicy
	policy.DurableDelete = c.durableDelete

	key, err = as.NewKey(c.namespace, c.set, id)
	if err != nil {
		return fmt.Errorf("failed to create key: %w", err)
	}

	err = c.currentAsClient.Put(policy, key, binMap)
	if err != nil {
		return fmt.Errorf("failed to write row: %w", err)
	}
	return nil

}

func (c *aerospikeWriter) disconnect() {
	c.connLock.Lock()
	defer c.connLock.Unlock()

	if c.currentAsClient != nil {
		c.currentAsClient = nil
	}
	c.currentAsClient = nil
}
