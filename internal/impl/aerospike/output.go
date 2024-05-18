package aerospike

import (
	"context"
	"errors"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	p "golang.org/x/exp/apidiff/testdata"
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
	coFieldHeaders     = "headers"
)

func outputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Publish to an aerospike set.").
		Description(`This output will interpolate functions within the aerospike set).

` + connectionNameDescription() + authDescription()).
		Fields(connectionHeadFields()...).
		Field(service.NewStringAnnotatedEnumField(kvpFieldOperation, kvpOperations).
			Description("The operation to perform on the KV set.")).
		Field(service.NewStringField(kvFieldBucket).
			Description("The fully qualified name of the <namespace>.<set> to interact with.")).
		Field(service.NewInterpolatedStringMapField("headers").
			Description("Explicit message headers to add to messages.").
			Default(map[string]any{}).
			Advanced().Optional().
			Example(map[string]any{
				"Timestamp": `${!meta("Timestamp")}`,
			})).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of messages to have in flight at a given time. Increase this to improve throughput.").
			Default(64)).
		Field(service.NewDurationField(kvpFieldTimeout).
			Description("The maximum period to wait on an operation before aborting and returning an error.").
			Advanced().Default("5s")).
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
	aerospikeDetails *aerospikeMsgData
	policyHeader     map[string]interface{}
	maxInFlight      int

	interruptChan chan struct{}
	interruptOnce sync.Once

	argsMapping *bloblang.Executor
	connLock    sync.RWMutex
}

func newAerospikeWriter(conf *service.ParsedConfig, mgr *service.Resources) (c *aerospikeWriter, err error) {
	c = &aerospikeWriter{
		aerospikeDetails: &aerospikeMsgData{
			log: mgr.Logger(),
		},
	}

	if c.aerospikeDetails.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	c.maxInFlight, err = conf.FieldMaxInFlight()
	if err != nil {
		return nil, err
	}

	var subject string
	if subject, err = conf.FieldString(kvFieldBucket); err != nil {
		return nil, err
	}

	if strings.Count(subject, ".") != 1 {
		return nil, errors.New("namespace.set must be in the form of <namespace>.<set>")
	}

	c.aerospikeDetails.namespace = strings.Split(subject, ".")[0]
	c.aerospikeDetails.set = strings.Split(subject, ".")[1]

	headers, err := conf.FieldInterpolatedStringMap(coFieldHeaders)
	if err != nil {
		return nil, err
	}

	if c.aerospikeDetails.timeout, err = conf.FieldDuration(kvpFieldTimeout); err != nil {
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
			c.aerospikeDetails.recordExistsAction = as.UPDATE
		}
		valInt, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse record_exists_action: %w", err)
		}
		c.aerospikeDetails.recordExistsAction = as.RecordExistsAction(valInt)
	}

	val, ok = headers["commit_level"]
	if ok {
		s, isStatic := val.Static()
		if !isStatic {
			c.aerospikeDetails.commitLevel = as.COMMIT_MASTER
		}
		valInt, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse commit_level: %w", err)
		}
		c.aerospikeDetails.commitLevel = as.CommitLevel(valInt)
	}

	val, ok = headers["generation_policy"]
	if ok {
		s, isStatic := val.Static()
		if !isStatic {
			c.aerospikeDetails.generationPolicy = as.NONE
		}
		valInt, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse generation_policy: %w", err)
		}
		c.aerospikeDetails.generationPolicy = as.GenerationPolicy(valInt)
	}

	val, ok = headers["durabe_delete"]
	if ok {
		s, isStatic := val.Static()
		if !isStatic {
			c.aerospikeDetails.durableDelete = true
		}
		if strings.ToLower(s) == "true" {
			c.aerospikeDetails.durableDelete = true
		} else {
			c.aerospikeDetails.durableDelete = false
		}
	}

	val, ok = headers["send_key"]
	if ok {
		s, isStatic := val.Static()
		if !isStatic {
			c.sendKey = true
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

	var asClient *as.Client
	var err error

	if asClient, err = c.connDetails.get(ctx); err != nil {
		return err
	}

	c.currentAsClient = asClient
	return nil
}

func (c *aerospikeWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	c.connLock.RLock()

	defer c.connLock.RUnlock()

	if c.currentAsClient == nil {
		return service.ErrNotConnected
	}

	if len(batch) == 1 {
		return c.writeRow(batch)
	}
	return c.writeBatch(c.currentAsClient, batch)
}

func (c *aerospikeWriter) writeRow(ctx context.Context, b service.MessageBatch) error {
	values, err := c.mapArgs(b, 0)
	if err != nil {
		return fmt.Errorf("parsing args: %w", err)
	}
	msg := values[0].(*service.Message)

	key, bFound := msg.MetaGetMut(metaKVKey)
	if !bFound {
		return fmt.Errorf("key not found in message metadata")
	}
	bytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	ctx, done := context.WithTimeout(ctx, c.timeout)
	defer done()

	return c.writeAerospikeRecordFromMessage(msg)
}

func (c *aerospikeWriter) writeBatch(client *as.Client, b service.MessageBatch) error {

	for i := range b {
		values, err := c.mapArgs(b, i)
		msg := values[0].(*service.Message)

		if msg.GetError() != nil {
			return msg.GetError()
		}

		var bFound bool
		key, bFound = msg.MetaGetMut(metaKVKey)
		if bFound == false {
			return fmt.Errorf("key not found in message metadata")
		}

		bytes, err := msg.AsBytes()
		if err != nil {
			return err
		}

		ctx, done := context.WithTimeout(ctx, c.aerospikeDetails.timeout)
		defer done()

		switch p.aerospikeDetails.operation {

		case kvpOperationGet:
			entry, err := p.aerospikeDetails.Get(ctx, p, key)
			if err != nil {
				return nil, err
			}
			return service.MessageBatch{newMessageFromKVEntry(entry)}, nil

		//case kvpOperationGetRevision:
		//	revision, err := p.parseRevision(msg)
		//	if err != nil {
		//		return nil, err
		//	}
		//	entry, err := kv.GetRevision(ctx, key, revision)
		//	if err != nil {
		//		return nil, err
		//	}
		//	return service.MessageBatch{newMessageFromKVEntry(entry)}, nil

		case kvpOperationCreate:
			revision, err := p.aerospikeDetails.Create(ctx, key, bytes)
			if err != nil {
				return nil, err
			}

			m := msg.Copy()
			p.addMetadata(m, key, uint64(revision), kvpOperationCreate)
			return service.MessageBatch{}, nil

		case kvpOperationPut:
			revision, err := p.aerospikeDetails.Put(ctx, key, bytes)
			if err != nil {
				return nil, err
			}

			m := msg.Copy()
			p.addMetadata(m, key, uint64(revision), kvpOperationPut)
			return service.MessageBatch{m}, nil

		case kvpOperationUpdate:
			revision := p.revision
			if revision == -1 {
				revision = 0 // because we are using the default values
			}

			rev, err := p.aerospikeDetails.Update(ctx, key, uint32(revision), bytes)
			if err != nil {
				return nil, err
			}

			m := msg.Copy()
			p.addMetadata(m, key, uint64(rev), kvpOperationPut)
			return service.MessageBatch{m}, nil

		case kvpOperationDelete:
			// TODO: Support revision here?
			err := p.aerospikeDetails.Delete(ctx, key)
			if err != nil {
				return nil, err
			}

			m := msg.Copy()
			p.addMetadata(m, key, 0, kvpOperationDelete)
			return service.MessageBatch{m}, nil

		//case kvpOperationPurge:
		//	err := kv.Purge(ctx, key)
		//	if err != nil {
		//		return nil, err
		//	}
		//
		//	m := msg.Copy()
		//	p.addMetadata(m, key, 0, nats.KeyValuePurge)
		//	return service.MessageBatch{m}, nil
		//
		//case kvpOperationHistory:
		//	entries, err := kv.History(ctx, key)
		//	if err != nil {
		//		return nil, err
		//	}
		//	var records []any
		//	for _, entry := range entries {
		//		records = append(records, map[string]any{
		//			"key":   entry.Key(),
		//			"value": entry.Value(),
		//			//"bucket":    entry.Bucket(),
		//			"revision":  entry.Revision(),
		//			"delta":     entry.Delta(),
		//			"operation": entry.Operation().String(),
		//			"created":   entry.Created(),
		//		})
		//	}
		//
		//	m := service.NewMessage(nil)
		//	m.SetStructuredMut(records)
		//	return service.MessageBatch{m}, nil

		//case kvpOperationKeys:
		//	// `kv.ListKeys()` does not allow users to specify a key filter, so we call `kv.Watch()` directly.
		//	watcher, err := kv.Watch(ctx, key, []jetstream.WatchOpt{jetstream.IgnoreDeletes(), jetstream.MetaOnly()}...)
		//	if err != nil {
		//		return nil, err
		//	}
		//	defer func() {
		//		if err := watcher.Stop(); err != nil {
		//			p.log.Debugf("Failed to close key watcher: %s", err)
		//		}
		//	}()
		//
		//	var keys []any
		//loop:
		//	for {
		//		select {
		//		case entry := <-watcher.Updates():
		//			if entry == nil {
		//				break loop
		//			}
		//			keys = append(keys, entry.Key())
		//		case <-ctx.Done():
		//			return nil, fmt.Errorf("watcher update loop exited prematurely: %s", ctx.Err())
		//		}
		//	}
		//
		//	m := service.NewMessage(nil)
		//	m.SetStructuredMut(keys)
		//	m.MetaSetMut(metaKVBucket, p.set)
		//	return service.MessageBatch{m}, nil

		default:
			return nil, fmt.Errorf("invalid kv operation: %s", p.aerospikeDetails.operation)
		}

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
