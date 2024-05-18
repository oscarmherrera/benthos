package aerospike

import (
	"context"
	"errors"
	"fmt"
	"github.com/Jeffail/shutdown"
	as "github.com/aerospike/aerospike-client-go/v6"
	"strings"
	"sync"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	kvpFieldOperation = "operation"
	//kvpFieldKey       = "key"
	kvpFieldRevision = "generation"
	kvpFieldTimeout  = "timeout"
)

type kvpOperationType string

const (
	kvpOperationGet     kvpOperationType = "get"
	kvpOperationCreate  kvpOperationType = "create"
	kvpOperationPut     kvpOperationType = "put"
	kvpOperationUpdate  kvpOperationType = "update"
	kvpOperationDelete  kvpOperationType = "delete"
	kvpOperationPurge   kvpOperationType = "purge"
	kvpOperationHistory kvpOperationType = "history"
	kvpOperationKeys    kvpOperationType = "keys"
)

var kvpOperations = map[string]string{
	string(kvpOperationGet):     "Returns the latest value for `key`.",
	string(kvpOperationCreate):  "Adds the key/value pair if it does not exist. Returns an error if it already exists.",
	string(kvpOperationPut):     "Places a new value for the key into the store.",
	string(kvpOperationUpdate):  "Updates the value for `key` only if the `revision` matches the latest revision.",
	string(kvpOperationDelete):  "Deletes the key/value pair, but keeps historical values.",
	string(kvpOperationPurge):   "Deletes the key/value pair and all historical values.",
	string(kvpOperationHistory): "Returns historical values of `key` as an array of objects containing the following fields: `key`, `value`, `bucket`, `revision`, `delta`, `operation`, `created`.",
	string(kvpOperationKeys):    "Returns the keys in the `bucket` which match the `keys_filter` as an array of strings.",
}

func aerospikeKVProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("1.0.0").
		Summary("Perform operations on a aerospike key-value record.").
		Description(`
### KV Operations

The Aerospike KV processor supports a multitude of KV operations via the [operation](#operation) field. Along with ` + "`get`" + `, ` + "`put`" + `, and ` + "`delete`" + `, this processor supports atomic operations like ` + "`update`" + ` and ` + "`create`" + `, as well as utility operations like ` + "`purge`" + `, ` + "`history`" + `, and ` + "`keys`" + `.

### Metadata

This processor adds the following metadata fields to each message, depending on the chosen ` + "`operation`" + `:

#### get, get_revision
` + "``` text" + `
- aerospike_key
- aerospike_bins
- aerospike_generation
- aerospike_expiration
- aerospike_durable_delete
- aerospike_send_key
` + "```" + `

#### create, update, delete, purge
` + "``` text" + `
- aerospike_key
- aerospike_expiration
- aerospike_durable_delete
- aerospike_kv_operation
` + "```" + `

#### keys
` + "``` text" + `
- aerospike_bins
` + "```" + `

` + connectionNameDescription() + authDescription()).
		Fields(kvDocs([]*service.ConfigField{
			service.NewStringAnnotatedEnumField(kvpFieldOperation, kvpOperations).
				Description("The operation to perform on the KV set."),
			service.NewStringField(kvFieldBucket).
				Description("The fully qualified name of the <namespace>.<set> to interact with."),

			//service.NewStringField(asFieldQuery),
			service.NewObjectField(asFieldQuery,
				service.NewStringField(asSet).
					Description("The set to execute the query on"),
				service.NewStringListField(asBins).
					Description("The bins to retrieve from the set, if there is no bin list all bins will be retrieved").
					Optional(),
			),

			service.NewInterpolatedStringMapField("headers").
				Description("Explicit message headers to add to messages.").
				Default(map[string]any{}).
				Advanced().Optional().
				Example(map[string]any{
					"Timestamp": `${!meta("Timestamp")}`,
				}),

			service.NewIntField(kvpFieldRevision).
				Description("The revision to use for the `update or get` operation when using MRT in the future.").
				Default(-1).Optional(),

			service.NewDurationField(kvpFieldTimeout).
				Description("The maximum period to wait on an operation before aborting and returning an error.").
				Advanced().Default("5s"),
		}...)...).
		LintRule(`root = match {
      ["get_revision", "update"].contains(this.operation) && !this.exists("revision") => [ "'revision' must be set when operation is '" + this.operation + "'" ],
      !["get_revision", "update"].contains(this.operation) && this.exists("revision") => [ "'revision' cannot be set when operation is '" + this.operation + "'" ],
    }`)
}

func init() {
	err := service.RegisterProcessor(
		"aerospike", aerospikeKVProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newKVProcessor(conf, mgr)
		},
	)
	if err != nil {
		panic(err)
	}
}

type kvProcessor struct {
	aerospikeDetails *aerospikeMsgData
	revision         int // -1 means latest
	shutSig          *shutdown.Signaller
	connMut          sync.Mutex
}

func newKVProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*kvProcessor, error) {
	p := &kvProcessor{
		aerospikeDetails: &aerospikeMsgData{
			log: mgr.Logger(),
		},
		shutSig: shutdown.NewSignaller(),
	}

	var subject string
	var err error
	if p.aerospikeDetails.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	if subject, err = conf.FieldString(kvFieldBucket); err != nil {
		return nil, err
	}

	if operation, err := conf.FieldString(kvpFieldOperation); err != nil {
		return nil, err
	} else {
		p.aerospikeDetails.operation = kvpOperationType(operation)
	}

	//if p.key, err = conf.FieldInterpolatedString(kvpFieldKey); err != nil {
	//	return nil, err
	//}

	if conf.Contains(kvpFieldRevision) {
		if p.revision, err = conf.FieldInt(kvpFieldRevision); err != nil {
			return nil, err
		}
	}

	if p.aerospikeDetails.timeout, err = conf.FieldDuration(kvpFieldTimeout); err != nil {
		return nil, err
	}

	if strings.Count(subject, ".") != 1 {
		return nil, errors.New("subject must be in the form of <namespace>.<set>")
	}

	p.aerospikeDetails.namespace = strings.Split(subject, ".")[0]
	p.aerospikeDetails.set = strings.Split(subject, ".")[1]

	err = p.Connect(context.Background())
	return p, err
}

func (p *kvProcessor) disconnect() {
	p.connMut.Lock()
	defer p.connMut.Unlock()

	if p.aerospikeDetails.connDetails.asClient != nil {
		p.aerospikeDetails.connDetails.asClient = nil
	}
	p.aerospikeDetails.connDetails.asClient = nil
}

type aerospikeMsg struct {
	Key   *as.Key
	Value []byte
	//Bins       map[string]interface{}
	Namespace  string
	Set        string
	Generation int32
	Expiration int32
	Operation  string
	Created    bool
}

func (p *kvProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	p.connMut.Lock()
	//kv := p.kv
	defer p.connMut.Unlock()
	var key any
	var err error

	//Check if there is an error in the message
	if msg.GetError() != nil {
		return nil, msg.GetError()
	}

	var bFound bool
	key, bFound = msg.MetaGetMut(metaKVKey)
	if bFound == false {
		return nil, fmt.Errorf("key not found in message metadata")
	}

	bytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	ctx, done := context.WithTimeout(ctx, p.aerospikeDetails.timeout)
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
}

func (p *kvProcessor) addMetadata(msg *service.Message, key any, revision uint64, operation kvpOperationType) {
	_, bFlag := msg.MetaGetMut(metaKVKey)

	if bFlag == false {
		msg.MetaSetMut(metaKVKey, key)
	}

	msg.MetaSetMut(metaKVNamespace, p.aerospikeDetails.namespace)
	msg.MetaSetMut(metaKVSet, p.aerospikeDetails.set)
	msg.MetaSetMut(metaKVGeneration, revision)
	msg.MetaSetMut(metaKVOperation, string(operation))
}

func (p *kvProcessor) Connect(ctx context.Context) (err error) {
	p.connMut.Lock()
	defer p.connMut.Unlock()

	var asClient *as.Client

	if asClient, err = p.aerospikeDetails.connDetails.get(ctx); err != nil {
		return err
	}

	p.aerospikeDetails.connDetails.asClient = asClient

	return nil
}

func (p *kvProcessor) Close(ctx context.Context) error {
	go func() {
		p.disconnect()
		p.shutSig.TriggerHasStopped()
	}()
	select {
	case <-p.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
