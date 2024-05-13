package aerospike

import (
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	metaKVKey        = "nats_kv_key"
	metaKVNamespace  = "aerospike_namespace"
	metaKVSet        = "aerospike_set"
	metaKVExpiration = "aerospike_expiration"
	metaKVGeneration = "aerospike_generation"
	metaKVOperation  = "aerospike_operation"
	metaKVCreated    = "aerospike_created"
)

func newMessageFromKVEntry(entry *aerospikeMsg) *service.Message {

	msg := service.NewMessage(entry.Value)
	msg.MetaSetMut(metaKVKey, entry.Key)
	msg.MetaSetMut(metaKVNamespace, entry.Namespace)
	msg.MetaSetMut(metaKVSet, entry.Set)
	msg.MetaSetMut(metaKVExpiration, entry.Expiration)
	msg.MetaSetMut(metaKVGeneration, entry.Generation)
	msg.MetaSetMut(metaKVOperation, entry.Operation)
	msg.MetaSetMut(metaKVCreated, entry.Created)

	return msg
}
