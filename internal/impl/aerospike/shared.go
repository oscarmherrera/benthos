package aerospike

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aerospike/aerospike-client-go/v6"
	"github.com/benthosdev/benthos/v4/public/service"
	"time"
)

type aerospikeMsgData struct {
	log                *service.Logger
	connDetails        connectionDetails
	namespace          string
	set                string
	operation          kvpOperationType
	recordExistsAction aerospike.RecordExistsAction
	commitLevel        aerospike.CommitLevel
	generationPolicy   aerospike.GenerationPolicy
	durableDelete      bool
	sendKey            bool
	timeout            time.Duration
}

func (a *aerospikeMsgData) Get(ctx context.Context, p *kvProcessor, key any) (*aerospikeMsg, error) {
	policy := aerospike.NewPolicy()
	policy.SendKey = true
	var err error
	var asKey *aerospike.Key

	value := aerospike.NewValue(key)
	value.GetType()
	switch v := value.(type) {
	case aerospike.IntegerValue:
		iKey := int(v)
		asKey, err = aerospike.NewKey(a.namespace, a.set, iKey)
		if err != nil {
			return nil, fmt.Errorf("unable to create integer key: %s", err)
		}
	case aerospike.StringValue:
		sKey := string(v)
		asKey, err = aerospike.NewKey(a.namespace, a.set, sKey)
		if err != nil {
			return nil, fmt.Errorf("unable to create string key: %s", err)
		}
	case aerospike.BytesValue:
		bvKey := []byte(v)
		asKey, err = aerospike.NewKey(a.namespace, a.set, bvKey)
		if err != nil {
			return nil, fmt.Errorf("unable to create byte array key: %s", err)
		}
	default:
		asKey = nil
		return nil, fmt.Errorf("unsupported user key type %T", v)
	}

	record, err := p.aerospikeDetails.connDetails.asClient.Get(policy, asKey)

	if err != nil {
		return nil, err
	}

	if record == nil {
		return nil, nil
	}

	msgValue, err := convertBinMapToBytes(record.Bins)
	if err != nil {
		return nil, fmt.Errorf("unable to convert bins to bytes: %s", err)
	}

	msg := &aerospikeMsg{
		Value:      msgValue,
		Key:        record.Key,
		Namespace:  a.namespace,
		Set:        a.set,
		Generation: int32(record.Generation),
		Expiration: int32(record.Expiration),
		Operation:  string(kvpOperationGet),
	}

	return msg, nil
}

func (a *aerospikeMsgData) Create(ctx context.Context, key any, msgBytes []byte) (uint32, error) {
	return a.put(ctx, aerospike.CREATE_ONLY, key, 0, msgBytes)
}

func (a *aerospikeMsgData) Put(ctx context.Context, key any, msgBytes []byte) (uint32, error) {
	return a.put(ctx, aerospike.REPLACE, key, 0, msgBytes)
}

func (a *aerospikeMsgData) Update(ctx context.Context, key any, gen uint32, msgBytes []byte) (uint32, error) {
	return a.put(ctx, aerospike.UPDATE, key, gen, msgBytes)
}

func (a *aerospikeMsgData) Delete(ctx context.Context, key any) error {
	policy := aerospike.NewWritePolicy(0, 0)
	policy.SendKey = true
	policy.DurableDelete = true

	var err error
	var asKey *aerospike.Key

	value := aerospike.NewValue(key)
	value.GetType()
	switch v := value.(type) {
	case aerospike.IntegerValue:
		iKey := int(v)
		asKey, err = aerospike.NewKey(a.namespace, a.set, iKey)
		if err != nil {
			return fmt.Errorf("unable to create integer key: %s", err)
		}
	case aerospike.StringValue:
		sKey := string(v)
		asKey, err = aerospike.NewKey(a.namespace, a.set, sKey)
		if err != nil {
			return fmt.Errorf("unable to create string key: %s", err)
		}
	case aerospike.BytesValue:
		bvKey := []byte(v)
		asKey, err = aerospike.NewKey(a.namespace, a.set, bvKey)
		if err != nil {
			return fmt.Errorf("unable to create byte array key: %s", err)
		}
	default:
		asKey = nil
		return fmt.Errorf("unsupported user key type %T", v)
	}

	recordExisted, err := a.connDetails.asClient.Delete(policy, asKey)
	if err != nil {
		return err
	}
	if !recordExisted {
		a.log.Warnf("Record with key %v does not exist", key)
	}
	return nil
}

func (a *aerospikeMsgData) put(ctx context.Context, action aerospike.RecordExistsAction, key any, gen uint32, msgBytes []byte) (uint32, error) {
	policy := aerospike.NewWritePolicy(gen, 0)
	policy.SendKey = true
	policy.DurableDelete = true
	policy.RecordExistsAction = action
	policy.Expiration = 0

	var err error
	var asKey *aerospike.Key

	value := aerospike.NewValue(key)
	value.GetType()
	switch v := value.(type) {
	case aerospike.IntegerValue:
		iKey := int(v)
		asKey, err = aerospike.NewKey(a.namespace, a.set, iKey)
		if err != nil {
			return 0, fmt.Errorf("unable to create integer key: %s", err)
		}
	case aerospike.StringValue:
		sKey := string(v)
		asKey, err = aerospike.NewKey(a.namespace, a.set, sKey)
		if err != nil {
			return 0, fmt.Errorf("unable to create string key: %s", err)
		}
	case aerospike.BytesValue:
		bvKey := []byte(v)
		asKey, err = aerospike.NewKey(a.namespace, a.set, bvKey)
		if err != nil {
			return 0, fmt.Errorf("unable to create byte array key: %s", err)
		}
	default:
		asKey = nil
		return 0, fmt.Errorf("unsupported user key type %T", v)
	}

	msgValue, err := convertByteArrayToBinMap(msgBytes)
	if err != nil {
		return 0, fmt.Errorf("unable to convert bytes to bins: %s", err)
	}

	var binMap *aerospike.BinMap
	binMap = (*aerospike.BinMap)(msgValue)

	err = a.connDetails.asClient.Put(policy, asKey, *binMap)
	if err != nil {
		return 0, fmt.Errorf("unable to put record: %s, %v", err, asKey)
	}
	a.log.Debug(fmt.Sprintf("Put record with key %v", key))

	// Go get th current revision of the record
	readPolicy := aerospike.NewPolicy()
	record, err := a.connDetails.asClient.GetHeader(readPolicy, asKey)
	if err != nil {
		return 0, fmt.Errorf("unable to get record header: %s", err)
	}
	return record.Generation, nil
}

func convertByteArrayToBinMap(b []byte) (*map[string]interface{}, error) {
	var m map[string]interface{}
	err := json.Unmarshal(b, &m)
	if err != nil {
		return nil, fmt.Errorf("unable to convert bytes to map: %s", err)
	}

	return &m, nil
}

func convertBinMapToBytes(m map[string]interface{}) ([]byte, error) {
	byteArray, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("unable to convert map to bytes: %s", err)
	}

	return byteArray, nil

}

//
//import "github.com/benthosdev/benthos/v4/public/service"
//
//func clientFields() []*service.ConfigField {
//	return []*service.ConfigField{
//		service.NewStringListField(cFieldAddresses).
//			Description("A list of Cassandra nodes to connect to. Multiple comma separated addresses can be specified on a single line.").
//			Examples(
//				[]string{"localhost:9042"},
//				[]string{"foo:9042", "bar:9042"},
//				[]string{"foo:9042,bar:9042"},
//			),
//		service.NewTLSToggledField(cFieldTLS).Advanced(),
//		service.NewObjectField(cFieldPassAuth,
//			service.NewBoolField(cFieldPassAuthEnabled).
//				Description("Whether to use password authentication").
//				Default(false),
//			service.NewStringField(cFieldPassAuthUsername).
//				Description("The username to authenticate as.").
//				Default(""),
//			service.NewStringField(cFieldPassAuthPassword).
//				Description("The password to authenticate with.").
//				Secret().
//				Default(""),
//		).
//			Description("Optional configuration of Cassandra authentication parameters.").
//			Advanced(),
//		service.NewBoolField(cFieldDisableIHL).
//			Description("If enabled the driver will not attempt to get host info from the system.peers table. This can speed up queries but will mean that data_centre, rack and token information will not be available.").
//			Advanced().
//			Default(false),
//		service.NewIntField(cFieldMaxRetries).
//			Description("The maximum number of retries before giving up on a request.").
//			Advanced().
//			Default(3),
//		service.NewObjectField(cFieldBackoff,
//			service.NewDurationField(cFieldBackoffInitInterval).
//				Description("The initial period to wait between retry attempts.").
//				Default("1s"),
//			service.NewDurationField(cFieldBackoffMaxInterval).
//				Description("The maximum period to wait between retry attempts.").
//				Default("5s"),
//		).
//			Description("Control time intervals between retry attempts.").
//			Advanced(),
//		service.NewDurationField(cFieldTimeout).
//			Description("The client connection timeout.").
//			Default("600ms"),
//	}
//}
