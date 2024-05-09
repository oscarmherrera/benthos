package aerospike

import "github.com/benthosdev/benthos/v4/public/service"

func clientFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField(cFieldAddresses).
			Description("A list of Cassandra nodes to connect to. Multiple comma separated addresses can be specified on a single line.").
			Examples(
				[]string{"localhost:9042"},
				[]string{"foo:9042", "bar:9042"},
				[]string{"foo:9042,bar:9042"},
			),
		service.NewTLSToggledField(cFieldTLS).Advanced(),
		service.NewObjectField(cFieldPassAuth,
			service.NewBoolField(cFieldPassAuthEnabled).
				Description("Whether to use password authentication").
				Default(false),
			service.NewStringField(cFieldPassAuthUsername).
				Description("The username to authenticate as.").
				Default(""),
			service.NewStringField(cFieldPassAuthPassword).
				Description("The password to authenticate with.").
				Secret().
				Default(""),
		).
			Description("Optional configuration of Cassandra authentication parameters.").
			Advanced(),
		service.NewBoolField(cFieldDisableIHL).
			Description("If enabled the driver will not attempt to get host info from the system.peers table. This can speed up queries but will mean that data_centre, rack and token information will not be available.").
			Advanced().
			Default(false),
		service.NewIntField(cFieldMaxRetries).
			Description("The maximum number of retries before giving up on a request.").
			Advanced().
			Default(3),
		service.NewObjectField(cFieldBackoff,
			service.NewDurationField(cFieldBackoffInitInterval).
				Description("The initial period to wait between retry attempts.").
				Default("1s"),
			service.NewDurationField(cFieldBackoffMaxInterval).
				Description("The maximum period to wait between retry attempts.").
				Default("5s"),
		).
			Description("Control time intervals between retry attempts.").
			Advanced(),
		service.NewDurationField(cFieldTimeout).
			Description("The client connection timeout.").
			Default("600ms"),
	}
}
