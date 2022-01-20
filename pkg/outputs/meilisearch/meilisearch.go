package meilisearch

import (
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/outputs"
)

type clientConfig struct {
	// Number of worker goroutines publishing log events
	Workers int `config:"workers" validate:"min=1"`
	// Max number of events in a batch to send to a single client
	BatchSize int `config:"batch_size" validate:"min=1"`
	// Max number of retries for single batch of events
	RetryLimit int `config:"retry_limit"`

	Index string `config:"index"`

	Addr string `config:"addr"`
}

func init() {
	outputs.RegisterType("meilisearch", makeMS)
}

func makeMS(_ outputs.IndexManager, _ beat.Info, stats outputs.Observer, cfg *common.Config,
) (outputs.Group, error) {
	config := clientConfig{}

	clients := make([]outputs.NetworkClient, config.Workers)
	for i := 0; i < config.Workers; i++ {
		client := newClient(stats, config.Index, config.Addr)
		clients[i] = client
	}

	return outputs.SuccessNet(true, config.BatchSize, config.RetryLimit, clients)
}
