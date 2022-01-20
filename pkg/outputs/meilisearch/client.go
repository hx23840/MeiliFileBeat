package meilisearch

import (
	"context"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/meilisearch/meilisearch-go"
)

type Client struct {
	stats  outputs.Observer
	Index  string
	Host   string
	Client *meilisearch.Client
}

func newClient(
	stats outputs.Observer,
	index string,
	host string,
) *Client {
	client := meilisearch.NewClient(meilisearch.ClientConfig{
		Host: host,
	})

	return &Client{
		stats,
		index,
		host,
		client,
	}
}

func (meiliSearchClient *Client) String() string {
	return "MeiliSearchClient"
}

func (meiliSearchClient *Client) Connect() error {
	return nil
}

func (meiliSearchClient *Client) Close() error {
	return nil
}

func (meiliSearchClient *Client) Publish(context context.Context, batch publisher.Batch) error {
	events := batch.Events()
	// 记录这批日志
	meiliSearchClient.stats.NewBatch(len(events))
	failEvents, err := meiliSearchClient.PublishEvents(events)
	if err != nil {
		// 如果发送正常，则ACK
		batch.ACK()
	} else {
		// 发送失败，则重试。受RetryLimit的限制
		batch.RetryEvents(failEvents)
	}

	return nil
}

func (meiliSearchClient *Client) PublishEvents(events []publisher.Event) ([]publisher.Event, error) {
	for i, event := range events {
		err := meiliSearchClient.publishEvent(&event)
		if err != nil {
			// 如果单条消息发送失败，则将剩余的消息直接重试
			return events[i:], err
		}
	}

	return nil, nil
}

func (meiliSearchClient *Client) publishEvent(event *publisher.Event) error {
	// An index is where the documents are stored.
	index := meiliSearchClient.Client.Index(meiliSearchClient.Index)

	documents := []map[string]interface{}{
		{"msg": event.Content.Fields.String()},
	}

	_, err := index.AddDocuments(documents)
	if err != nil {
		return err
	}

	return nil
}
