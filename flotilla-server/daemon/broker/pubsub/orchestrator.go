package pubsub

import (
	"errors"
	"io/ioutil"
	"log"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/cloud"
	"google.golang.org/cloud/pubsub"
)

const topic = "test"

// CloudPubSubBroker is an implementation of the broker interface which handles
// orchestrating Google Cloud Pub/Sub.
type CloudPubSubBroker struct {
	ProjectID string
	JSONKey   string
}

// Start will start the message broker and prepare it for testing.
func (c *CloudPubSubBroker) Start(host, port string) (interface{}, error) {
	ctx, err := newContext(c.ProjectID, c.JSONKey)
	if err != nil {
		return "", err
	}

	exists, err := pubsub.TopicExists(ctx, topic)
	if err != nil {
		log.Printf("Failed to check Cloud Pub/Sub topic: %s", err.Error())
		return "", err
	}

	if exists {
		if err := pubsub.DeleteTopic(ctx, topic); err != nil {
			log.Printf("Failed to delete Cloud Pub/Sub topic: %s", err.Error())
			return "", err
		}
	}

	if err := pubsub.CreateTopic(ctx, topic); err != nil {
		log.Printf("Failed to create Cloud Pub/Sub topic: %s", err.Error())
		return "", err
	}

	log.Println("Created Cloud Pub/Sub topic")

	return "", nil
}

// Stop will stop the message broker.
func (c *CloudPubSubBroker) Stop() (interface{}, error) {
	ctx, err := newContext(c.ProjectID, c.JSONKey)
	if err != nil {
		return "", err
	}

	if err := pubsub.DeleteTopic(ctx, topic); err != nil {
		log.Printf("Failed to delete Cloud Pub/Sub topic: %s", err.Error())
		return "", err
	}

	log.Println("Deleted Cloud Pub/Sub topic")
	return "", err
}

func newContext(projectID, jsonKey string) (context.Context, error) {
	if projectID == "" {
		return nil, errors.New("project id not provided")
	}

	if jsonKey == "" {
		return nil, errors.New("JSON key not provided")
	}

	key, err := ioutil.ReadFile(jsonKey)
	if err != nil {
		return nil, err
	}

	conf, err := google.JWTConfigFromJSON(
		oauth2.NoContext,
		key,
		pubsub.ScopeCloudPlatform,
		pubsub.ScopePubSub,
	)
	if err != nil {
		return nil, err
	}

	ctx := cloud.NewContext(projectID, conf.Client(oauth2.NoContext, nil))
	return ctx, nil
}
