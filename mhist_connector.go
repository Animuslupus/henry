package henry

import (
	"context"
	"log"

	"github.com/alexmorten/mhist/models"

	"github.com/alexmorten/mhist/proto"
	"google.golang.org/grpc"
)

// MhistConnector reads new messages from mhist and distributes them accross the network
type MhistConnector struct {
	stub               proto.MhistClient
	subscriptionStream proto.Mhist_SubscribeClient
	writeStream        proto.Mhist_StoreStreamClient
	broker             NetworkBroker
	brokerChannel      chan Message
}

// NewMhistConnector creates a new MhistConnector
func NewMhistConnector(address string, filter *proto.Filter, broker NetworkBroker, brokerChannel chan Message) (*MhistConnector, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	stub := proto.NewMhistClient(conn)
	subStream, err := stub.Subscribe(context.Background(), filter)
	if err != nil {
		return nil, err
	}
	writeStream, err := stub.StoreStream(context.Background())
	if err != nil {
		return nil, err
	}
	return &MhistConnector{
		stub:               stub,
		subscriptionStream: subStream,
		writeStream:        writeStream,
		broker:             broker,
		brokerChannel:      brokerChannel,
	}, nil
}

// Listen for incoming GRPC messages on the subscribed channels of MHIST and forwards them through the Broker
func (c *MhistConnector) Listen() {
	for {
		message, err := c.subscriptionStream.Recv()
		if err != nil {
			log.Fatalf("Failed to receive message: %v", err)
		}
		go c.broker.HandleMeasurementMessage(message)
	}
}

// Update MHIST using data forwarded by the broker
func (c *MhistConnector) Update() {
	for {
		message := <-c.brokerChannel
		measurement := proto.MeasurementFromModel(&models.Raw{
			Value: message.Measurement,
		})
		err := c.writeStream.Send(&proto.MeasurementMessage{
			Name:        message.Channel,
			Measurement: measurement,
		})
		if err != nil {
			log.Printf("Writing to MHIST StoreStream failed: %v", err)
		}
		log.Printf("Message added to MHIST:\nName: %s,\nMeasurement: %s,\nTimestamp: %d", message.Channel, string(message.Measurement), message.Timestamp)
	}
}
