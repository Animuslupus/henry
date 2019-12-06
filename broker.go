package henry

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net"

	"github.com/alexmorten/mhist/proto"
)

const maxBufferSize = 1024

// NetworkBroker is an interface that represents different network broker
type NetworkBroker interface {
	HandleMeasurementMessage(*proto.MeasurementMessage)
	Listen(receive chan Message)
}

// UDPBroker forwards messages from MHIST into the mesh network using UDP
type UDPBroker struct {
	broadcast        *net.UDPAddr
	listiningAddress *net.UDPAddr
}

// Message stores the data broadcasted to the meshnetwork
type Message struct {
	Channel     string
	Measurement []byte
	Timestamp   int64
}

// NewUDPBroker generates a new UDPBroker given a broadcast ip address and port
func NewUDPBroker(receivingAddress string, port int) *UDPBroker {
	broadcastAddress, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", receivingAddress, port))
	if err != nil {
		log.Fatalf("Error occured during UDP address resolving: %v", err)
	}
	listiningAddress, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	return &UDPBroker{
		broadcast:        broadcastAddress,
		listiningAddress: listiningAddress,
	}
}

// HandleMeasurementMessage takes in a MeasurementMessage from MHIST and broadcasts it through an UDP packet
func (b *UDPBroker) HandleMeasurementMessage(measurement *proto.MeasurementMessage) {
	var buffer bytes.Buffer
	message := Message{
		Channel:     measurement.Name,
		Measurement: measurement.Measurement.GetRaw().Value,
		Timestamp:   measurement.Measurement.GetRaw().Ts,
	}
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(message)
	if err != nil {
		log.Println(message)
		log.Fatalf("Encode error: %v", err)
	}
	conn, err := net.DialUDP("udp", nil, b.broadcast)
	defer conn.Close()
	if err != nil {
		log.Fatalf("UDP Connection failed: %v", err)
	}
	conn.Write(buffer.Bytes())
	log.Printf("UPD Message send to broadcast:\nName: %s,\nMeasurement: %s,\nTimestamp: %d", message.Channel, string(message.Measurement), message.Timestamp)
}

// Listen receives and formats incoming UDP messages
func (b *UDPBroker) Listen(receive chan Message) {
	conn, err := net.ListenUDP("udp", b.listiningAddress)
	if err != nil {
		log.Fatalf("Listining on %v failed: %v", b.listiningAddress, err)
	}
	defer conn.Close()
	for {
		var message Message
		byteBuffer := make([]byte, maxBufferSize)
		_, err := conn.Read(byteBuffer)
		if err != nil {
			fmt.Printf("Failed to read from: %v, because of error: %v", b.listiningAddress, err)
		}
		buffer := bytes.NewBuffer(byteBuffer)
		dec := gob.NewDecoder(buffer)
		err = dec.Decode(&message)
		if err != nil {
			fmt.Printf("Failed to decode message: %s, because of error: %v", string(buffer.Bytes()), err)
		}
		receive <- message
	}
}
