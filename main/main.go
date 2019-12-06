package main

import (
	"flag"
	"log"
	"strings"

	"github.com/alexmorten/mhist/proto"
	"github.com/animuslupus/henry"
)

func main() {
	var broadcastIP string
	var mhistAddress string
	var mhistChannelNames string
	var port int

	flag.StringVar(&broadcastIP, "broadcast_ip", "", "The broadcast ip-address of the network")
	flag.StringVar(&mhistAddress, "mhist_address", "localhost:6666", "The address of the local instance of MHIST")
	flag.StringVar(&mhistChannelNames, "mhist_channel_filter", "map_updates,position_updates", "Comma separated list of channels henry should subscribe to.")
	flag.IntVar(&port, "port", 4020, "the port henry should run on")
	flag.Parse()

	if broadcastIP == "" {
		log.Fatal("Broadcast Ip has to be specified!")
	}

	var brokerChannel chan henry.Message

	broker := henry.NewUDPBroker(broadcastIP, port)
	go broker.Listen(brokerChannel)

	mhistChannelFilter := &proto.Filter{Names: strings.Split(mhistChannelNames, ",")}

	receiver, err := henry.NewMhistConnector(mhistAddress, mhistChannelFilter, broker, brokerChannel)
	if err != nil {
		log.Fatalf("Creation of MHIST Receiver failed: %v", err)
	}
	go receiver.Update()
	receiver.Listen()
}
