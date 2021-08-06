package main

import (
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/nu7hatch/gouuid"
	"log"
	"os"
	"time"
)

func handleMessage(message *amqp.Message) error {
	fmt.Printf("Message received: %s\n", message.Value)
	message.Accept(context.Background())
	return nil
}

func main() {
	connOpts := []amqp.ConnOption{
		amqp.ConnIdleTimeout(0),
		amqp.ConnSASLPlain("mdm", "mdm"),
		amqp.ConnContainerID("revkov"),
	}
	// Create client
	client, err := amqp.Dial("amqp://qm1.sm-soft.ru:5674",
		connOpts...
	)
	if err != nil {
		log.Fatal("Dialing AMQP server:", err)
	}
	defer client.Close()

	// Open a session
	session, err := client.NewSession()
	if err != nil {
		log.Fatal("Creating AMQP session:", err)
	}

	ctx := context.Background()

	// Continuously read messages
	{
		// Create a receiver
		receiver, err := session.NewReceiver(
			amqp.LinkSourceAddress("ELECTION_DVO.MDM.PROVIDER"),
			amqp.LinkName("ELECTION_DVO.MDM.PROVIDER"),
			amqp.LinkSourceExpiryPolicy(amqp.ExpiryLinkDetach),
			amqp.LinkSourceTimeout(100000),
			amqp.LinkCredit(10),
		)
		if err != nil {
			log.Fatal("Creating receiver link:", err)
		}
		defer func() {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			receiver.Close(ctx)
			cancel()
		}()

		for {
			/*err = receiver.HandleMessage(ctx, handleMessage)
			if err != nil {
				log.Fatal("1Reading message from AMQP:", err)
			}*/
			// Receive next message
			msg, err := receiver.Receive(ctx)
			if err != nil {
				log.Fatal("2Reading message from AMQP:", err)
			} else {
				fmt.Printf("Message received: %s\n", msg.Value)
			}
			var msgStr = fmt.Sprintf("%s", msg.Value)
			// Accept message
			msg.Accept(ctx)
			u, err := uuid.NewV4()
			os.WriteFile(u.String() + ".xml", []byte(msgStr), 0644)

		}
	}
}