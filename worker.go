package main

import (
	"fmt"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func (w *Worker) Run() {
	verboseLogger.Printf("[%d] initializing\n", w.WorkerId)

	queue := make(chan [2]string)
	cid := w.WorkerId
	t := randomSource.Int31()

	topicName := fmt.Sprintf(topicNameTemplate, t)
	publisherClientId := fmt.Sprintf(publisherClientIdTemplate, w.WorkerId, t)

	verboseLogger.Printf("[%d] topic=%s publisherClientId=%s\n", cid, topicName, publisherClientId)

	publisherOptions := mqtt.NewClientOptions().SetClientID(publisherClientId).SetUsername(w.Username).SetPassword(w.Password).AddBroker(w.BrokerUrl)

	publisherOptions.SetCleanSession(false)
	publisherOptions.SetProtocolVersion(3)
	publisherOptions.SetAutoReconnect(false)
	publisherOptions.SetTLSConfig(config)

	publisherOptions.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		if !strings.HasPrefix(string(msg.Payload()), `{"Error":""`) {
			fmt.Println("publish", msg.Topic(), string(msg.Payload()))
		}
		queue <- [2]string{msg.Topic(), string(msg.Payload())}
	})
	publisher := mqtt.NewClient(publisherOptions)

	verboseLogger.Printf("[%d] connecting publisher\n", w.WorkerId)
	if token := publisher.Connect(); token.WaitTimeout(opTimeout) && token.Error() != nil {
		resultChan <- Result{
			WorkerId:     w.WorkerId,
			Event:        "ConnectFailed",
			Error:        true,
			ErrorMessage: token.Error(),
		}
		return
	}

	defer func() {
		verboseLogger.Printf("[%d] Disconnect\n", w.WorkerId)
		publisher.Disconnect(5)
	}()

	verboseLogger.Printf("[%d] starting control loop %s\n", w.WorkerId, topicName)

	timeout := make(chan bool, 1)
	stopWorker := false
	receivedCount := 0
	publishedCount := 0

	t0 := time.Now()
	for i := 0; i < w.Nmessages; i++ {
		text := fmt.Sprintf(`{"userName": "C2-User(%d)", "passWord": "C2-Pass(%d)"}`, i, i)
		token := publisher.Publish(topicName, 0, false, text)
		publishedCount++
		token.Wait()
	}
	//publisher.Disconnect(250)

	publishTime := time.Since(t0)
	verboseLogger.Printf("[%d] all messages published\n", w.WorkerId)

	go func() {
		time.Sleep(w.Timeout)
		timeout <- true
	}()

	t0 = time.Now()
	for receivedCount < w.Nmessages && !stopWorker {
		select {
		case <-queue:
			receivedCount++

			verboseLogger.Printf("[%d] %d/%d received\n", w.WorkerId, receivedCount, w.Nmessages)
			if receivedCount == w.Nmessages {
				resultChan <- Result{
					WorkerId:          w.WorkerId,
					Event:             "Completed",
					PublishTime:       publishTime,
					ReceiveTime:       time.Since(t0),
					MessagesReceived:  receivedCount,
					MessagesPublished: publishedCount,
				}
			} else {
				resultChan <- Result{
					WorkerId:          w.WorkerId,
					Event:             "ProgressReport",
					PublishTime:       publishTime,
					ReceiveTime:       time.Since(t0),
					MessagesReceived:  receivedCount,
					MessagesPublished: publishedCount,
				}
			}
		case <-timeout:
			verboseLogger.Printf("[%d] timeout!!\n", cid)
			stopWorker = true

			resultChan <- Result{
				WorkerId:          w.WorkerId,
				Event:             "TimeoutExceeded",
				PublishTime:       publishTime,
				MessagesReceived:  receivedCount,
				MessagesPublished: publishedCount,
				Error:             true,
			}
		case <-abortChan:
			verboseLogger.Printf("[%d] received abort signal", w.WorkerId)
			stopWorker = true

			resultChan <- Result{
				WorkerId:          w.WorkerId,
				Event:             "Aborted",
				PublishTime:       publishTime,
				MessagesReceived:  receivedCount,
				MessagesPublished: publishedCount,
				Error:             false,
			}
		}
	}

	verboseLogger.Printf("[%d] worker finished\n", w.WorkerId)
}
