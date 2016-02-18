package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pubnub/go/messaging"
)

var pubnub *messaging.Pubnub
var pubnubServiceLogger *messaging.Pubnub

const (
	publishKey          string = "pub-c-8ef7e630-2aff-4a02-9225-843942a32ef7"
	subscribeKey        string = "sub-c-0675159c-d4c6-11e5-a9b2-02ee2ddab7fe"
	historyPublishCount int    = 5
	historySleep        int    = 5
)

func main() {
	done := make(chan bool)
	pubnub = messaging.NewPubnub(publishKey, subscribeKey, "", "", false, "")
	pubnubServiceLogger =
		messaging.NewPubnub(publishKey, subscribeKey, "", "", false, "")
	go historyWorker()
	<-done
}

func historyWorker() {
	for {
		delay := historyRequest()
		publishResult("history", delay)
		time.Sleep(2 * time.Second)
		logError(fmt.Sprintf("diff: %s", delay.Diff))
	}
}

func historyRequest() (delay Delay) {
	var published time.Time

	channel := uuid()
	successChannel := make(chan []byte)
	errorChannel := make(chan []byte)
	publishedMessages := make([]string, historyPublishCount)

	now := time.Now()
	delay.Started = now
	fmt.Println("Published at", now)

	for i := 0; i < historyPublishCount; i++ {
		message := uuid()

		go pubnub.Publish(channel, message, successChannel, errorChannel)
		select {
		case <-successChannel:
			publishedMessages[i] = message
		case err := <-errorChannel:
			logError(string(err))
			return
		case <-messaging.Timeout():
			logError("Publish timeout")
			return
		}
	}

	published = time.Now()

historyLoop:
	for {
		go pubnub.History(channel, historyPublishCount, 0, 0, false,
			successChannel, errorChannel)
		select {
		case msg := <-successChannel:
			var resp []interface{}
			err := json.Unmarshal(msg, &resp)
			if err != nil {
				logError(err.Error())
			}

			if resp[1].(string) == "0" {
				// logError("is zero")
				break
			}

			switch rs := resp[0].(type) {
			case []interface{}:
				// TODO: assert messages too
				if len(rs) == historyPublishCount {
					break historyLoop
				} else {
					logError(fmt.Sprintf("Length mismatch: %d (expected) vs %d (actual)",
						len(publishedMessages), len(rs)))
				}
			default:
				logError("Message is not sting")
			}
		case err := <-errorChannel:
			logError(string(err))
			return
		case <-messaging.Timeout():
			logError("History timeout")
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	delay.Diff = time.Since(published)

	now = time.Now()
	delay.Finished = now
	fmt.Println("Finished at", now)

	return delay
}

func publishResult(service string, delay Delay) {
	successChannel := make(chan []byte)
	errorChannel := make(chan []byte)

	go pubnub.Publish(fmt.Sprintf("logs-%s", service),
		delay, successChannel, errorChannel)
	select {
	case <-successChannel:
	case err := <-errorChannel:
		logError(string(err))
		return
	case <-messaging.Timeout():
		logError("Logging timeout")
		return
	}
}

func uuid() string {
	uuid, _ := messaging.GenUuid()
	return uuid
}

func logError(message string) {
	fmt.Println(message)
}

type Delay struct {
	Started   time.Time
	Finished  time.Time
	Diff      time.Duration
	Errors    []string
	Responses []string
}

type historyResponse struct {
	Messages []string
	Start    string
	End      string
}
