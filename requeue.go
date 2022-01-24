package rabbit

import "github.com/streadway/amqp"

func RejectRequeueOnce(msg *amqp.Delivery) {
	if msg.Redelivered {
		msg.Reject(false)
		return
	}
	msg.Reject(true)
}
