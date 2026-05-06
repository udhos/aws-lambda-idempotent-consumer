// Package consumer implements the idempotent consumer for the message queue.
package consumer

import (
	"fmt"
	"time"
)

type operation struct {
	ID            string  `json:"id"`
	OperationName string  `json:"operation_name"`
	Amount        float64 `json:"amount"`
	AccountID     string  `json:"account_id"`
}

type message struct {
	ReceiptHandle string
	Operation     operation
}

// queue is the abstract interface for the message queue operations.
type queue interface {
	send(op operation) error
	receive() (message, error)
	delete(receiptHandle string) error
}

// database is the abstract interface for the database operations.
type database interface {
	updateItem(amount float64, accountID, conditionExpression string) error
	getBalance(accountID string) (float64, error)
}

// function is the abstract lambda function that will be invoked for each operation.
type function interface {
	invoke(q queue, table database) error
}

type sqsMessage struct {
	VisibleAfter time.Time
	Message      message
}

func (m sqsMessage) isVisible() bool {
	now := time.Now()
	return now.After(m.VisibleAfter)
}

type sqsQueue struct {
	queue             map[string]sqsMessage // receiptHandle -> message
	visibilityTimeout time.Duration
	nextReceiptHandle int
}

func newSQSQueue(visibilityTimeout time.Duration) *sqsQueue {
	return &sqsQueue{
		queue:             make(map[string]sqsMessage),
		visibilityTimeout: visibilityTimeout,
	}
}

func (q *sqsQueue) send(op operation) error {
	// Implementation to send a message to SQS
	msg := message{
		ReceiptHandle: fmt.Sprintf("%d", q.nextReceiptHandle),
		Operation:     op,
	}
	q.nextReceiptHandle++
	q.queue[msg.ReceiptHandle] = sqsMessage{
		Message:      msg,
		VisibleAfter: time.Now(), // Message is immediately visible
	}
	return nil
}

func (q *sqsQueue) receive() (message, error) {
	// find the first visible message
	for _, sqsMsg := range q.queue {
		if sqsMsg.isVisible() {
			// Mark the message as invisible for a certain duration (e.g., 30 seconds)
			sqsMsg.VisibleAfter = time.Now().Add(q.visibilityTimeout)

			// Update the message in the queue
			q.queue[sqsMsg.Message.ReceiptHandle] = sqsMsg

			return sqsMsg.Message, nil
		}
	}
	return message{}, fmt.Errorf("no visible messages")
}

func (q *sqsQueue) delete(receiptHandle string) error {
	// Implementation to delete a message from SQS
	delete(q.queue, receiptHandle)
	return nil
}

type dynamodb struct {
	balance map[string]float64 // accountID -> balance
}

func (d *dynamodb) updateItem(amount float64, accountID,
	conditionExpression string) error {
	d.balance[accountID] += amount
	return nil
}

func (d *dynamodb) getBalance(accountID string) (float64, error) {
	return d.balance[accountID], nil
}

type lambdaFunction struct{}

func (f *lambdaFunction) invoke(q queue, table database) error {
	msg, err := q.receive()
	if err != nil {
		return err
	}
	opErr := f.update(msg.Operation, table)
	if opErr != nil {
		return opErr
	}
	errDelete := q.delete(msg.ReceiptHandle)
	if errDelete != nil {
		return errDelete
	}
	return nil
}

func (f *lambdaFunction) update(op operation, table database) error {
	switch op.OperationName {
	case "deposit":
		return table.updateItem(op.Amount, op.AccountID, "attribute_not_exists(id)")
	case "withdraw":
		return table.updateItem(-op.Amount, op.AccountID, "attribute_not_exists(id)")
	default:
		return fmt.Errorf("invalid operation name: %s", op.OperationName)
	}
}
