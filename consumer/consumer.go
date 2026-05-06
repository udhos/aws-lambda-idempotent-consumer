// Package consumer implements the idempotent consumer for the message queue.
package consumer

import (
	"fmt"
	"sync"
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
	updateItem(amount float64, accountID, operationID, conditionExpression string) error
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

// sqsQueue is a simple in-memory implementation of the queue interface for testing purposes.
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

// dynamodb is a simple in-memory implementation of the database interface for testing purposes.
type dynamodb struct {
	balance   map[string]float64  // accountID -> balance
	processed map[string]struct{} // accountID:operationID -> seen
	mu        sync.Mutex
}

func (d *dynamodb) updateItem(amount float64, accountID,
	operationID, conditionExpression string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	switch conditionExpression {
	case "":
		// No condition expression means unconditional update.
	case "attribute_not_exists(id)":
		if d.processed == nil {
			d.processed = make(map[string]struct{})
		}

		key := fmt.Sprintf("%s:%s", accountID, operationID)
		if _, exists := d.processed[key]; exists {
			return nil
		}

		d.processed[key] = struct{}{}
	default:
		return fmt.Errorf("unsupported condition expression: %s", conditionExpression)
	}

	d.balance[accountID] += amount
	return nil
}

func (d *dynamodb) getBalance(accountID string) (float64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.balance[accountID], nil
}

// lambdaFunction is the implementation of the function interface that will
// be invoked for each operation. this is where the business logic for
// processing the operations will be implemented. this is the main component
// of the consumer that will be tested in the unit tests. in contrast, the
// queue and database implementations are just for testing purposes and
// will not be part of the actual consumer implementation.
type lambdaFunction struct {
	injectCrashAfterReceive   bool
	injectTimeoutBeforeDelete bool
}

func (f *lambdaFunction) invoke(q queue, table database) error {
	msg, err := q.receive()
	if err != nil {
		return err
	}
	if f.injectCrashAfterReceive {
		return fmt.Errorf("simulated crash after receive")
	}
	opErr := f.update(msg.Operation, table)
	if opErr != nil {
		return opErr
	}
	if f.injectTimeoutBeforeDelete {
		return fmt.Errorf("simulated timeout before delete")
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
		return table.updateItem(op.Amount, op.AccountID, op.ID, "attribute_not_exists(id)")
	case "withdraw":
		return table.updateItem(-op.Amount, op.AccountID, op.ID, "attribute_not_exists(id)")
	default:
		return fmt.Errorf("invalid operation name: %s", op.OperationName)
	}
}
