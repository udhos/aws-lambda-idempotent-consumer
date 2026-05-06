package consumer

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

const defaultVisibilityTimeout = 1 * time.Second

type transientFailDB struct {
	inner       *dynamodb
	failUpdates int
	mu          sync.Mutex
}

type failDeleteQueue struct {
	inner       *sqsQueue
	failDeletes int
	mu          sync.Mutex
}

func (q *failDeleteQueue) send(op operation) error {
	return q.inner.send(op)
}

func (q *failDeleteQueue) receive() (message, error) {
	return q.inner.receive()
}

func (q *failDeleteQueue) delete(receiptHandle string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.failDeletes > 0 {
		q.failDeletes--
		return fmt.Errorf("simulated delete failure")
	}

	return q.inner.delete(receiptHandle)
}

func (q *failDeleteQueue) size() int {
	return q.inner.size()
}

func (d *transientFailDB) transactWriteItems(input transactWriteItemsInput) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.failUpdates > 0 {
		d.failUpdates--
		return fmt.Errorf("transient dynamodb error")
	}
	return d.inner.transactWriteItems(input)
}

func (d *transientFailDB) getBalance(accountID string) (float64, error) {
	return d.inner.getBalance(accountID)
}

func TestDeposit(t *testing.T) {
	q := newSQSQueue(defaultVisibilityTimeout)

	db := &dynamodb{balance: make(map[string]float64)}
	fn := &lambdaFunction{}

	op := operation{
		ID:            "1",
		OperationName: "deposit",
		Amount:        100.0,
		AccountID:     "account1",
	}

	q.send(op)

	err := fn.invoke(q, db)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	balance, err := db.getBalance("account1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if balance != 100.0 {
		t.Fatalf("expected balance to be 100.0, got %v", balance)
	}
}

func TestWithdraw(t *testing.T) {
	db := &dynamodb{balance: make(map[string]float64)}
	fn := &lambdaFunction{}
	q := newSQSQueue(defaultVisibilityTimeout)

	// First, deposit some money
	depositOp := operation{
		ID:            "1",
		OperationName: "deposit",
		Amount:        100.0,
		AccountID:     "account1",
	}
	q.send(depositOp)
	err := fn.invoke(q, db)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Now, withdraw some money
	withdrawOp := operation{
		ID:            "2",
		OperationName: "withdraw",
		Amount:        50.0,
		AccountID:     "account1",
	}
	q.send(withdrawOp)
	err = fn.invoke(q, db)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	balance, err := db.getBalance("account1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if balance != 50.0 {
		t.Fatalf("expected balance to be 50.0, got %v", balance)
	}
}

func TestDuplicateMessageIsProcessedOnce(t *testing.T) {
	q := newSQSQueue(defaultVisibilityTimeout)
	db := &dynamodb{balance: make(map[string]float64)}
	fn := &lambdaFunction{}

	op := operation{
		ID:            "1",
		OperationName: "deposit",
		Amount:        100.0,
		AccountID:     "account1",
	}

	// Simulate SQS duplicating the same message.
	q.send(op)
	q.send(op)

	err := fn.invoke(q, db)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = fn.invoke(q, db)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	balance, err := db.getBalance("account1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if balance != 100.0 {
		t.Fatalf("expected balance to be 100.0 after duplicate message, got %v", balance)
	}
}

func TestConditionExpressionAffectsDuplicateHandling(t *testing.T) {
	db := &dynamodb{balance: make(map[string]float64)}

	// With condition expression, duplicate operation ID should be ignored.
	err := db.transactWriteItems(transactWriteItemsInput{
		Put: putItemInput{
			AccountID:           "account_cond",
			OperationID:         "op1",
			ConditionExpression: "attribute_not_exists(id)",
		},
		Update: updateItemInput{AccountID: "account_cond", Delta: 100.0},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	err = db.transactWriteItems(transactWriteItemsInput{
		Put: putItemInput{
			AccountID:           "account_cond",
			OperationID:         "op1",
			ConditionExpression: "attribute_not_exists(id)",
		},
		Update: updateItemInput{AccountID: "account_cond", Delta: 100.0},
	})
	if err == nil {
		t.Fatalf("expected conditional check failure for duplicate operation")
	}
	if err != errConditionalCheckFailed {
		t.Fatalf("expected errConditionalCheckFailed, got %v", err)
	}

	conditionedBalance, err := db.getBalance("account_cond")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if conditionedBalance != 100.0 {
		t.Fatalf("expected conditioned balance to be 100.0, got %v", conditionedBalance)
	}

	// Without condition expression, duplicate operation ID should be applied twice.
	err = db.transactWriteItems(transactWriteItemsInput{
		Put: putItemInput{
			AccountID:           "account_uncond",
			OperationID:         "op1",
			ConditionExpression: "",
		},
		Update: updateItemInput{AccountID: "account_uncond", Delta: 100.0},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	err = db.transactWriteItems(transactWriteItemsInput{
		Put: putItemInput{
			AccountID:           "account_uncond",
			OperationID:         "op1",
			ConditionExpression: "",
		},
		Update: updateItemInput{AccountID: "account_uncond", Delta: 100.0},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	unconditionedBalance, err := db.getBalance("account_uncond")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if unconditionedBalance != 200.0 {
		t.Fatalf("expected unconditioned balance to be 200.0, got %v", unconditionedBalance)
	}
}

func TestUnknownConditionExpressionIsRefused(t *testing.T) {
	db := &dynamodb{balance: make(map[string]float64)}

	err := db.transactWriteItems(transactWriteItemsInput{
		Put: putItemInput{
			AccountID:           "account1",
			OperationID:         "op1",
			ConditionExpression: "attribute_not_exists(unknown)",
		},
		Update: updateItemInput{AccountID: "account1", Delta: 100.0},
	})
	if err == nil {
		t.Fatalf("expected error for unknown condition expression")
	}

	balance, err := db.getBalance("account1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if balance != 0.0 {
		t.Fatalf("expected balance to remain 0.0, got %v", balance)
	}
}

func TestLambdaFailureCausesReissueAndRetryProcessesMessage(t *testing.T) {
	visibilityTimeout := 20 * time.Millisecond
	q := newSQSQueue(visibilityTimeout)
	db := &dynamodb{balance: make(map[string]float64)}
	fn := &lambdaFunction{injectCrashAfterReceive: true}

	op := operation{
		ID:            "h2-1",
		OperationName: "deposit",
		Amount:        100.0,
		AccountID:     "account_h2",
	}

	q.send(op)

	// First invocation crashes after receive; message should not be deleted.
	err := fn.invoke(q, db)
	if err == nil {
		t.Fatalf("expected error from simulated crash, got nil")
	}

	// Before visibility timeout expires, message should still be invisible.
	fn.injectCrashAfterReceive = false
	err = fn.invoke(q, db)
	if err == nil {
		t.Fatalf("expected no visible messages before visibility timeout")
	}

	// After timeout, message is reissued and should be processed successfully.
	time.Sleep(visibilityTimeout + 10*time.Millisecond)
	err = fn.invoke(q, db)
	if err != nil {
		t.Fatalf("expected no error on retry after visibility timeout, got %v", err)
	}

	balance, err := db.getBalance("account_h2")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if balance != 100.0 {
		t.Fatalf("expected balance to be 100.0 after retry, got %v", balance)
	}

	if q.size() != 0 {
		t.Fatalf("expected queue to be empty after successful retry, got %d messages", q.size())
	}
}

func TestTransientDynamoDBErrorCausesRetryThenSuccess(t *testing.T) {
	visibilityTimeout := 20 * time.Millisecond
	q := newSQSQueue(visibilityTimeout)
	baseDB := &dynamodb{balance: make(map[string]float64)}
	db := &transientFailDB{inner: baseDB, failUpdates: 1}
	fn := &lambdaFunction{}

	op := operation{
		ID:            "h4-1",
		OperationName: "deposit",
		Amount:        100.0,
		AccountID:     "account_h4",
	}

	q.send(op)

	// First attempt fails with a transient DynamoDB error, so message is not deleted.
	err := fn.invoke(q, db)
	if err == nil {
		t.Fatalf("expected transient DynamoDB error, got nil")
	}

	// Message should become available again after visibility timeout and then succeed.
	time.Sleep(visibilityTimeout + 10*time.Millisecond)
	err = fn.invoke(q, db)
	if err != nil {
		t.Fatalf("expected no error on retry, got %v", err)
	}

	balance, err := db.getBalance("account_h4")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if balance != 100.0 {
		t.Fatalf("expected balance to be 100.0 after retry, got %v", balance)
	}

	if q.size() != 0 {
		t.Fatalf("expected queue to be empty after successful retry, got %d messages", q.size())
	}
}

func TestConcurrentUpdatesToSameAccountAreConsistent(t *testing.T) {
	const workers = 100

	db := &dynamodb{balance: make(map[string]float64)}
	fn := &lambdaFunction{}

	start := make(chan struct{})
	errs := make(chan error, workers)

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			op := operation{
				ID:            fmt.Sprintf("h3-%d", i),
				OperationName: "deposit",
				Amount:        1.0,
				AccountID:     "account_h3",
			}
			errs <- fn.update(op, db)
		}(i)
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	}

	balance, err := db.getBalance("account_h3")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if balance != workers {
		t.Fatalf("expected balance to be %d, got %v", workers, balance)
	}
}

func TestConcurrentDuplicateOperationIDIsAppliedOnce(t *testing.T) {
	const duplicates = 100

	q := newSQSQueue(defaultVisibilityTimeout)
	db := &dynamodb{balance: make(map[string]float64)}

	op := operation{
		ID:            "h3-dup-op",
		OperationName: "deposit",
		Amount:        1.0,
		AccountID:     "account_h3_dup",
	}
	for range duplicates {
		q.send(op)
	}

	start := make(chan struct{})
	errs := make(chan error, duplicates)

	var wg sync.WaitGroup
	for range duplicates {
		wg.Go(func() {
			<-start
			fn := &lambdaFunction{}
			errs <- fn.invoke(q, db)
		})
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	}

	if q.size() != 0 {
		t.Fatalf("expected queue to be empty after processing duplicates, got %d messages", q.size())
	}

	balance, err := db.getBalance("account_h3_dup")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if balance != 1.0 {
		t.Fatalf("expected balance to be 1.0 with duplicate operation ID, got %v", balance)
	}
}

func TestConcurrentLambdasProcessDifferentMessages(t *testing.T) {
	const workers = 100

	q := newSQSQueue(defaultVisibilityTimeout)
	db := &dynamodb{balance: make(map[string]float64)}

	for i := range workers {
		op := operation{
			ID:            fmt.Sprintf("h7-%d", i),
			OperationName: "deposit",
			Amount:        1.0,
			AccountID:     fmt.Sprintf("account_h7_%d", i),
		}
		q.send(op)
	}

	start := make(chan struct{})
	errs := make(chan error, workers)

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			<-start
			fn := &lambdaFunction{}
			errs <- fn.invoke(q, db)
		})
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	}

	if q.size() != 0 {
		t.Fatalf("expected queue to be empty after concurrent processing, got %d messages", q.size())
	}

	for i := range workers {
		balance, err := db.getBalance(fmt.Sprintf("account_h7_%d", i))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if balance != 1.0 {
			t.Fatalf("expected account %d balance to be 1.0, got %v", i, balance)
		}
	}
}

func TestTimeoutBeforeDeleteCausesReissueWithoutDoubleApply(t *testing.T) {
	visibilityTimeout := 20 * time.Millisecond
	q := newSQSQueue(visibilityTimeout)
	db := &dynamodb{balance: make(map[string]float64)}
	fn := &lambdaFunction{injectTimeoutBeforeDelete: true}

	op := operation{
		ID:            "h6-1",
		OperationName: "deposit",
		Amount:        100.0,
		AccountID:     "account_h6",
	}

	q.send(op)

	// First invocation times out after update but before delete.
	err := fn.invoke(q, db)
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}

	// The operation should already have been applied once.
	balance, err := db.getBalance("account_h6")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if balance != 100.0 {
		t.Fatalf("expected balance to be 100.0 after timeout, got %v", balance)
	}

	// After visibility timeout, the message is reissued; idempotency prevents double apply.
	fn.injectTimeoutBeforeDelete = false
	time.Sleep(visibilityTimeout + 10*time.Millisecond)
	err = fn.invoke(q, db)
	if err != nil {
		t.Fatalf("expected no error on retry after timeout, got %v", err)
	}

	balance, err = db.getBalance("account_h6")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if balance != 100.0 {
		t.Fatalf("expected balance to remain 100.0 after retry, got %v", balance)
	}

	if q.size() != 0 {
		t.Fatalf("expected queue to be empty after successful retry, got %d messages", q.size())
	}
}

func TestMessageIsNotLostByFunctionDuringRetries(t *testing.T) {
	visibilityTimeout := 20 * time.Millisecond
	q := newSQSQueue(visibilityTimeout)
	baseDB := &dynamodb{balance: make(map[string]float64)}
	db := &transientFailDB{inner: baseDB, failUpdates: 3}
	fn := &lambdaFunction{}

	op := operation{
		ID:            "h10-1",
		OperationName: "deposit",
		Amount:        100.0,
		AccountID:     "account_h10",
	}

	q.send(op)

	for attempt := 1; attempt <= 3; attempt++ {
		err := fn.invoke(q, db)
		if err == nil {
			t.Fatalf("expected transient error on attempt %d, got nil", attempt)
		}

		if q.size() != 1 {
			t.Fatalf("expected message to remain in queue after failed attempt %d, got %d messages", attempt, q.size())
		}

		receiveCount := q.receiveCountByOperationID(op.ID)
		if receiveCount != attempt {
			t.Fatalf("expected receive count %d after failed attempt, got %d", attempt, receiveCount)
		}

		time.Sleep(visibilityTimeout + 10*time.Millisecond)
	}

	err := fn.invoke(q, db)
	if err != nil {
		t.Fatalf("expected success after transient failures, got %v", err)
	}

	if q.size() != 0 {
		t.Fatalf("expected queue to be empty after successful processing, got %d messages", q.size())
	}

	balance, err := db.getBalance("account_h10")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if balance != 100.0 {
		t.Fatalf("expected balance to be 100.0 after retries, got %v", balance)
	}
}

func TestConcurrentRetriesDoNotLoseMessage(t *testing.T) {
	visibilityTimeout := 20 * time.Millisecond
	q := newSQSQueue(visibilityTimeout)
	baseDB := &dynamodb{balance: make(map[string]float64)}
	db := &transientFailDB{inner: baseDB, failUpdates: 8}

	op := operation{
		ID:            "h10-concurrent-1",
		OperationName: "deposit",
		Amount:        100.0,
		AccountID:     "account_h10_concurrent",
	}
	q.send(op)

	const workers = 8
	const attemptsPerWorker = 12
	start := make(chan struct{})

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			<-start
			fn := &lambdaFunction{}
			for range attemptsPerWorker {
				_ = fn.invoke(q, db)
				time.Sleep(visibilityTimeout + 2*time.Millisecond)
			}
		})
	}

	close(start)
	wg.Wait()

	if q.size() != 0 {
		t.Fatalf("expected queue to be empty after concurrent retries converge, got %d messages", q.size())
	}

	balance, err := db.getBalance("account_h10_concurrent")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if balance != 100.0 {
		t.Fatalf("expected balance to be 100.0 after concurrent retries, got %v", balance)
	}
}

func TestDeleteFailureCausesRetryWithoutDoubleApply(t *testing.T) {
	visibilityTimeout := 20 * time.Millisecond
	baseQueue := newSQSQueue(visibilityTimeout)
	q := &failDeleteQueue{inner: baseQueue, failDeletes: 1}
	db := &dynamodb{balance: make(map[string]float64)}
	fn := &lambdaFunction{}

	op := operation{
		ID:            "delete-fail-1",
		OperationName: "deposit",
		Amount:        100.0,
		AccountID:     "account_delete_fail",
	}
	q.send(op)

	// First attempt commits the update but fails to delete the message.
	err := fn.invoke(q, db)
	if err == nil {
		t.Fatalf("expected delete failure, got nil")
	}

	if q.size() != 1 {
		t.Fatalf("expected message to remain in queue after delete failure, got %d", q.size())
	}

	balance, err := db.getBalance("account_delete_fail")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if balance != 100.0 {
		t.Fatalf("expected balance to be 100.0 after first attempt, got %v", balance)
	}

	// Retry should succeed and idempotency must prevent double apply.
	time.Sleep(visibilityTimeout + 10*time.Millisecond)
	err = fn.invoke(q, db)
	if err != nil {
		t.Fatalf("expected no error on retry after delete failure, got %v", err)
	}

	if q.size() != 0 {
		t.Fatalf("expected queue to be empty after successful retry, got %d", q.size())
	}

	balance, err = db.getBalance("account_delete_fail")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if balance != 100.0 {
		t.Fatalf("expected balance to remain 100.0 after retry, got %v", balance)
	}
}
