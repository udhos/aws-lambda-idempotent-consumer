package consumer

import (
	"testing"
	"time"
)

const defaultVisibilityTimeout = 1 * time.Second

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
	err := db.updateItem(100.0, "account_cond", "op1", "attribute_not_exists(id)")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	err = db.updateItem(100.0, "account_cond", "op1", "attribute_not_exists(id)")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	conditionedBalance, err := db.getBalance("account_cond")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if conditionedBalance != 100.0 {
		t.Fatalf("expected conditioned balance to be 100.0, got %v", conditionedBalance)
	}

	// Without condition expression, duplicate operation ID should be applied twice.
	err = db.updateItem(100.0, "account_uncond", "op1", "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	err = db.updateItem(100.0, "account_uncond", "op1", "")
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

	err := db.updateItem(100.0, "account1", "op1", "attribute_not_exists(unknown)")
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

	if len(q.queue) != 0 {
		t.Fatalf("expected queue to be empty after successful retry, got %d messages", len(q.queue))
	}
}
