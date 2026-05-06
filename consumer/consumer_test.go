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
