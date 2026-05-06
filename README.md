# aws-lambda-idempotent-consumer

[aws-lambda-idempotent-consumer](https://github.com/udhos/aws-lambda-idempotent-consumer) demonstrates how to implement an idempotent consumer in AWS Lambda consuming events from SQS queue and using DynamoDB for state management.

# Hurdles

1. SQS might duplicate messages.
2. Lambda failure might cause message to be reissued by SQS and processed again by Lambda.
3. DynamoDB might have concurrent updates to the same item.
4. DynamoDB might have transient errors.
5. Lambda might have transient errors (e.g. function might crash).
6. Lambda might timeout.
7. Concurrent Lambdas might process different messages at the same time.
8. Concurrent Lambdas might process the same message at the same time.
9. The solution cannot apply the same operation twice to the database (e.g. double deposit to the same account).
10. The solution cannot lose messages (e.g. deposit not applied to the account).

# Event format

```json
{
    "id": "1234567890",
    "operation_name": "deposit", # deposit or withdraw
    "amount": 100.0,
    "account_id": "9876543210"
}
```

# Idempotency strategy with DynamoDB primitives

This project implements an idempotent consumer by combining three core behaviors:

1. Receive one message from the queue.
2. Apply the operation in DynamoDB using a conditional primitive tied to operation id.
3. Delete the queue message only after a successful or duplicate-safe database outcome.

## DynamoDB primitive used

The database update is guarded by a condition expression:

- `attribute_not_exists(id)`

Conceptually, this means:

- First time an operation id is seen: condition passes, update is applied.
- Duplicate operation id: condition fails with a conditional check failure.

That conditional check failure is treated by the Lambda logic as an idempotent no-op (already processed), not as a fatal error that should keep retrying forever.

### Concrete DynamoDB primitive shape being simulated

At a DynamoDB API level, the simulation corresponds most closely to a TransactWriteItems call composed of:

1. A conditional marker write for operation id (`attribute_not_exists(id)`).
2. A balance update (`SET balance = if_not_exists(balance, :zero) + :delta`).

Representative transaction intent:

- Put/condition item:
    - identity for `(account_id, operation_id)`
    - `ConditionExpression: attribute_not_exists(id)`
- Update item:
    - account balance row
    - `UpdateExpression: SET balance = if_not_exists(balance, :zero) + :delta`

Semantics:

1. Condition passes: transaction commits, marker + balance update are applied once.
2. Condition fails: DynamoDB returns conditional-check-failed (duplicate).
3. Lambda treats that specific duplicate outcome as no-op success and still acknowledges the SQS message.

In this repository, that behavior is modeled by the transaction-like database interface and in-memory conditional-check-failed simulation for duplicates.

## Simulation-to-DynamoDB field mapping

In the in-memory simulation, the `dynamodb` struct uses two maps:

- `balance[accountID]`: current account balance
- `processed[accountID:operationID]`: dedup marker for already-seen operations

In concrete DynamoDB terms, these map to two logical concerns:

1. Account state item (balance)
2. Operation idempotency marker (processed operation id)

One practical single-table shape is:

- Account item:
    - `PK = ACCOUNT#<account_id>`
    - `SK = META`
    - attribute: `balance`

- Operation marker item:
    - `PK = ACCOUNT#<account_id>`
    - `SK = OP#<operation_id>`
    - optional attributes: operation metadata and timestamp

The in-memory key `accountID:operationID` corresponds to the marker identity `(PK, SK)` above.

When applying an operation, the robust DynamoDB equivalent is a transactional/conditional write that:

1. Creates the operation marker only if it does not exist.
2. Updates account balance.

If marker creation fails with conditional-check-failed, the operation is a duplicate and should be treated as already applied.

## Why duplicate conditional failures are acknowledged

If a duplicate is treated as a hard error, Lambda will not delete the message and SQS will redeliver it forever.

Instead, this consumer treats duplicate detection as successful completion of the business intent:

- no second balance change is applied,
- the message is acknowledged (deleted),
- retry loops are avoided.

## Failure and retry model

The function keeps a strict rule:

- Never delete before the database step.

This gives robust behavior across failures:

- Crash/transient failure before database commit: message is retried, operation eventually applies.
- Timeout or delete failure after database commit: message is retried, duplicate is detected by condition expression, no double-apply occurs, message is eventually deleted.
- Transient DynamoDB error: message remains in queue and is retried until successful.

## Concurrency behavior

Under concurrent processing, the same operation id can be attempted multiple times. The conditional primitive ensures only one attempt can apply the operation, and all other attempts are safely classified as duplicates.

This prevents double writes while still allowing aggressive retry and concurrent execution.

## Mapping to robustness goals

Using the conditional primitive plus delete-after-success flow provides the core guarantees:

- Duplicate deliveries do not cause double-apply.
- Retries after failure or timeout do not lose messages.
- Concurrent processing remains safe for the same logical operation.
- Message acknowledgement is coupled to durable database outcome.

In short: DynamoDB conditional writes provide deterministic deduplication, and Lambda/SQS retry semantics provide eventual processing without data loss from the function itself.
