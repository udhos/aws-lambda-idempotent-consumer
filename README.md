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
