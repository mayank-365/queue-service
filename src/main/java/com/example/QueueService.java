package com.example;

public interface QueueService {
  /** push a message onto a queue. */
  void push(String queueUrl, String messageBody);

  /** retrieves a single message from a queue. */
  Message pull(String queueUrl);

  /** deletes a message from the queue that was received by pull(). */
  void delete(String queueUrl, String receiptId);
}
