package com.example;

import com.example.dto.PriorityMessage;
import org.junit.Before;
import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.*;

public class InMemoryPriorityQueueServiceTest {

    private InMemoryPriorityQueueService qs;
    private final String queueUrl = "https://sqs.ap-1.amazonaws.com/007/MyQueue";

    @Before
    public void setup() {
        qs = new InMemoryPriorityQueueService();
    }

    /**
     * Test method to test Push, Pull and Delete Method
     */
    @Test
    public void testSendMessage() {
        String msgBody = "{\n" +
            "    \"message\": \"Welcome 2\",\n" +
            "    \"priority\": 100\n" +
            "}";
        qs.push(queueUrl, msgBody);
        PriorityMessage msg = qs.pull(queueUrl);

        assertNotNull(msg);
        assertEquals(msgBody, msg.getMsgBody());
    }

    /**
     * Test method when Default Priority is Assigned
     */
    @Test
    public void testPullMessageWithDefaultPriority() {
        String msgBody = "{\n" +
            "    \"message\": \"Welcome 2\",\n" +
            "}";
        qs.push(queueUrl, msgBody);
        PriorityMessage msg = qs.pull(queueUrl);

        assertEquals(msgBody, msg.getBody());
        assertEquals(0, msg.getPriority());
        assertTrue(msg.getReceiptId() != null && msg.getReceiptId().length() > 0);
    }

    /**
     * Test method when Priority is defined
     */
    @Test
    public void testPullMessageWithDefinedPriority() {
        String msgBody = "{\n" +
                "    \"message\": \"Welcome 1\",\n" +
                "    \"priority\": 100\n" +
                "}";

        qs.push(queueUrl, msgBody);
        PriorityMessage msg = qs.pull(queueUrl);

        assertEquals(msgBody, msg.getBody());
        assertEquals(100, msg.getPriority());
        assertTrue(msg.getReceiptId() != null && msg.getReceiptId().length() > 0);
    }

    /**
     * Test method when Pull and having different priorities defined
     */
    @Test
    public void testPullMultipleMessage() {
        String msgBody1 = "{\n" +
            "    \"message\": \"Welcome 1\",\n" +
            "    \"priority\": 10\n" +
            "}";
        String msgBody2 = "{\n" +
            "    \"message\": \"Welcome 2\",\n" +
            "    \"priority\": 100\n" +
            "}";
        String msgBody3 = "{\n" +
            "    \"message\": \"Welcome 3\",\n" +
            "    \"priority\": 1\n" +
            "}";

        // push all 3 messages
        qs.push(queueUrl, msgBody1);
        qs.push(queueUrl, msgBody2);
        qs.push(queueUrl, msgBody3);

        // pull 1st message and delete it
        PriorityMessage pulledMsg1 = qs.pull(queueUrl);
        qs.delete(queueUrl, pulledMsg1.getReceiptId());

        // pull 2nd message and delete it
        PriorityMessage pulledMsg2 = qs.pull(queueUrl);
        qs.delete(queueUrl, pulledMsg2.getReceiptId());

        // pull 3rd message and delete it
        PriorityMessage pulledMsg3 = qs.pull(queueUrl);
        qs.delete(queueUrl, pulledMsg3.getReceiptId());

        // assert all 3 pulled messages are not null
        assertNotNull(pulledMsg1);
        assertNotNull(pulledMsg2);
        assertNotNull(pulledMsg3);

        // message 1 will be priority as 100
        assertEquals(msgBody2, pulledMsg1.getBody());
        assertEquals(100, pulledMsg1.getPriority());
        assertTrue(pulledMsg1.getReceiptId() != null && pulledMsg1.getReceiptId().length() > 0);

        // message 2 will be priority as 10
        assertEquals(msgBody1, pulledMsg2.getBody());
        assertEquals(10, pulledMsg2.getPriority());
        assertTrue(pulledMsg2.getReceiptId() != null && pulledMsg2.getReceiptId().length() > 0);

        // message 1 will be priority as 1
        assertEquals(msgBody3, pulledMsg3.getBody());
        assertEquals(1, pulledMsg3.getPriority());
        assertTrue(pulledMsg3.getReceiptId() != null && pulledMsg3.getReceiptId().length() > 0);
    }

    /**
     * Test method when Queue is Empty
     */
    @Test
    public void testPullEmptyQueue() {
        PriorityMessage msg = qs.pull(queueUrl);
        assertNull(msg);
    }

    /**
     * Test method to test the Delete functionality
     */
    @Test
    public void testDeleteMessage() {
        String msgBody = "{\n" +
                "    \"message\": \"Welcome 3\",\n" +
                "    \"priority\": 1\n" +
                "}";

        qs.push(queueUrl, msgBody);
        PriorityMessage msg = qs.pull(queueUrl);

        qs.delete(queueUrl, msg.getReceiptId());
        msg = qs.pull(queueUrl);

        assertNull(msg);
    }

    /**
     * Test method to test the FIFO ordering when Priorities are same
     * @throws InterruptedException
     */
    @Test
    public void testFIFO3Msgs() throws InterruptedException {
        String[] msgStrs = {
                "{\n" +
                        "    \"message\": \"Welcome 1\",\n" +
                        "    \"priority\": 1\n" +
                        "}",
                "{\n" +
                        "    \"message\": \"Welcome 2\",\n" +
                        "    \"priority\": 1\n" +
                        "}",
                "{\n" +
                        "    \"message\": \"Welcome 3\",\n" +
                        "    \"priority\": 1\n" +
                        "}"
        };

        // push all 3 messages
        qs.push(queueUrl, msgStrs[0]);
        Thread.sleep(500);
        qs.push(queueUrl, msgStrs[1]);
        Thread.sleep(500);
        qs.push(queueUrl, msgStrs[2]);

        // pull first message
        PriorityMessage msg1 = qs.pull(queueUrl);
        qs.delete(queueUrl, msg1.getReceiptId());

        // pull second message
        PriorityMessage msg2 = qs.pull(queueUrl);
        qs.delete(queueUrl, msg2.getReceiptId());

        // pull third message
        PriorityMessage msg3 = qs.pull(queueUrl);
        qs.delete(queueUrl, msg3.getReceiptId());

        // all messages will be of same priority
        assertEquals(1, msg1.getPriority());
        assertEquals(1, msg2.getPriority());
        assertEquals(1, msg3.getPriority());

        // assert first pulled message has msgBody1 and likewise for second & third message
        assertEquals(msgStrs[0], msg1.getBody());
        assertEquals(msgStrs[1], msg2.getBody());
        assertEquals(msgStrs[2], msg3.getBody());
    }

    /**
     * Test method to test timeout
     */
    @Test
    public void testAckTimeout() {

        String msgBody = "{\n" +
            "    \"message\": \"Welcome 1\",\n" +
            "    \"priority\": 1\n" +
            "}";

        InMemoryPriorityQueueService queueService = new InMemoryPriorityQueueService() {
            long now() {
                return System.currentTimeMillis() + 1000 * 30 + 1;
            }
        };

        queueService.push(queueUrl, msgBody);
        queueService.pull(queueUrl);
        PriorityMessage msg = queueService.pull(queueUrl);
        assertTrue(msg != null && Objects.equals(msg.getBody(), msgBody));
    }
}