package com.example;

import com.example.dto.PriorityMessage;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class RedisQueueServiceTest {
    private RedisQueueService qs;
    private final String queueUrl = "mystash";

    @Before
    public void setup() {
        qs = new RedisQueueService();
    }

    /**
     * Test method to test Push, Pull and Delete Functionality
     */
    @Test
    public void testSendMessage() {
        String msgJson = "{\n" +
                "  \"message\": {\n" +
                "    \"msgBody\": \"Good message!\"\n" +
                "  }\n" +
                "}";
        qs.push(queueUrl, msgJson);
        PriorityMessage msg = qs.pull(queueUrl);

        assertNotNull(msg);
        assertEquals("Good message!", msg.getBody());
        assertEquals(0, msg.getPriority());

        qs.delete(queueUrl, msg.getReceiptId());
    }

    /**
     * Test method to test for Default Priority
     */
    @Test
    public void testPullMessageWithDefaultPriority() {
        String msgJson = "{\n" +
                "  \"message\": {\n" +
                "    \"msgBody\": \"Good message!\"\n" +
                "  }\n" +
                "}";

        qs.push(queueUrl, msgJson);
        PriorityMessage msg = qs.pull(queueUrl);

        assertEquals("Good message!", msg.getBody());
        assertEquals(0, msg.getPriority());
        assertTrue(msg.getReceiptId() != null && msg.getReceiptId().length() > 0);

        qs.delete(queueUrl, msg.getReceiptId());
    }

    /**
     * Test method to Test for different priorities
     */
    @Test
    public void testPullMultipleMessage() {
        String[] msgJson = {
                "{\n" +
                        "  \"message\": {\n" +
                        "    \"msgBody\": \"Good message1!\"\n" +
                        "  },\n" +
                        "  \"score\": 1\n" +
                        "}",
                "{\n" +
                        "  \"message\": {\n" +
                        "    \"msgBody\": \"Good message2!\"\n" +
                        "  },\n" +
                        "  \"score\": 100\n" +
                        "}",
                "{\n" +
                        "  \"message\": {\n" +
                        "    \"msgBody\": \"Good message3!\"\n" +
                        "  },\n" +
                        "  \"score\": 10\n" +
                        "}"
        };

        // push all 3 messages
        qs.push(queueUrl, msgJson[0]);
        qs.push(queueUrl, msgJson[1]);
        qs.push(queueUrl, msgJson[2]);

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
        assertEquals("Good message2!", pulledMsg1.getBody());
        assertEquals(100, pulledMsg1.getPriority());
        assertTrue(pulledMsg1.getReceiptId() != null && pulledMsg1.getReceiptId().length() > 0);

        // message 2 will be priority as 10
        assertEquals("Good message3!", pulledMsg2.getBody());
        assertEquals(10, pulledMsg2.getPriority());
        assertTrue(pulledMsg2.getReceiptId() != null && pulledMsg2.getReceiptId().length() > 0);

        // message 1 will be priority as 1
        assertEquals("Good message1!", pulledMsg3.getBody());
        assertEquals(1, pulledMsg3.getPriority());
        assertTrue(pulledMsg3.getReceiptId() != null && pulledMsg3.getReceiptId().length() > 0);
    }

    /**
     * Test method to test the behaviour when Pulling from the Empty Queue
     */
    @Test
    public void testPullEmptyQueue() {
        PriorityMessage msg = qs.pull(queueUrl);
        assertNull(msg);
    }

    /**
     * Test Method to verify the FIFO ordering when Priorities are equal
     */
    @Test
    public void testFIFO2Msgs() {


        String[] msgStrs = {
                "{\n" +
                        "  \"message\": {\n" +
                        "    \"msgBody\": \"Good message1!\"\n" +
                        "  },\n" +
                        "  \"score\": 1\n" +
                        "}",
                "{\n" +
                        "  \"message\": {\n" +
                        "    \"msgBody\": \"Good message2!\"\n" +
                        "  },\n" +
                        "  \"score\": 1\n" +
                        "}"
        };

        qs.push(queueUrl, msgStrs[0]);
        qs.push(queueUrl, msgStrs[1]);

        PriorityMessage msg1 = qs.pull(queueUrl);
        qs.delete(queueUrl, msg1.getReceiptId());

        PriorityMessage msg2 = qs.pull(queueUrl);
        qs.delete(queueUrl, msg2.getReceiptId());

        assertEquals(1, msg1.getPriority());
        assertEquals(1, msg2.getPriority());

        assertEquals("Good message1!", msg1.getBody());
        assertEquals("Good message2!", msg2.getBody());
    }
}