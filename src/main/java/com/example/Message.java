package com.example;

import lombok.Data;

@Data
public class Message {
  /** How many times this message has been delivered. */
  private int attempts;

  /** Visible from time */
  private long visibleFrom;

  /** An identifier associated with the act of receiving the message. */
  private String receiptId;

  private String msgBody;

  public Message(){}

  public Message(String msgBody) {
    this.msgBody = msgBody;
  }

  public Message(String msgBody, String receiptId) {
    this.msgBody = msgBody;
    this.receiptId = receiptId;
  }

  /*
  public boolean isVisible() {
  	return visibleFrom < System.currentTimeMillis();
  }*/

  public boolean isVisibleAt(long instant) {
    return visibleFrom < instant;
  }

  public String getBody() {
    return msgBody;
  }

  protected void incrementAttempts() {
    this.attempts++;
  }

}
