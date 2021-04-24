package model;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.joda.time.Instant;

public class DeadLetterError {
    private Exception exception;
    private PubsubMessage message;
    private Instant timestamp;

    public DeadLetterError(Exception exception, PubsubMessage message, Instant timestamp) {
      this.exception = exception;
      this.message = message;
      this.timestamp = timestamp;
    }

    public DeadLetterError(){

    }

  public Exception getException() {
    return exception;
  }

  public void setException(Exception exception){
    this.exception = exception;
  }


  public PubsubMessage getMessage() {
    return message;
  }

  public void setMessage(PubsubMessage message){
     this.message = message;
  }


  public Instant getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Instant timestamp){
    this.timestamp = timestamp;
  }

}
