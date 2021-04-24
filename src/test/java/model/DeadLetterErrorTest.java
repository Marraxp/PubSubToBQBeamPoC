package model;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import exceptions.DeadLetterException;
import org.joda.time.Instant;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.junit.Test;

public class DeadLetterErrorTest {

  private Instant baseTime = Instant.parse("1987-09-10T20:45:00.000Z");

  private Exception createExceptionToTest(){
    Exception exception = new DeadLetterException("Test Exception");
    StackTraceElement[] trace = new StackTraceElement[] {
        new StackTraceElement("DeadLetterException",
            "Test",
            "TestFile",
            10)};
    exception.setStackTrace(trace);
    return exception;
  }

  @Test
  public void createDeadLetterErrorSuccessfullyWithFullArgsCons(){
    String payload1 = "{\"ID\":\"1\", \"Name\": \"Mariano\", \"Age\":33, \"Address\":\"Some\", \"Weight\":88.2}";
    PubsubMessage message = new PubsubMessage(payload1.getBytes(), ImmutableMap
        .of("id", "123", "type", "custom_event"));
    Exception exception = this.createExceptionToTest();
    Instant timestamp = this.baseTime;

    DeadLetterError error = new DeadLetterError(exception, message, timestamp);

    assertEquals(message, error.getMessage());
    assertEquals(exception, error.getException());
    assertEquals(timestamp, error.getTimestamp());
  }

  @Test
  public void createDeadLetterErrorSuccessfullyWithNoArgsConsAndSetter(){
    String payload1 = "{\"ID\":\"1\", \"Name\": \"Mariano\", \"Age\":33, \"Address\":\"Some\", \"Weight\":88.2}";
    PubsubMessage message = new PubsubMessage(payload1.getBytes(), ImmutableMap
        .of("id", "123", "type", "custom_event"));
    Exception exception = this.createExceptionToTest();
    Instant timestamp = this.baseTime;

    DeadLetterError error = new DeadLetterError();
    error.setException(exception);
    error.setMessage(message);
    error.setTimestamp(timestamp);

    assertEquals(message, error.getMessage());
    assertEquals(exception, error.getException());
    assertEquals(timestamp, error.getTimestamp());
  }

}
