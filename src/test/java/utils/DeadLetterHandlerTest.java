package utils;

import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableMap;
import exceptions.DeadLetterException;
import java.text.SimpleDateFormat;
import model.DeadLetterError;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.joda.time.Instant;
import org.junit.Test;

public class DeadLetterHandlerTest {

  private Instant baseTime = Instant.parse("1987-09-10T20:45:00.000Z");

  private Exception createExceptionToTest(){
    Exception exception = new DeadLetterException("Test Exception");
    return exception;
  }

  private PubsubMessage createPubSubMessageToTest(){
    String payload1 = "{\"ID\":\"1\", \"Name\": \"Mariano\", \"Age\":33, \"Address\":\"Some\", \"Weight\":88.2}";
    PubsubMessage message = new PubsubMessage(payload1.getBytes(), ImmutableMap
        .of("id", "123", "type", "custom_event"));
    return message;
  }

  @Test
  public void successfullyToBqTableRow(){
    Exception exception = this.createExceptionToTest();
    PubsubMessage message = this.createPubSubMessageToTest();
    SimpleDateFormat simpleDateFormat =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
    String date = simpleDateFormat.format(baseTime.toDate());

    DeadLetterError error = new DeadLetterError(exception, message, baseTime);

    TableRow row = DeadLetterHandler.toBqTableRow(error);

    assertEquals(exception.getClass(), row.get("error_class"));
    assertEquals(exception.getMessage(), row.get("error_msg"));
    assertEquals( message.getPayload(), row.get("msg"));
    assertEquals(date, row.get("time_received"));
  }

  @Test
  public void successfullyGetTableSchema(){

    String fieldName0 = "error_class";
    String fieldName1 = "error_msg";
    String fieldName2 = "msg";
    String fieldName3 = "time_received";
    String type = "STRING";

    TableSchema actualSchema = DeadLetterHandler.getTableSchema();

    assertEquals(fieldName0, actualSchema.getFields().get(0).getName());
    assertEquals(fieldName1, actualSchema.getFields().get(1).getName());
    assertEquals(fieldName2, actualSchema.getFields().get(2).getName());
    assertEquals(fieldName3, actualSchema.getFields().get(3).getName());

    assertEquals(type, actualSchema.getFields().get(0).getType());
    assertEquals(type, actualSchema.getFields().get(1).getType());
    assertEquals(type, actualSchema.getFields().get(2).getType());
    assertEquals(type, actualSchema.getFields().get(3).getType());

  }



}
