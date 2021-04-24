package transformations;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MessageParserTest {

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.create();
  private Instant baseTime = Instant.parse("1987-09-10T20:45:00.000Z");


  @Test
  public void successfullyGenerateAParsedMessageWithSchema(){
    String payload1 = "{\"ID\":\"1\", \"Name\": \"Mariano\", \"Age\":33, \"Address\":\"Some\", \"Weight\":88.2}";
    String payload2 = "{\"ID\":\"2\", \"Name\": \"Mariano\", \"Age\":33, \"Address\":\"Some\", \"Weight\":88.2}";
    String payload3 = "{\"ID\":\"3\", \"Name\": \"Mariano\", \"Age\":33, \"Address\":\"Some\", \"Weight\":88.2}";
    String payload4 = "{\"ID\":\"4\", \"Name\": \"Mariano\", \"Age\":33, \"Address\":\"Some\", \"Weight\":88.2}";
    PubsubMessage message1 = new PubsubMessage(payload1.getBytes(), ImmutableMap.of("id", "123", "type", "custom_event"));
    PubsubMessage message2 = new PubsubMessage(payload2.getBytes(), ImmutableMap.of("id", "124", "type", "custom_event"));
    PubsubMessage message3 = new PubsubMessage(payload3.getBytes(), ImmutableMap.of("id", "125", "type", "custom_event"));
    PubsubMessage message4 = new PubsubMessage(payload4.getBytes(), ImmutableMap.of("id", "126", "type", "custom_event"));
    TableRow expectedTableRow = new TableRow();
    expectedTableRow
        .set("ID", 1)
        .set("Name", "Mariano")
        .set("Age", 33)
        .set("Address", "Some").set("Weigth", 88.2);
    String stringSchema = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Person\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"ID\",\n"
        + "      \"type\": \"string\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"Name\",\n"
        + "      \"type\": \"string\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"Age\",\n"
        + "      \"type\": \"int\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"Address\",\n"
        + "      \"type\": \"string\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"Weight\",\n"
        + "      \"type\": \"double\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    ValueProvider<String> schema = StaticValueProvider.of(stringSchema);
    Schema schemaAvro = new Schema.Parser().parse(stringSchema);

    PCollection<PubsubMessage> input = testPipeline
        .apply(Create.of(message1, message2, message3, message4));

    PCollectionTuple parser = input
        .apply(ParDo.of(new MessageParser(schema))
        .withOutputTags(MessageParser.successfulParse, TupleTagList.of(MessageParser.deadLetterTag))
    );

    PCollection<TableRow> output = parser
        .get(MessageParser.successfulParse)
        .setCoder(TableRowJsonCoder.of());


    PAssert.that(output).containsInAnyOrder(expectedTableRow);

    testPipeline.run();

  }

}
