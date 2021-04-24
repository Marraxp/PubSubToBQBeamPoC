import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableRow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;

import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import transformations.MessageParser;

@RunWith(JUnit4.class)
public class PubSubToBigQueryTest {

  /*@Test
  public void runPipelineSuccessfully(){
    PipelineOptionsFactory.register(PubSubToBigQueryOptions.class);
    PubSubToBigQueryOptions options = PipelineOptionsFactory.as(PubSubToBigQueryOptions.class);
    options.setPubSubSchema(StaticValueProvider.of("{\"type\":\"record\",\"name\":\"Person\",\"fields\":[{\"name\":\"ID\",\"type\":\"string\"},{\"name\":\"Name\",\"type\":\"string\"},{\"name\":\"Age\",\"type\":\"int\"},{\"name\":\"Address\",\"type\":\"string\"},{\"name\":\"Weight\",\"type\":\"double\"}]}"));
    options.setBigQueryDataSet("marra-poc");
    options.setBigQueryTable("person");
    options.setWindowSize(1);
    options.setPubSubTopic("marra-poc-v2");
    options.setInputProject("turingears-dev");
    options.setOutputProject("turingears-dev");
    options.setJobName("marra-poc");
    options.setRunner(DataflowRunner.class);
    options.setTempLocation("gs://marra-poc-77d07e2e-e713-4f72-9d3f-7f89ba98531b/dataflow/temp");

    options.setProject("turingears-dev");
    options.setStagingLocation("gs://marra-poc-77d07e2e-e713-4f72-9d3f-7f89ba98531b/dataflow/stg");
    options.setStreaming(true);
    options.setRegion("us-central1");

    Pipeline pipeline = Pipeline.create(options);
    pipeline.run().waitUntilFinish(Duration.standardMinutes(1));

  }*/


  @Test
  public void testPipelineReadFromPubSub(){
    Pipeline p = TestPipeline.create();
    String projectId = "turingears-dev";
    String pubSubTopic = "marra-poc-v2";

    PCollection<String> output = p.apply("Read from PubSub Topic", PubsubIO
        .readStrings()
        .fromTopic("projects/"+projectId+"/topics/"+pubSubTopic)
    );


    PAssert.that(output).containsInAnyOrder("4");

  }

  @Test
  public void testPipelineReadFromPubSubAndParseMessageRight(){
    Pipeline p = TestPipeline.create();
    String projectId = "turingears-dev";
    String pubSubTopic = "marra-poc-v2";
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

    TableRow expectedTableRow = new TableRow();
    expectedTableRow
        .set("ID", 1)
        .set("Name", "Mariano2")
        .set("Age", 33)
        .set("Address", "Some").set("Weigth", 88.2);


    PCollectionTuple output = p.apply("Read from PubSub Topic", PubsubIO
        .readMessages()
        .fromTopic("projects/"+projectId+"/topics/"+pubSubTopic)
    )
        .apply("ParseMessage", ParDo.of(new MessageParser(StaticValueProvider.of(stringSchema)))
            .withOutputTags(MessageParser.successfulParse, TupleTagList.of(MessageParser.deadLetterTag)));

    PCollection<TableRow> actual = output.get(MessageParser.successfulParse);


    PAssert.that(actual).containsInAnyOrder(expectedTableRow);

  }




}
