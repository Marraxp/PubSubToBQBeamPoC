
import options.PubSubToBigQueryOptions;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import transformations.MessageParser;
import utils.DeadLetterHandler;
import utils.TableRowUtils;


public class PubSubToBigQuery {

  public static void main(String[] args) {

    PipelineOptionsFactory.register(PubSubToBigQueryOptions.class);
    PubSubToBigQueryOptions options = PipelineOptionsFactory.fromArgs(args).as(PubSubToBigQueryOptions.class);
    run(options);

  }

  public static PipelineResult run(PubSubToBigQueryOptions options){
    Pipeline pipeline = Pipeline.create(options);

    ValueProvider<String> schema = options.getPubSubSchema();

    Schema schemaAvro = new Schema.Parser().parse(schema.get());


    String tableSpec = options.getOutputProject() + ":" +
        options.getBigQueryDataSet() + "." +
        options.getBigQueryTable();

    String deadLetterTableSpec = options.getOutputProject() + ":" +
        options.getBigQueryDataSet() + "." +
        options.getBigQueryTable() + "_deadLetter" ;


    PCollectionTuple parseMessage = pipeline
        .apply("Read PubSub Message from Topic",
            PubsubIO
            .readMessages()
            .fromTopic("projects/" + options.getInputProject() + "/topics/"
                + options.getPubSubTopic())
        )
        .apply("Parse Message", ParDo.of(new MessageParser(schema))
            .withOutputTags(MessageParser.successfulParse, TupleTagList.of(MessageParser.deadLetterTag))
        );

    // Happy path to BQ
    parseMessage.get(MessageParser.successfulParse)
        .setCoder(TableRowJsonCoder.of())
        .apply("Write Good Record to BQ",
            BigQueryIO
                .writeTableRows()
                .to(tableSpec)
                .withSchema(TableRowUtils.getTableSchemaFromAvro(schemaAvro))
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
        );

    //Create Error Record for deadLetter records & Non Happy Path to BQ

    parseMessage.get(MessageParser.deadLetterTag)
        .setCoder(TableRowJsonCoder.of())
        .apply("Write Error Record to BQ",
            BigQueryIO
                .writeTableRows()
                .to(deadLetterTableSpec)
                .withSchema(DeadLetterHandler.getTableSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
        );

    return pipeline.run();

  }



}
