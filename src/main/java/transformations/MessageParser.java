package transformations;


import com.google.api.client.json.GenericJson;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;
import model.DeadLetterError;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import sun.net.www.content.text.Generic;
import utils.DeadLetterHandler;
import utils.TableRowUtils;

public class MessageParser extends DoFn<PubsubMessage, TableRow> {

  public static TupleTag<TableRow> successfulParse = new TupleTag<TableRow>();
  public static TupleTag<TableRow> deadLetterTag = new TupleTag<TableRow>();

  private String schema;

  public MessageParser(ValueProvider<String> schema){
    this.schema = schema.get();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws IOException {
    byte[] message = c.element().getPayload();
    System.out.println("message" + message.toString());
    System.out.println("schema" + this.schema);

    try{
      Schema schemaAvro = new Schema.Parser().parse(schema);

      DatumReader<GenericRecord> reader = new GenericDatumReader<>(schemaAvro);
      InputStream inputStream = new ByteArrayInputStream(message);
      DataInputStream dataInputStream = new DataInputStream(inputStream);
      Decoder decoder = DecoderFactory.get().jsonDecoder(schemaAvro, dataInputStream);
      GenericRecord genericRecord = reader.read(null, decoder);
      TableRow row = TableRowUtils.getTableRow(genericRecord);

      c.output(successfulParse, row);
    } catch(IOException e){
      DeadLetterError deadLetterError =
          new DeadLetterError(e, c.element(), c.timestamp());
      TableRow deadLetterTable = DeadLetterHandler.toBqTableRow(deadLetterError);
      c.output(deadLetterTag, deadLetterTable);
    } catch (AvroTypeException e){
      DeadLetterError deadLetterError =
          new DeadLetterError(e, c.element(), c.timestamp());
      TableRow deadLetterTable = DeadLetterHandler.toBqTableRow(deadLetterError);
      c.output(deadLetterTag, deadLetterTable);
    }

  }



}
