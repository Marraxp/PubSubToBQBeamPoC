package utils;


import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.Test;

public class TableRowUtilsTest {

  private Schema generateAvroSchema(){
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

    Schema schema = new Schema.Parser().parse(stringSchema);
    return schema;
  }
  private GenericRecord createGoodGenericRecord() throws IOException {
    String json = "{\"ID\":\"3\", \"Name\": \"Mariano\", \"Age\":33, \"Address\":\"Some\", \"Weight\":88.2}";
    Schema schema = this.generateAvroSchema();

    DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);

    GenericRecord genericRecord = reader.read(null, decoder);

    return genericRecord;
  }

  @Test
  public void successfullyGetTableRow() throws IOException {
    String ID = "3";
    String Name = "Mariano";
    Integer Age = 33;
    String Address = "Some";
    Double Weight = 88.2;

    GenericRecord record = this.createGoodGenericRecord();

    TableRow row = TableRowUtils.getTableRow(record);

    assertEquals(ID, row.get("ID"));
    assertEquals(Name, row.get("Name"));
    assertEquals(Age, row.get("Age"));
    assertEquals(Address, row.get("Address"));
    assertEquals(Weight, row.get("Weight"));

  }

  @Test
  public void successfullyGetTableSchemaFromAvro() {
    Schema avroSchema = this.generateAvroSchema();
    String fieldName0 = "ID";
    String fieldName1 = "Name";
    String fieldName2 = "Age";
    String fieldName3 = "Address";
    String fieldName4 = "Weight";

    String type0 = "STRING";
    String type1 = "STRING";
    String type2 = "INTEGER";
    String type3 = "STRING";
    String type4 = "FLOAT";

    TableSchema actualSchema = TableRowUtils.getTableSchemaFromAvro(avroSchema);

    assertEquals(fieldName0, actualSchema.getFields().get(0).getName());
    assertEquals(fieldName1, actualSchema.getFields().get(1).getName());
    assertEquals(fieldName2, actualSchema.getFields().get(2).getName());
    assertEquals(fieldName3, actualSchema.getFields().get(3).getName());
    assertEquals(fieldName4, actualSchema.getFields().get(4).getName());

    assertEquals(type0, actualSchema.getFields().get(0).getType());
    assertEquals(type1, actualSchema.getFields().get(1).getType());
    assertEquals(type2, actualSchema.getFields().get(2).getType());
    assertEquals(type3, actualSchema.getFields().get(3).getType());
    assertEquals(type4, actualSchema.getFields().get(4).getType());




  }

}
