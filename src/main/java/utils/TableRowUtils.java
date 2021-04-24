package utils;

import com.google.api.client.json.GenericJson;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

public class TableRowUtils {

  public TableRowUtils(){
  }

  public static TableRow getTableRow(GenericRecord record) {
    TableRow row = new TableRow();
    encode(record, row);
    return row;
  }

  public static TableSchema getTableSchemaFromAvro(Schema schema){
    TableSchema tableSchema = new TableSchema();
    tableSchema.setFields(getFieldsSchema(schema.getFields()));

    return tableSchema;
  }

  private static TableCell getTableCell(GenericRecord record) {
    TableCell cell = new TableCell();
    encode(record, cell);
    return cell;
  }
  private static List<TableFieldSchema> getFieldsSchema(List<Field> fields) {
    return fields.stream().map(field -> {
      TableFieldSchema column = new TableFieldSchema().setName(field.name());
      Type type = field.schema().getType();
      switch (type) {
        case RECORD:
          column.setType("RECORD");
          column.setFields(getFieldsSchema(fields));
          break;
        case INT:
        case LONG:
          column.setType("INTEGER");
          break;
        case BOOLEAN:
          column.setType("BOOLEAN");
          break;
        case FLOAT:
        case DOUBLE:
          column.setType("FLOAT");
          break;
        default:
          column.setType("STRING");
      }
      return column;
    }).collect(Collectors.toList());
  }
  private static void encode(GenericRecord record, GenericJson row) {
    Schema schema = record.getSchema();
    schema.getFields().forEach(field -> {
      Type type = field.schema().getType();
      switch (type) {
        case RECORD:
          row.set(field.name(), getTableCell((GenericRecord) record.get(field.pos())));
          break;
        case INT:
          row.set(field.name(), ((Number)record.get(field.pos())).intValue());
          break;
        case LONG:
          row.set(field.name(), ((Number)record.get(field.pos())).longValue());
          break;
        case BOOLEAN:
          row.set(field.name(), record.get(field.pos()));
          break;
        case FLOAT:
          row.set(field.name(), ((Number)record.get(field.pos())).floatValue());
          break;
        case DOUBLE:
          row.set(field.name(), ((Number)record.get(field.pos())).doubleValue());
          break;
        default:
          row.set(field.name(), String.valueOf(record.get(field.pos())));
      }
    });
  }
}
