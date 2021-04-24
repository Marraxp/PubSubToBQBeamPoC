package utils;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import java.text.SimpleDateFormat;

import model.DeadLetterError;


public class DeadLetterHandler {

  public DeadLetterHandler(){
  }

    public static TableRow toBqTableRow(DeadLetterError error) {
      TableRow tableRow = new TableRow();
      tableRow.set("error_class", error.getException().getClass());
      tableRow.set("error_msg", error.getException().getMessage());

      tableRow.set("msg", error.getMessage().getPayload());
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
      tableRow.set("time_received", simpleDateFormat.format(error.getTimestamp().toDate()));
      return tableRow;
    }


    public static TableSchema getTableSchema() {
      TableSchema tableSchema = new TableSchema().setFields(
          ImmutableList.of(
          new TableFieldSchema().setName("error_class").setType("STRING"),
          new TableFieldSchema().setName("error_msg").setType("STRING"),
          new TableFieldSchema().setName("msg").setType("STRING"),
          new TableFieldSchema().setName("time_received").setType("STRING")));

      return tableSchema;
    }

}

