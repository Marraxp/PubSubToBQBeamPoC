package options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

public interface PubSubToBigQueryOptions extends PipelineOptions, DataflowPipelineOptions {



  @Description("Google Cloud Project to read from")
  @Required
  String getInputProject();

  void setInputProject(String inputProject);

  @Description("Google Cloud Project to write to")
  @Required
  String getOutputProject();

  void setOutputProject(String outputProject);


  @Description("Google Cloud Pub/Sub Topic to read from")
  @Required
  String getPubSubTopic();

  void setPubSubTopic(String topic);


  @Description("Window size in number of minutes.")
  @Required
  Integer getWindowSize();

  void setWindowSize(Integer windowSize);


  @Description("Google Cloud BigQuery Dataset to write to")
  @Required
  String getBigQueryDataSet();

  void setBigQueryDataSet(String dataSet);


  @Description("Google Cloud BigQuery Table to write to")
  @Required
  String getBigQueryTable();

  void setBigQueryTable(String dataSet);


  @Description("Google Cloud PubSub Schema")
  @Required
  ValueProvider<String> getPubSubSchema();

  void setPubSubSchema(ValueProvider<String> schema);


}
