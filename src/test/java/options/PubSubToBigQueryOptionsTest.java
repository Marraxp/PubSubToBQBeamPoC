package options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class PubSubToBigQueryOptionsTest {


  @Test
  public void successfullyJobNameSet(){
    String testName = "MyTestJobName";

    PubSubToBigQueryOptions options = PipelineOptionsFactory.as(PubSubToBigQueryOptions.class);
    options.setJobName(testName);

    assertEquals(testName, options.getJobName());
  }


  @Test
  public void successfullyInputProjectSet(){
    String inputProject = "MyInputProject";

    PubSubToBigQueryOptions options = PipelineOptionsFactory.as(PubSubToBigQueryOptions.class);
    options.setInputProject(inputProject);

    assertEquals(inputProject, options.getInputProject());
  }

  @Test
  public void successfullyOutputProjectSet(){
    String outputProject = "MyOutputProject";

    PubSubToBigQueryOptions options = PipelineOptionsFactory.as(PubSubToBigQueryOptions.class);
    options.setOutputProject(outputProject);

    assertEquals(outputProject, options.getOutputProject());
  }

  @Test
  public void successfullyPubSubTopicSet(){
    String pubSubTopic = "myTopic";

    PubSubToBigQueryOptions options = PipelineOptionsFactory.as(PubSubToBigQueryOptions.class);
    options.setPubSubTopic(pubSubTopic);

    assertEquals(pubSubTopic, options.getPubSubTopic());
  }

  @Test
  public void successfullyWindowSizeSet(){
    Integer windowSize = 10;

    PubSubToBigQueryOptions options = PipelineOptionsFactory.as(PubSubToBigQueryOptions.class);
    options.setWindowSize(windowSize);

    assertEquals(windowSize, options.getWindowSize());
  }

  @Test
  public void successfullyBQDatasetSet(){
    String bqDataset = "MyDataset";

    PubSubToBigQueryOptions options = PipelineOptionsFactory.as(PubSubToBigQueryOptions.class);
    options.setBigQueryDataSet(bqDataset);

    assertEquals(bqDataset, options.getBigQueryDataSet());
  }

  @Test
  public void successfullyBQTableSet(){
    String bqTable = "MyTable";

    PubSubToBigQueryOptions options = PipelineOptionsFactory.as(PubSubToBigQueryOptions.class);
    options.setBigQueryTable(bqTable);

    assertEquals(bqTable, options.getBigQueryTable());
  }

  @Test
  public void successfullyPubSubSchemaSet(){
    String pubSubSchemaID = "MyPubSubSchemaID";

    PubSubToBigQueryOptions options = PipelineOptionsFactory.as(PubSubToBigQueryOptions.class);
    options.setPubSubSchema(StaticValueProvider.of(pubSubSchemaID));

    assertEquals(pubSubSchemaID, options.getPubSubSchema());
  }

}
