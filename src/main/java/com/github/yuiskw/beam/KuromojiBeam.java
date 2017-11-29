/**
 * Copyright (c) 2017 Yu Ishikawa.
 */
package com.github.yuiskw.beam;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;

/**
 * This class is used for a Dataflow job which write parsed Laplace logs to BigQuery.
 */
public class KuromojiBeam {

  /**
   * command line options interface
   */
  public interface Optoins extends DataflowPipelineOptions {
    @Description("Input BigQuery dataset name")
    @Validation.Required
    String getInputDataset();

    void setInputDataset(String inputDataset);

    @Description("Input BigQuery table name")
    @Validation.Required
    String getInputTable();

    void setInputTable(String inputTable);

    @Description("Output BigQuery dataset")
    @Validation.Required
    String getOutputDataset();

    void setOutputDataset(String outputDataset);

    @Description("Output BigQuery table")
    @Validation.Required
    String getOutputTable();

    void setOutputTable(String outputTable);

    @Description("column that we want to tokenize")
    @Validation.Required
    String getTokenizedColumn();

    void setTokenizedColumn(String tokenizedColumn);

    @Description("schema which follows BigQuery spec. ex) name:string,gender:string,count:integer")
    @Validation.Required
    String getSchema();

    void setSchema(String schema);

    @Description("Output column name")
    @Default.String("tokens")
    String getOutputColumn();

    void setOutputColumn(String tokenizedColumn);
  }

  public static void main(String[] args) {
    Optoins options = getOptions(args);

    String projectId = options.getProject();
    String inputDatasetId = options.getInputDataset();
    String inputTableId = options.getInputDataset();
    String outputDatasetId = options.getOutputDataset();
    String outputTableId = options.getOutputDataset();
    String tokenizedColumn = options.getTokenizedColumn();
    String outputColumn = options.getOutputColumn();
    LinkedHashMap<String, String> schemaMap = parseSchema(options.getSchema());
    TableSchema schema = convertToTableSchema(schemaMap, outputColumn);

    // Input
    TableReference tableRef = new TableReference()
        .setDatasetId(inputDatasetId)
        .setTableId(inputTableId);
    BigQueryIO.Read reader = BigQueryIO.read().from(tableRef);

    // Output
    BigQueryIO.Write<TableRow> writer = BigQueryIO.writeTableRows().withSchema(schema);

    // Build and run pipeline
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(reader)
        //.apply(ParDo.of(new TableRow2EntityFn(projectId, namespace, parents, kind, keyColumn)))
        .apply(writer);
    pipeline.run();
  }

  /**
   * Get command line options
   */
  public static Optoins getOptions(String[] args) {
    Optoins options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Optoins.class);
    return options;
  }

  /**
   * Parse table schema specification.
   * <p>
   * e.g.) name:string,gender:string,count:integer
   */
  public static LinkedHashMap<String, String> parseSchema(String schemaString) {
    LinkedHashMap<String, String> schemaMap = new LinkedHashMap<String, String>();
    if (schemaString != null) {
      // TODO validation
      for (String path : schemaString.split(",")) {
        // trim
        String trimmed = path.replaceAll("(^\\s+|\\s+$)", "");

        // split with ":" and trim each element
        String[] elements = trimmed.split(":");
        String k = elements[0].replaceAll("(^\\s+|\\s+$)", "");
        String v = elements[1].replaceAll("(^\\s+|\\s+$)", "");
        schemaMap.put(k, v);
      }
    }
    return schemaMap;
  }

  /**
   * Convert a schema definition to TableSchema
   */
  public static TableSchema convertToTableSchema(
      LinkedHashMap<String, String> schemaMap, String outputTokenizedColumn) {
    if (schemaMap == null) {
      // TODO error handling
    }

    List<TableFieldSchema> fields = new ArrayList<>();
    for (String column : schemaMap.keySet()) {
      String datatype = schemaMap.get(column).toLowerCase();
      if (datatype == "INTEGER") {
        fields.add(new TableFieldSchema().setName(column).setType("INTEGER"));
      } else if (datatype == "string") {
        fields.add(new TableFieldSchema().setName(column).setType("STRING"));
      } else if (datatype == "bytes") {
        fields.add(new TableFieldSchema().setName(column).setType("BYTES"));
      } else if (datatype == "float") {
        fields.add(new TableFieldSchema().setName(column).setType("FLOAT"));
      } else if (datatype == "boolean") {
        fields.add(new TableFieldSchema().setName(column).setType("BOOLEAN"));
      } else if (datatype == "timestamp") {
        fields.add(new TableFieldSchema().setName(column).setType("TIMESTAMP"));
      } else {
        // TODO error handling
      }
    }

    // Add a field for the output tokenized column.
    TableFieldSchema outputField = new TableFieldSchema()
        .setName(outputTokenizedColumn)
        .setType("RECORD")
        .setFields(
            new ArrayList<TableFieldSchema>() {
              {
                add(new TableFieldSchema().setName("token").setType("STRING"));
              }
            }
        );
    fields.add(outputField);

    // Make a table schema
    TableSchema schema = new TableSchema().setFields(fields);
    return schema;
  }
}
