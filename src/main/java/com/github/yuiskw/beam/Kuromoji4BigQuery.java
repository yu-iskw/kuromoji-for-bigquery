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
import org.apache.beam.sdk.transforms.ParDo;

/**
 * This class is used for a Dataflow job which write parsed Laplace logs to BigQuery.
 */
public class Kuromoji4BigQuery {

  public static String COLUMN_SURFACE_FORM = "surface_form";
  public static String COLUMN_PART_OF_SPEECH = "part_of_speech";
  public static String COLUMN_BASE_FORM = "base_form";

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

    @Description("Output BigQuery table create disposition (CREATE_NEVER or CREATE_IF_NEEDED)")
    @Default.String("CREATE_IF_NEEDED")
    String getCreateDisposition();
    void setCreateDisposition(String createDisposition);

    @Description("Output BigQuery table write disposition" +
        " (WRITE_TRUNCATE, WRITE_APPEND or WRITE_EMPTY)")
    @Default.String("WRITE_TRUNCATE")
    String getWriteDisposition();
    void setWriteDisposition(String writeDisposition);

    @Description("column that we want to tokenize")
    @Validation.Required
    String getTokenizedColumn();
    void setTokenizedColumn(String tokenizedColumn);

    @Description("schema which follows BigQuery spec. ex) name:string,gender:string,count:integer")
    @Validation.Required
    String getSchema();
    void setSchema(String schema);

    @Description("Output column name")
    @Default.String("token")
    String getOutputColumn();
    void setOutputColumn(String outputColumn);

    @Description("Kuromoji mode (any of NORMAL, SEARCH and EXTENDED)")
    @Default.String("NORMAL")
    String getKuromojiMode();
    void setKuromojiMode(String kuromojiMode);

    // TODO support dictionary
  }

  public static void main(String[] args) {
    // Get commandline options
    Optoins options = getOptions(args);
    String projectId = options.getProject();
    String inputDatasetId = options.getInputDataset();
    String inputTableId = options.getInputTable();
    String outputDatasetId = options.getOutputDataset();
    String outputTableId = options.getOutputTable();
    String createDisposition = options.getCreateDisposition().toUpperCase();
    String writeDisposition = options.getWriteDisposition().toUpperCase();
    String tokenizedColumn = options.getTokenizedColumn();
    String outputColumn = options.getOutputColumn();
    String kuromojiMode = options.getKuromojiMode().toUpperCase();

    // Make a output schema
    LinkedHashMap<String, String> schemaMap = parseSchema(options.getSchema());
    TableSchema schema = convertToTableSchema(schemaMap, outputColumn);

    // Input
    TableReference inputTableRef = new TableReference()
        .setProjectId(projectId)
        .setDatasetId(inputDatasetId)
        .setTableId(inputTableId);
    BigQueryIO.TypedRead<TableRow> reader = BigQueryIO.readTableRows()
        .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)  // to use the BigQuery Storage API
        .withSelectedFields(getSelectedFields(schemaMap, tokenizedColumn))
        .from(inputTableRef);

    // Output
    TableReference outputTableRef = new TableReference()
        .setProjectId(projectId)
        .setDatasetId(outputDatasetId)
        .setTableId(outputTableId);
    BigQueryIO.Write<TableRow> writer = BigQueryIO.writeTableRows()
        .withSchema(schema)
        .to(outputTableRef)
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.valueOf(createDisposition))
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.valueOf(writeDisposition));

    // Build and run pipeline
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(reader)
        .apply(ParDo.of(new TokenizeFn(schemaMap, tokenizedColumn, outputColumn, kuromojiMode)))
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
      throw new IllegalArgumentException("schemaMap is null.");
    }

    List<TableFieldSchema> fields = new ArrayList<>();
    for (String column : schemaMap.keySet()) {
      String datatype = schemaMap.get(column).toLowerCase();
      if (datatype.equals("integer")) {
        fields.add(new TableFieldSchema().setName(column).setType("INTEGER"));
      } else if (datatype.equals("string")) {
        fields.add(new TableFieldSchema().setName(column).setType("STRING"));
      } else if (datatype.equals("bytes")) {
        fields.add(new TableFieldSchema().setName(column).setType("BYTES"));
      } else if (datatype.equals("float")) {
        fields.add(new TableFieldSchema().setName(column).setType("FLOAT"));
      } else if (datatype.equals("boolean")) {
        fields.add(new TableFieldSchema().setName(column).setType("BOOLEAN"));
      } else if (datatype.equals("timestamp")) {
        fields.add(new TableFieldSchema().setName(column).setType("TIMESTAMP"));
      } else {
        throw new IllegalArgumentException(datatype + "is invalid or is not supported.");
      }
    }

    // Add a field for the output tokenized column.
    TableFieldSchema outputField = new TableFieldSchema()
        .setName(outputTokenizedColumn)
        .setType("RECORD")
        .setMode("REPEATED")
        .setFields(
            new ArrayList<TableFieldSchema>() {
              {
                add(new TableFieldSchema().setName(COLUMN_SURFACE_FORM).setType("STRING"));
                add(new TableFieldSchema().setName(COLUMN_PART_OF_SPEECH).setType("STRING"));
                add(new TableFieldSchema().setName(COLUMN_BASE_FORM).setType("STRING"));
              }
            }
        );
    fields.add(outputField);

    // Make a table schema
    TableSchema schema = new TableSchema().setFields(fields);
    return schema;
  }

  /**
   * Get selected fields
   *
   * @param schemaMap a map of BigQuery table schema
   * @param tokenizedColumn column to be tokenized
   * @return A list of selected fields
   */
  public static List<String> getSelectedFields(
      LinkedHashMap<String, String> schemaMap,
      String tokenizedColumn) {
    ArrayList<String> columns = new ArrayList<String>();

    // Extract columns names from the schema.
    String[] columnsInSchema = schemaMap.keySet().toArray(new String[schemaMap.keySet().size()]);
    for (String c: columnsInSchema) {
      columns.add(c);
    }

    // Append the tokenized column, if it doesn't exist in the schema
    if (! columns.contains(tokenizedColumn)) {
      columns.add(tokenizedColumn);
    }
    return columns;
  }
}
