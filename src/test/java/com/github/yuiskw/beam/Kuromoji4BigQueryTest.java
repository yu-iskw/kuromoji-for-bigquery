/**
 * Copyright (c) 2017 Yu Ishikawa.
 */
package com.github.yuiskw.beam;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.testing.TestPipeline;

public class Kuromoji4BigQueryTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testGetOptions() {
    String[] args = {
        "--project=test-project-id",
        "--schema=id:integer,name:string",
        "--inputDataset=input_dataset",
        "--inputTable=input_table",
        "--outputDataset=output_dataset",
        "--outputTable=output_table",
        "--tokenizedColumn=text",
        "--outputColumn=output_tokens",
        "--kuromojiMode=SEARCH",
    };
    Kuromoji4BigQuery.Optoins options = Kuromoji4BigQuery.getOptions(args);
    assertEquals("test-project-id", options.getProject());
    assertEquals("input_dataset", options.getInputDataset());
    assertEquals("input_table", options.getInputTable());
    assertEquals("output_dataset", options.getOutputDataset());
    assertEquals("output_table", options.getOutputTable());
    assertEquals("id:integer,name:string", options.getSchema());
    assertEquals("text", options.getTokenizedColumn());
    assertEquals("output_tokens", options.getOutputColumn());
    assertEquals("SEARCH", options.getKuromojiMode());
  }

  @Test
  public void testParseSchema() {
    String schemaString = "id:integer,name:string,height:float";
    LinkedHashMap<String, String> schemaMap =
        Kuromoji4BigQuery.parseSchema(schemaString);
    assertEquals(3, schemaMap.size());
    assertEquals("integer", schemaMap.get("id"));
    assertEquals("string", schemaMap.get("name"));
    assertEquals("float", schemaMap.get("height"));

    TableSchema schema = Kuromoji4BigQuery.convertToTableSchema(schemaMap, "token");
    List<TableFieldSchema> fields = schema.getFields();
    assertEquals(4, fields.size());
    assertEquals("id", fields.get(0).getName());
    assertEquals("INTEGER", fields.get(0).getType());
    assertEquals("name", fields.get(1).getName());
    assertEquals("STRING", fields.get(1).getType());
    assertEquals("height", fields.get(2).getName());
    assertEquals("FLOAT", fields.get(2).getType());
    assertEquals("token", fields.get(3).getName());
    assertEquals("RECORD", fields.get(3).getType());
    assertEquals("surface_form", fields.get(3).getFields().get(0).getName());
    assertEquals("STRING", fields.get(3).getFields().get(0).getType());
  }

  @Test
  public void testParseSchemaWithSpaces() {
    String parentPaths = "id:integer, name: string,   height :float";
    LinkedHashMap<String, String> parents =
        Kuromoji4BigQuery.parseSchema(parentPaths);
    assertEquals(3, parents.size());
    assertEquals("integer", parents.get("id"));
    assertEquals("string", parents.get("name"));
    assertEquals("float", parents.get("height"));
  }

  @Test
  public void testGetSelectedFields1() {
    String tokenizedColumn = "text";
    LinkedHashMap<String, String> schemaMap = new LinkedHashMap<String, String>();
    schemaMap.put("x", "interger");
    schemaMap.put("y", "string");
    List<String> selectedFields = Kuromoji4BigQuery.getSelectedFields(schemaMap, tokenizedColumn);
    List<String> expected = Arrays.asList("x", "y", "text");
    assertEquals(selectedFields, expected);
  }

  @Test
  public void testGetSelectedFields2() {
    String tokenizedColumn = "text";
    LinkedHashMap<String, String> schemaMap = new LinkedHashMap<String, String>();
    schemaMap.put("x", "interger");
    schemaMap.put("text", "string");
    List<String> selectedFields = Kuromoji4BigQuery.getSelectedFields(schemaMap, tokenizedColumn);
    List<String> expected = Arrays.asList("x", "text");
    assertEquals(selectedFields, expected);
  }


  /**
   Test Query

   SELECT
     1 AS id,
     False AS bool_value,
     1.23 AS float_value,
     CURRENT_TIMESTAMP() AS timestamp_value,
     "吾輩わがはいは猫である。" AS text
   UNION ALL
   SELECT
     2 AS id,
     False AS bool_value,
     1.23 AS float_value,
     CURRENT_TIMESTAMP() AS timestamp_value,
     "名前はまだ無い。" AS text
   */
  @Ignore
  public void testMain() {
    String[] args = {
        "--project=test-project",
        "--schema=id:integer",
        "--inputDataset=test_yu",
        "--inputTable=test_kuromoji_beam_input",
        "--outputDataset=test_yu",
        "--outputTable=test_kuromoji_beam_output",
        "--tokenizedColumn=text",
        "--outputColumn=token",
        "--kuromojiMode=NORMAL",
        "--tempLocation=gs://test_yu/test-log/",
        "--gcpTempLocation=gs://test_yu/test-log/"
    };
    Kuromoji4BigQuery.main(args);
  }
}