/**
 * Copyright (c) 2017 Yu Ishikawa.
 */
package com.github.yuiskw.beam;

import java.util.LinkedHashMap;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import com.google.api.services.bigquery.model.TableRow;
import org.joda.time.Instant;

public class TokenizeFnTest {

  private TableRow getTestTableRow() {
    TableRow row = new TableRow();
    String hoge = "abc";
    String timestamp = Instant.now().toString();
    Instant.parse(timestamp);
    row.set("id", 12345)
        .set("title", "吾輩は猫である")
        .set("text", "吾輩わがはいは猫である。名前はまだ無い。")
        .set("bool_value", true)
        .set("float_value", 1.23)
        .set("ts", timestamp);
    return row;
  }

  @Test
  public void testConvert() {
    LinkedHashMap<String, String> shemaMap = KuromojiBeam.parseSchema("id:integer");
    String tokenizedColumn = "text";
    String outputColumn = "output_tokens";
    String kuromojiMode = "NORMAL";
    TokenizeFn fn = new TokenizeFn(shemaMap, tokenizedColumn, outputColumn, kuromojiMode);

    TableRow row = getTestTableRow();
    TableRow output = fn.convert(row);
    assertEquals(2, output.size());
    assertEquals(12345, output.get("id"));
    List<TableRow> tokens = (List<TableRow>) output.get(outputColumn);
    assertEquals(12, tokens.size());
  }
}