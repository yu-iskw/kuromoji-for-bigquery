/**
 * Copyright (c) 2017 Yu Ishikawa.
 */
package com.github.yuiskw.beam;

import java.text.ParseException;
import java.util.Date;
import java.util.LinkedHashMap;

import org.junit.Test;
import static org.junit.Assert.*;
import com.google.api.services.bigquery.model.TableRow;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import org.joda.time.DateTime;
import org.joda.time.Instant;

public class TokenizeFnTest {

  private String projectId = "sage-shard-740";
  private String namespace = "test_double";
  private String kind = "TestKind";
  private String keyColumn = "uuid";

  private TableRow getTestTableRow() {
    TableRow row = new TableRow();
    String hoge = "abc";
    String timestamp = Instant.now().toString();
    Instant.parse(timestamp);
    row.set("uuid", "")
        .set("user_id", 123)
        .set("long_value", 1L)
        .set("name", "abc")
        .set("bool_value", true)
        .set("z", hoge)
        .set("float_value", 1.23)
        .set("date", new Date())
        .set("datetime", new DateTime(new Date()))
        .set("ts", timestamp)
        .set("child", new TableRow().set("hoge", "fuga"));
    return row;
  }

  @Test
  public void testConvert1() {
    TableRow row = getTestTableRow();

    try {
      TokenizeFn fn = new TokenizeFn(projectId, namespace, null, kind, keyColumn);
      Entity entity = fn.convertTableRowToEntity(row);
      Key key = entity.getKey();
      assertEquals(key.getPartitionId().getProjectId(), projectId);
      assertEquals(key.getPartitionId().getNamespaceId(), namespace);
      assertEquals(key.getPath(0).getKind(), kind);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testConvert2() {
    TableRow row = getTestTableRow();

    try {
      LinkedHashMap<String, String> parents =
          KuromojiBeam.parseSchema("Parent1:p1,Parent2:p2");
      TokenizeFn fn = new TokenizeFn(projectId, namespace, parents, kind, keyColumn);
      Entity entity = fn.convertTableRowToEntity(row);
      Key key = entity.getKey();
      assertEquals(key.getPartitionId().getProjectId(), projectId);
      assertEquals(key.getPartitionId().getNamespaceId(), namespace);
      assertEquals(key.getPath(0).getKind(), "Parent1");
      assertEquals(key.getPath(1).getKind(), "Parent2");
      assertEquals(key.getPath(2).getKind(), kind);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testIsDate() {
    assertNotNull(TokenizeFn.parseDate("2017-01-01"));
    assertNotNull(TokenizeFn.parseDate("2017-1-1"));
    assertNull(TokenizeFn.parseDate("hoge"));
  }

  @Test
  public void testIsTime() {
    assertNotNull(TokenizeFn.parseTime("04:14:37.844024"));
    assertNotNull(TokenizeFn.parseTime("4:4:7.4"));
    assertNotNull(TokenizeFn.parseTime("04:14:37"));
    assertNull(TokenizeFn.parseTime("hoge"));
  }

  @Test
  public void testIsTimestamp() {
    assertNotNull(TokenizeFn.parseTimestamp("2017-09-16 04:14:37.844024 UTC"));
    assertNotNull(TokenizeFn.parseTimestamp("2017-09-16 04:14:37.844024 PST"));
    assertNotNull(TokenizeFn.parseTimestamp("2017-09-16 04:14:37.844024 JST"));
    assertNotNull(TokenizeFn.parseTimestamp("2017-9-16 4:14:37.844024 UTC"));
    assertNotNull(TokenizeFn.parseTimestamp("2017-09-16T04:14:37.844024"));
    assertNotNull(TokenizeFn.parseTimestamp("2017-09-16 04:14:37"));
    assertNull(TokenizeFn.parseTimestamp("hoge"));
  }
}