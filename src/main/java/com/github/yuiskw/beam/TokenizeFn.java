/**
 * Copyright (c) 2017 Yu Ishikawa.
 */
package com.github.yuiskw.beam;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.atilika.kuromoji.Token;
import org.atilika.kuromoji.Tokenizer;


/**
 * This class is used for converting a bigquery row to another one tokenizing target column.
 */
public class TokenizeFn extends DoFn<TableRow, TableRow> {

  /** Tokenized columna name */
  private String tokenizedColumn;
  /** Output Column for tokens */
  private String outputColumn;
  /** Kuromoji mode */
  private String kuromojiMode;
  /** Schema map */
  private LinkedHashMap<String, String> schemaMap;
  /** kuromoji tokenizer */
  private Tokenizer tokenizer = null;

  public TokenizeFn(
      LinkedHashMap<String, String> schemaMap,
      String tokenizedColumn,
      String outputColumn,
      String kuromojiMode) {
    this.schemaMap = schemaMap;
    this.tokenizedColumn = tokenizedColumn;
    this.outputColumn = outputColumn;
    this.kuromojiMode = kuromojiMode;
  }

  /**
   * Convert TableRow to Entity
   */
  @ProcessElement
  public void processElement(ProcessContext c) {
    try {
      TableRow row = c.element();
      TableRow outputRow = convert(row);
      c.output(outputRow);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public TableRow convert(TableRow row) {
    TableRow outputRow = new TableRow();

    // Select columns.
    for (String column : this.schemaMap.keySet()) {
      outputRow.put(column, row.get(column));
    }

    // Tokenize the target column.
    Tokenizer tokenizerSingleton = getOrCreateTokenizer();
    List<Token> tokens = tokenizerSingleton.tokenize(row.get(this.tokenizedColumn).toString());
    List<TableRow> surfaceFormList = new ArrayList();
    for (Token token : tokens) {
      TableRow nestedRow = new TableRow()
          .set("token", token.getSurfaceForm())
          .set("part_of_speech", token.getPartOfSpeech());
      token.getPartOfSpeech();
      surfaceFormList.add(nestedRow);
    }
    outputRow.put(this.outputColumn, surfaceFormList);

    return outputRow;
  }

  /**
   * Get or create a tokenizer.
   */
  private Tokenizer getOrCreateTokenizer() {
    if (tokenizer == null) {
      this.tokenizer = Tokenizer.builder().mode(getKuromojiMode(kuromojiMode)).build();
    }
    return tokenizer;
  }

  private static Tokenizer.Mode getKuromojiMode(String mode) {
    if (mode.toUpperCase() == "SEARCH") {
      return Tokenizer.Mode.SEARCH;
    }
    else if (mode.toUpperCase() == "EXTENDED") {
      return Tokenizer.Mode.EXTENDED;
    }
    else {
      return Tokenizer.Mode.NORMAL;
    }
  }
}
