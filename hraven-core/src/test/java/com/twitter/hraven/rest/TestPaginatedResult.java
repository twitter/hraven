package com.twitter.hraven.rest;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * Unit tests for the PaginatedResult class
 */

public class TestPaginatedResult {

  private final int INTEGER_PAGE_LIMIT = 10;

  @Test
  public void checkOnePageResults() {
    PaginatedResult<Integer> pageOfInts = new PaginatedResult<Integer>(INTEGER_PAGE_LIMIT);
    assertNotNull(pageOfInts);
    assertEquals(pageOfInts.getLimit(), INTEGER_PAGE_LIMIT);
    List<Integer> actualValues = new ArrayList<Integer>();
    populateListOfInts(actualValues, INTEGER_PAGE_LIMIT);
    pageOfInts.setValues(actualValues);
    List<Integer> expectedValues = new ArrayList<Integer>();
    populateListOfInts(expectedValues, INTEGER_PAGE_LIMIT);
    assertEquals(actualValues.size(), pageOfInts.getLimit());
    assertEquals(expectedValues.size(), pageOfInts.getLimit());
    assertNull(pageOfInts.getNextStartRow());
    assertEquals(expectedValues, pageOfInts.getValues());
  }

  @Test
  public void checkMultiplePageResults() {
    final int EXTRA_RESULTS = 1;
    final Integer NEXT_START_ROW = (INTEGER_PAGE_LIMIT + 1) * INTEGER_PAGE_LIMIT;
    PaginatedResult<Integer> pageOfInts = new PaginatedResult<Integer>(INTEGER_PAGE_LIMIT);
    assertNotNull(pageOfInts);
    assertEquals(pageOfInts.getLimit(), INTEGER_PAGE_LIMIT);
    List<Integer> actualValues = new ArrayList<Integer>();
    populateListOfInts(actualValues, INTEGER_PAGE_LIMIT + EXTRA_RESULTS);
    pageOfInts.setValues(actualValues.subList(0, INTEGER_PAGE_LIMIT));
    List<Integer> expectedValues = new ArrayList<Integer>();
    populateListOfInts(expectedValues, INTEGER_PAGE_LIMIT);
    pageOfInts.setNextStartRow(Bytes.toBytes(actualValues.get( INTEGER_PAGE_LIMIT)));
    assertEquals(actualValues.size(), pageOfInts.getLimit() + EXTRA_RESULTS);
    assertEquals(expectedValues.size(), pageOfInts.getLimit());
    assertNotNull(pageOfInts.getNextStartRow());
    assertEquals(NEXT_START_ROW.intValue(), Bytes.toInt(pageOfInts.getNextStartRow()));
    assertEquals(expectedValues, pageOfInts.getValues());
  }

  @Test
  public void checkLessThanOnePageResults() {
    final int LESS_THAN_ONE_PAGE = INTEGER_PAGE_LIMIT / 2;
    PaginatedResult<Integer> pageOfInts = new PaginatedResult<Integer>(INTEGER_PAGE_LIMIT);
    assertNotNull(pageOfInts);
    assertEquals(pageOfInts.getLimit(), INTEGER_PAGE_LIMIT);
    List<Integer> actualValues = new ArrayList<Integer>();
    populateListOfInts(actualValues, LESS_THAN_ONE_PAGE);
    pageOfInts.setValues(actualValues);
    List<Integer> expectedValues = new ArrayList<Integer>();
    populateListOfInts(expectedValues, LESS_THAN_ONE_PAGE);
    assertEquals(LESS_THAN_ONE_PAGE, pageOfInts.getValues().size());
    assertNull(pageOfInts.getNextStartRow());
    assertEquals(expectedValues, pageOfInts.getValues());

  }

  private void populateListOfInts(List<Integer> inputValues, int limit) {
    for (int i = 1; i <= limit; i++) {
      inputValues.add(i * INTEGER_PAGE_LIMIT);
    }
  }
}
