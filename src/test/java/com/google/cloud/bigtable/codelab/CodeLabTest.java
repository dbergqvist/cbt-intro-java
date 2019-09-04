/**
 * Copyright 2018 Google LLC. All Rights Reserved.
 *
 * <p>Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.codelab;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

public class CodeLabTest {

  private static final String COLUMN_FAMILY_NAME_STRING = "cf";
  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(COLUMN_FAMILY_NAME_STRING);
  private static final String LAT_COLUMN_NAME_STRING = "VehicleLocation.Latitude";
  private static final byte[] LAT_COLUMN_NAME = Bytes.toBytes(LAT_COLUMN_NAME_STRING);
  private static final String LONG_COLUMN_NAME_STRING = "VehicleLocation.Longitude";
  private static final byte[] LONG_COLUMN_NAME = Bytes.toBytes(LONG_COLUMN_NAME_STRING);
  private static final String[] MANHATTAN_BUS_LINES =
      ("M1,M2,M3,M4,M5,M7,M8,M9,M10,M11,M12,M15,M20,M21,M22,M31,M35,M42,M50,M55,M57,M66,M72,M96,"
          + "M98,M100,M101,M102,M103,M104,M106,M116,M14A,M34A-SBS,M14D,M15-SBS,M23-SBS,"
          + "M34-SBS,M60-SBS,M79-SBS,M86-SBS")
          .split(",");
  private static final String PROJECT_ID = "";
  private static final String INSTANCE_ID = "";
  private static final String TABLE_ID = "";
  private static Connection connection;
  private static BigtableDataClient dataClient;

  @Before
  public void setUp() throws IOException {
    dataClient = BigtableDataClient.create(PROJECT_ID, INSTANCE_ID);
    connection = BigtableConfiguration.connect(PROJECT_ID, INSTANCE_ID);
  }

  @Test
  public void lookupVehicleInGivenHour() throws IOException {
    String rowKey = "MTA/M86-SBS/1496275200000/NYCT_5824";
    Table table = connection.getTable(TableName.valueOf(TABLE_ID));
    Result getResult =
        table.get(
            new Get(Bytes.toBytes(rowKey))
                .setMaxVersions(Integer.MAX_VALUE)
                .addColumn(COLUMN_FAMILY_NAME, LAT_COLUMN_NAME)
                .addColumn(COLUMN_FAMILY_NAME, LONG_COLUMN_NAME));
    System.out.println(
        "Lookup a specific vehicle on the M86 route on June 1, 2017 from 12:00am to 1:00am:");
    StringBuffer builder = printLatLongPairs(getResult);

    Filters.ChainFilter filter = FILTERS.chain().filter(
        FILTERS.interleave()
            .filter(FILTERS.qualifier().exactMatch(LAT_COLUMN_NAME_STRING))
            .filter(FILTERS.qualifier().exactMatch(LONG_COLUMN_NAME_STRING)));

    List<RowCell> cells = dataClient.readRow(TABLE_ID, rowKey, filter).getCells();
    StringBuffer builder2 = printLatLongPairs(cells);
    Assert.assertEquals(builder.toString(), builder2.toString());
  }

  @Test
  public void filterBusesGoingEast() throws IOException {
    Table table = connection.getTable(TableName.valueOf(TABLE_ID));
    Scan scan = new Scan();
    SingleColumnValueFilter valueFilter =
        new SingleColumnValueFilter(
            COLUMN_FAMILY_NAME,
            Bytes.toBytes("DestinationName"),
            CompareOp.EQUAL,
            Bytes.toBytes("Select Bus Service Yorkville East End AV"));
    scan.setMaxVersions(1)
        .addColumn(COLUMN_FAMILY_NAME, LAT_COLUMN_NAME)
        .addColumn(COLUMN_FAMILY_NAME, LONG_COLUMN_NAME)
        .withStartRow(Bytes.toBytes("MTA/M86-SBS/"))
        .setRowPrefixFilter(Bytes.toBytes("MTA/M86-SBS/"))
        .setFilter(valueFilter);

    System.out.println("Scan for all m86 heading East during the month:");
    ResultScanner scanner = table.getScanner(scan);
    StringBuilder buffer = new StringBuilder();
    for (Result row : scanner) {
      buffer.append(printLatLongPairs(row));
    }

    Filters.ChainFilter chain = FILTERS
        .chain()
        .filter(FILTERS.limit().cellsPerColumn(1))
        .filter(FILTERS.condition(
            FILTERS.chain()
                .filter(FILTERS.limit().cellsPerColumn(1))
                .filter(FILTERS.qualifier().exactMatch("DestinationName"))
                .filter(FILTERS.value().exactMatch("Select Bus Service Yorkville East End AV")))
            .then(FILTERS
                .interleave()
                .filter(FILTERS.qualifier().exactMatch(LAT_COLUMN_NAME_STRING))
                .filter(FILTERS.qualifier().exactMatch(LONG_COLUMN_NAME_STRING))));

    Query query = Query.create(TABLE_ID)
        .prefix("MTA/M86-SBS/")
        .filter(chain);
    ServerStream<Row> rows = dataClient.readRows(query);

    StringBuilder buffer2 = new StringBuilder();
    for (Row r : rows) {
      buffer2.append(printLatLongPairs(r.getCells()));
    }

    Assert.assertEquals(buffer.toString(), buffer2.toString());
  }

  @Test
  public void filterBusesGoingWest() throws IOException {
    Table table = connection.getTable(TableName.valueOf(TABLE_ID));
    SingleColumnValueFilter valueFilter =
        new SingleColumnValueFilter(
            COLUMN_FAMILY_NAME,
            Bytes.toBytes("DestinationName"),
            CompareOp.EQUAL,
            Bytes.toBytes("Select Bus Service Westside West End AV"));

    Scan scan = new Scan();
    scan.setMaxVersions(1)
        .addColumn(COLUMN_FAMILY_NAME, LAT_COLUMN_NAME)
        .addColumn(COLUMN_FAMILY_NAME, LONG_COLUMN_NAME)
        .withStartRow(Bytes.toBytes("MTA/M86-SBS/"))
        .setRowPrefixFilter(Bytes.toBytes("MTA/M86-SBS/"))
        .setFilter(valueFilter);

    System.out.println("Scan for all m86 heading West during the month:");
    ResultScanner scanner = table.getScanner(scan);
    StringBuilder buffer = new StringBuilder();
    for (Result row : scanner) {
      buffer.append(printLatLongPairs(row));
    }

    Filters.ChainFilter chain = FILTERS
        .chain()
        .filter(FILTERS.limit().cellsPerColumn(1))
        .filter(FILTERS.condition(
            FILTERS.chain()
                .filter(FILTERS.limit().cellsPerColumn(1))
                .filter(FILTERS.qualifier().exactMatch("DestinationName"))
                .filter(FILTERS.value().exactMatch("Select Bus Service Westside West End AV")))
            .then(FILTERS
                .interleave()
                .filter(FILTERS.qualifier().exactMatch(LAT_COLUMN_NAME_STRING))
                .filter(FILTERS.qualifier().exactMatch(LONG_COLUMN_NAME_STRING))));

    Query query = Query.create(TABLE_ID)
        .prefix("MTA/M86-SBS/")
        .filter(chain);
    ServerStream<Row> rows = dataClient.readRows(query);

    StringBuilder buffer2 = new StringBuilder();
    for (Row r : rows) {
      buffer2.append(printLatLongPairs(r.getCells()));
    }

    Assert.assertEquals(buffer.toString(), buffer2.toString());
  }

  @Test
  public void scanBusLineInGivenHour() throws IOException {
    Table table = connection.getTable(TableName.valueOf(TABLE_ID));
    Scan scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE)
        .addColumn(COLUMN_FAMILY_NAME, LAT_COLUMN_NAME)
        .addColumn(COLUMN_FAMILY_NAME, LONG_COLUMN_NAME)
        .withStartRow(Bytes.toBytes("MTA/M86-SBS/1496275200000"))
        .setRowPrefixFilter(Bytes.toBytes("MTA/M86-SBS/1496275200000"));
    System.out.println("Scan for all M86 buses on June 1, 2017 from 12:00am to 1:00am:");
    ResultScanner scanner = table.getScanner(scan);

    StringBuilder builder = new StringBuilder();
    for (Result row : scanner) {
      builder.append(printLatLongPairs(row));
    }

    Query query = Query.create(TABLE_ID)
        .prefix("MTA/M86-SBS/1496275200000")
        .filter(FILTERS
            .interleave()
            .filter(FILTERS.qualifier().exactMatch(LAT_COLUMN_NAME_STRING))
            .filter(FILTERS.qualifier().exactMatch(LONG_COLUMN_NAME_STRING)));

    StringBuilder builder2 = new StringBuilder();
    ServerStream<Row> rows = dataClient.readRows(query);
    for (Row r : rows) {
      builder2.append(printLatLongPairs(r.getCells()));
    }

    Assert.assertEquals(builder.toString(), builder2.toString());
  }

  @Test
  public void scanEntireBusLine() throws IOException {
    Table table = connection.getTable(TableName.valueOf(TABLE_ID));
    Scan scan = new Scan();
    scan.setMaxVersions(1)
        .addColumn(COLUMN_FAMILY_NAME, LAT_COLUMN_NAME)
        .addColumn(COLUMN_FAMILY_NAME, LONG_COLUMN_NAME)
        .withStartRow(Bytes.toBytes("MTA/M86-SBS/"))
        .setRowPrefixFilter(Bytes.toBytes("MTA/M86-SBS/"));

    System.out.println("Scan for all m86 during the month:");
    ResultScanner scanner = table.getScanner(scan);
    for (Result row : scanner) {
      printLatLongPairs(row);
    }
  }

  @Test
  public void scanManhattanBusesInGivenHour() throws IOException {
    Table table = connection.getTable(TableName.valueOf(TABLE_ID));
    List<RowRange> ranges = new ArrayList<>();

    for (String busLine : MANHATTAN_BUS_LINES) {
      ranges.add(
          new RowRange(
              Bytes.toBytes("MTA/" + busLine + "/1496275200000"), true,
              Bytes.toBytes("MTA/" + busLine + "/1496275200001"), false));
    }
    Filter filter = new MultiRowRangeFilter(ranges);

    Scan scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE)
        .addColumn(COLUMN_FAMILY_NAME, LAT_COLUMN_NAME)
        .addColumn(COLUMN_FAMILY_NAME, LONG_COLUMN_NAME)
        .withStartRow(Bytes.toBytes("MTA/M"))
        .setRowPrefixFilter(Bytes.toBytes("MTA/M"))
        .setFilter(filter);

    System.out.println("Scan for all buses on June 1, 2017 from 12:00am to 1:00am:");
    ResultScanner scanner = table.getScanner(scan);
    for (Result row : scanner) {
      printLatLongPairs(row);
    }
  }

  private static StringBuffer printLatLongPairs(Result result) {
    StringBuffer builder = new StringBuffer();
    Cell[] raw = result.rawCells();
    if (raw == null) {
      throw new IllegalArgumentException(
          "No data was returned. If you recently ran the import job, try again in a minute.");
    }
    assert (raw.length % 2 == 0);
    for (int i = 0; i < raw.length / 2; i++) {
      builder.append(Bytes.toString(raw[i].getValueArray()));
      builder.append(",");
      builder.append(Bytes.toString(raw[i + raw.length / 2].getValueArray())).append("\n");
    }
    return builder;
  }

  private static StringBuffer printLatLongPairs(List<RowCell> cells) {
    StringBuffer builder = new StringBuffer();
    if (cells == null) {
      throw new IllegalArgumentException(
          "No data was returned. If you recently ran the import job, try again in a minute.");
    }
    assert (cells.size() % 2 == 0);
    for (int i = 0; i < cells.size() / 2; i++) {
      builder.append(cells.get(i).getValue().toStringUtf8());
      builder.append(",");
      builder.append(cells.get(i + cells.size() / 2).getValue().toStringUtf8()).append("\n");
    }
    return builder;
  }
}
