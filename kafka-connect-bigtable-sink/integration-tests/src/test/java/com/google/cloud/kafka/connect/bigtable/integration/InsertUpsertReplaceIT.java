/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.kafka.connect.bigtable.integration;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.kafka.connect.bigtable.config.BigtableErrorMode;
import com.google.cloud.kafka.connect.bigtable.config.BigtableSinkConfig;
import com.google.cloud.kafka.connect.bigtable.config.InsertMode;
import com.google.cloud.kafka.connect.bigtable.config.NullValueMode;
import com.google.protobuf.ByteString;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class InsertUpsertReplaceIT extends BaseKafkaConnectBigtableIT {
  private static final String KEY1 = "key1";
  private static final String KEY2 = "key2";
  private static final ByteString KEY1_BYTES =
      ByteString.copyFrom(KEY1.getBytes(StandardCharsets.UTF_8));
  private static final ByteString KEY2_BYTES =
      ByteString.copyFrom(KEY2.getBytes(StandardCharsets.UTF_8));
  private static final String VALUE1 = "value1";
  private static final String VALUE2 = "value2";
  private static final String VALUE3 = "value3";
  private static final ByteString VALUE1_BYTES =
      ByteString.copyFrom(VALUE1.getBytes(StandardCharsets.UTF_8));
  private static final ByteString VALUE2_BYTES =
      ByteString.copyFrom(VALUE2.getBytes(StandardCharsets.UTF_8));
  private static final ByteString VALUE3_BYTES =
      ByteString.copyFrom(VALUE3.getBytes(StandardCharsets.UTF_8));

  @Test
  public void testInsert() throws InterruptedException, ExecutionException {
    String dlqTopic = createDlq();
    Map<String, String> props = baseConnectorProps();
    props.put(BigtableSinkConfig.INSERT_MODE_CONFIG, InsertMode.INSERT.name());
    props.put(BigtableSinkConfig.ERROR_MODE_CONFIG, BigtableErrorMode.IGNORE.name());
    configureDlq(props, dlqTopic);
    String testId = startSingleTopicConnector(props);
    createTablesAndColumnFamilies(testId);

    connect.kafka().produce(testId, KEY1, VALUE1);
    waitUntilBigtableContainsNumberOfRows(testId, 1);
    connect.kafka().produce(testId, KEY1, VALUE2);
    connect.kafka().produce(testId, KEY2, VALUE3);
    waitUntilBigtableContainsNumberOfRows(testId, 2);
    assertSingleDlqEntry(dlqTopic, KEY1, VALUE2, null);

    Map<ByteString, Row> rows = readAllRows(bigtableData, testId);
    Row row1 = rows.get(KEY1_BYTES);
    Row row2 = rows.get(KEY2_BYTES);
    assertEquals(1, row1.getCells().size());
    assertEquals(VALUE1_BYTES, row1.getCells().get(0).getValue());
    assertEquals(1, row2.getCells().size());
    assertEquals(VALUE3_BYTES, row2.getCells().get(0).getValue());

    assertConnectorAndAllTasksAreRunning(testId);
  }

  @Test
  public void testUpsert() throws InterruptedException, ExecutionException {
    Map<String, String> props = baseConnectorProps();
    props.put(BigtableSinkConfig.INSERT_MODE_CONFIG, InsertMode.UPSERT.name());
    String testId = startSingleTopicConnector(props);
    createTablesAndColumnFamilies(testId);

    connect.kafka().produce(testId, KEY1, VALUE1);
    waitUntilBigtableContainsNumberOfRows(testId, 1);
    connect.kafka().produce(testId, KEY1, VALUE2);
    connect.kafka().produce(testId, KEY2, VALUE3);
    waitUntilBigtableContainsNumberOfRows(testId, 2);

    Map<ByteString, Row> rows = readAllRows(bigtableData, testId);
    Row row1 = rows.get(KEY1_BYTES);
    Row row2 = rows.get(KEY2_BYTES);
    assertEquals(2, row1.getCells().size());
    assertEquals(
        Set.of(VALUE1_BYTES, VALUE2_BYTES),
        row1.getCells().stream().map(RowCell::getValue).collect(Collectors.toSet()));
    assertEquals(1, row2.getCells().size());
    assertEquals(VALUE3_BYTES, row2.getCells().get(0).getValue());
    assertConnectorAndAllTasksAreRunning(testId);
  }

  // TODO: test deletes too
  @Test
  public void testReplace()
      throws InterruptedException, ExecutionException, JsonProcessingException {
    // We want to test that replace works as intended, i.e., that it removes all the previously
    // present cells, which are not in the new record. Thus, we need to write to different
    // cells. JsonConverter doesn't allow for that, so we need to use a different value converter.
    Map<String, String> valueConverterProps =
        Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://testReplace");
    Converter keyConverter = new StringConverter();
    Converter valueConverter = new AvroConverter();
    valueConverter.configure(valueConverterProps, false);

    Map<String, String> props = baseConnectorProps();
    for (Map.Entry<String, String> prop : valueConverterProps.entrySet()) {
      props.put(
          ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG + "." + prop.getKey(), prop.getValue());
    }
    props.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, valueConverter.getClass().getName());
    props.put(BigtableSinkConfig.INSERT_MODE_CONFIG, InsertMode.REPLACE_IF_NEWEST.name());
    // TODO: this comment is stupid, we should just use different schemas.
    // We need to ignore `null`s to ensure that it's `InsertMode.REPLACE_IF_NEWEST`'s behavior
    // rather than `NullValueMode.DELETE`'s or `NullValueMode.WRITE`'s.
    props.put(BigtableSinkConfig.VALUE_NULL_MODE_CONFIG, NullValueMode.IGNORE.name());

    String testId = startSingleTopicConnector(props);
    createTablesAndColumnFamilies(testId);

    String field1 = "f1";
    String field2 = "f2";
    Schema schema =
        SchemaBuilder.struct()
            .field(field1, Schema.OPTIONAL_STRING_SCHEMA)
            .field(field2, Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Struct value1 = new Struct(schema).put(field1, VALUE1);
    Struct value2 = new Struct(schema).put(field2, VALUE2);

    SchemaAndValue schemaAndValue1 = new SchemaAndValue(schema, value1);
    SchemaAndValue schemaAndValue2 = new SchemaAndValue(schema, value2);

    SchemaAndValue schemaAndKey1 = new SchemaAndValue(Schema.STRING_SCHEMA, KEY1);
    SchemaAndValue schemaAndKey2 = new SchemaAndValue(Schema.STRING_SCHEMA, KEY2);
    SchemaAndValue schemaAndKey3 = new SchemaAndValue(Schema.STRING_SCHEMA, "key3");

    long baseTimestamp = 10000;
    // Set initial values of the cells.
    sendRecords(
        testId,
        List.of(
            new AbstractMap.SimpleImmutableEntry<>(schemaAndKey1, schemaAndValue1),
            new AbstractMap.SimpleImmutableEntry<>(schemaAndKey2, schemaAndValue1)),
        keyConverter,
        valueConverter,
        baseTimestamp);
    waitUntilBigtableContainsNumberOfRows(testId, 2);
    // Successfully try to replace a record using the same timestamp.
    sendRecords(
        testId,
        List.of(new AbstractMap.SimpleImmutableEntry<>(schemaAndKey1, schemaAndValue2)),
        keyConverter,
        valueConverter,
        baseTimestamp);
    // Unsuccessfully try to replace a record using an earlier timestamp.
    sendRecords(
        testId,
        List.of(new AbstractMap.SimpleImmutableEntry<>(schemaAndKey2, schemaAndValue2)),
        keyConverter,
        valueConverter,
        baseTimestamp - 1000L);
    // TODO(prawilny): polish this comment?
    // Send anything so that we can know that all the records have been processed once there are 3
    // records in Bigtable.
    sendRecords(
        testId,
        List.of(new AbstractMap.SimpleImmutableEntry<>(schemaAndKey3, schemaAndValue1)),
        keyConverter,
        valueConverter,
        baseTimestamp);
    waitUntilBigtableContainsNumberOfRows(testId, 3);

    Map<ByteString, Row> rows = readAllRows(bigtableData, testId);
    Row row1 = rows.get(KEY1_BYTES);
    Row row2 = rows.get(KEY2_BYTES);
    assertEquals(1, row1.getCells().size());
    assertEquals(VALUE2_BYTES, row1.getCells().get(0).getValue());
    assertEquals(1, row2.getCells().size());
    assertEquals(VALUE1_BYTES, row2.getCells().get(0).getValue());
    assertConnectorAndAllTasksAreRunning(testId);
  }
}
