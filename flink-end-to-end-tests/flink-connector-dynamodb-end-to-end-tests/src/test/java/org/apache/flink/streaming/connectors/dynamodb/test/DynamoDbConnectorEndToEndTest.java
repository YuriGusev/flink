/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.dynamodb.test;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.dynamodb.shaded.software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import org.apache.flink.dynamodb.shaded.software.amazon.awssdk.regions.Region;
import org.apache.flink.dynamodb.shaded.software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import org.apache.flink.dynamodb.shaded.software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import org.apache.flink.dynamodb.shaded.software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import org.apache.flink.dynamodb.shaded.software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import org.apache.flink.dynamodb.shaded.software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import org.apache.flink.dynamodb.shaded.software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import org.apache.flink.dynamodb.shaded.software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import org.apache.flink.dynamodb.shaded.software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import org.apache.flink.dynamodb.shaded.software.amazon.awssdk.services.dynamodb.model.KeyType;
import org.apache.flink.dynamodb.shaded.software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import org.apache.flink.dynamodb.shaded.software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import org.apache.flink.dynamodb.shaded.software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import org.apache.flink.dynamodb.shaded.software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.dynamodb.DynamoDbProducer;
import org.apache.flink.streaming.connectors.dynamodb.DynamoDbSink;
import org.apache.flink.streaming.connectors.dynamodb.DynamoDbSinkFunction;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** End to End Tests for DynamoDb connector. */
public class DynamoDbConnectorEndToEndTest {

    private static final String TEST_TABLE = "test_table";
    private static final String KEY_ATTRIBUTE = "key_attribute";

    private static DynamoDbClient dynamoDbClient;

    @ClassRule
    public static GenericContainer<?> dynamoDBLocal =
            new GenericContainer<>("amazon/dynamodb-local:1.16.0").withExposedPorts(8000);

    @BeforeClass
    public static void setUp() {
        dynamoDbClient =
                DynamoDbClient.builder()
                        .endpointOverride(
                                URI.create(
                                        "http://localhost:" + dynamoDBLocal.getFirstMappedPort()))
                        .region(Region.US_EAST_1)
                        .credentialsProvider(() -> AwsBasicCredentials.create("x", "y"))
                        .build();
        dynamoDbClient.createTable(
                CreateTableRequest.builder()
                        .tableName(TEST_TABLE)
                        .attributeDefinitions(
                                AttributeDefinition.builder()
                                        .attributeName(KEY_ATTRIBUTE)
                                        .attributeType(ScalarAttributeType.S)
                                        .build())
                        .keySchema(
                                KeySchemaElement.builder()
                                        .attributeName(KEY_ATTRIBUTE)
                                        .keyType(KeyType.HASH)
                                        .build())
                        .provisionedThroughput(
                                ProvisionedThroughput.builder()
                                        .readCapacityUnits(10L)
                                        .writeCapacityUnits(10L)
                                        .build())
                        .build());
    }

    @AfterClass
    public static void tearDown() {
        dynamoDbClient.deleteTable(DeleteTableRequest.builder().tableName(TEST_TABLE).build());
        dynamoDbClient.close();
    }

    @Test
    public void test() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(3);

        Properties properties = new Properties();
        properties.put("aws.region", "us-east-1");
        properties.put("aws.endpoint", "http://localhost:" + dynamoDBLocal.getFirstMappedPort());
        properties.put("aws.credentials.provider.basic.accesskeyid", "x");
        properties.put("aws.credentials.provider.basic.secretkey", "y");
        DynamoDbSink<String> dynamoDbSink =
                new DynamoDbSink<>(new DynamoDBTestSinkFunction(), properties);
        dynamoDbSink.setFailOnError(true);
        dynamoDbSink.setBatchSize(4);

        List<String> input =
                ImmutableList.of(
                        "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine",
                        "Ten");

        env.fromCollection(input).addSink(dynamoDbSink);
        env.execute("DynamoDB End to End Test");

        List<ImmutableMap<String, AttributeValue>> keys =
                input.stream()
                        .map(
                                k ->
                                        ImmutableMap.of(
                                                KEY_ATTRIBUTE,
                                                AttributeValue.builder().s(k).build()))
                        .collect(Collectors.toList());

        BatchGetItemResponse batchGetItemResponse =
                dynamoDbClient.batchGetItem(
                        BatchGetItemRequest.builder()
                                .requestItems(
                                        ImmutableMap.of(
                                                TEST_TABLE,
                                                KeysAndAttributes.builder().keys(keys).build()))
                                .build());
        Map<String, List<Map<String, AttributeValue>>> responses = batchGetItemResponse.responses();
        List<Map<String, AttributeValue>> insertedKeys = responses.get(TEST_TABLE);
        assertNotNull(insertedKeys);
        assertEquals(10, insertedKeys.size());

        Set<String> inserted =
                insertedKeys.stream()
                        .map(m -> m.get(KEY_ATTRIBUTE))
                        .map(AttributeValue::s)
                        .collect(Collectors.toSet());
        for (String key : input) {
            assertTrue("Missing " + key, inserted.contains(key));
        }
    }

    @Test(expected = Exception.class)
    public void testSinkThrowsExceptionOnFailure() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        Properties properties = new Properties();
        properties.put("aws.region", "us-east-1");
        properties.put("aws.endpoint", "http://unknown-host");
        DynamoDbSink<String> dynamoDbSink =
                new DynamoDbSink<>(new DynamoDBTestSinkFunction(), properties);
        dynamoDbSink.setFailOnError(true);
        dynamoDbSink.setBatchSize(1);

        env.fromCollection(ImmutableList.of("one")).addSink(dynamoDbSink);
        env.execute("DynamoDB End to End Test with Exception");
    }

    private static final class DynamoDBTestSinkFunction implements DynamoDbSinkFunction<String> {
        private static final long serialVersionUID = -6878686637563351934L;

        @Override
        public void process(
                String value, RuntimeContext context, DynamoDbProducer dynamoDbProducer) {
            dynamoDbProducer.produce(
                    PutItemRequest.builder()
                            .tableName(TEST_TABLE)
                            .item(
                                    ImmutableMap.of(
                                            KEY_ATTRIBUTE,
                                            AttributeValue.builder().s(value).build()))
                            .build());
        }
    }
}
