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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.dynamodb.DynamoDbProducer;
import org.apache.flink.streaming.connectors.dynamodb.DynamoDbSink;
import org.apache.flink.streaming.connectors.dynamodb.DynamoDbSinkFunction;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.net.URI;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/** End to End Tests for DynamoDb connector. */
public class DynamoDbConnectorEndToEndTest {

    @ClassRule
    public static GenericContainer dynamoDBLocal =
            new GenericContainer("amazon/dynamodb-local:1.16.0").withExposedPorts(8000);

    @Test
    public void test() throws Exception {
        DynamoDbClient dynamoDbClient =
                DynamoDbClient.builder()
                        .endpointOverride(
                                URI.create(
                                        "http://localhost:" + dynamoDBLocal.getFirstMappedPort()))
                        .region(Region.US_EAST_1)
                        .credentialsProvider(() -> AwsBasicCredentials.create("x", "y"))
                        .build();
        dynamoDbClient.createTable(
                CreateTableRequest.builder()
                        .tableName("test_table")
                        .attributeDefinitions(
                                AttributeDefinition.builder()
                                        .attributeName("number_id")
                                        .attributeType(ScalarAttributeType.S)
                                        .build())
                        .keySchema(
                                KeySchemaElement.builder()
                                        .attributeName("number_id")
                                        .keyType(KeyType.HASH)
                                        .build())
                        .provisionedThroughput(
                                ProvisionedThroughput.builder()
                                        .readCapacityUnits(10L)
                                        .writeCapacityUnits(10L)
                                        .build())
                        .build());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Properties properties = new Properties();
        properties.put("aws.region", "us-east-1");
        properties.put("aws.endpoint", "http://localhost:" + dynamoDBLocal.getFirstMappedPort());
        properties.put("aws.credentials.provider.basic.accesskeyid", "x");
        properties.put("aws.credentials.provider.basic.secretkey", "y");
        DynamoDbSink<Integer> dynamoDbSink =
                new DynamoDbSink<>(new DynamoDBTestSinkFunction(), properties);
        dynamoDbSink.setFailOnError(true);
        dynamoDbSink.setBatchSize(1);

        env.fromCollection(ImmutableList.of(1, 2, 3)).addSink(dynamoDbSink);
        env.execute("DynamoDBTest");
        GetItemResponse id =
                dynamoDbClient.getItem(
                        GetItemRequest.builder()
                                .tableName("test_table")
                                .key(
                                        ImmutableMap.of(
                                                "number_id",
                                                AttributeValue.builder().s("1").build()))
                                .build());
        assertEquals("1", id.item().get("number_id").s());
    }

    private static final class DynamoDBTestSinkFunction implements DynamoDbSinkFunction<Integer> {
        private static final long serialVersionUID = -6878686637563351934L;

        @Override
        public void process(
                Integer value, RuntimeContext context, DynamoDbProducer dynamoDbProducer) {
            dynamoDbProducer.produce(
                    PutItemRequest.builder()
                            .tableName("test_table")
                            .item(
                                    ImmutableMap.of(
                                            "number_id",
                                            AttributeValue.builder()
                                                    .s(Integer.toString(value))
                                                    .build(),
                                            "column",
                                            AttributeValue.builder().s("value").build()))
                            .build());
        }
    }
}
