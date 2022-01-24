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

package org.apache.flink.streaming.connectors.dynamodb.testutils;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.net.URI;

/** Helper methods to call dynamoDB service. */
public class DynamoDBHelpers {
    private final DynamoDbClient client;

    public DynamoDBHelpers(
            AwsCredentialsProvider credentialsProvider, URI endpoint, String region) {
        this.client =
                DynamoDbClient.builder()
                        .endpointOverride(endpoint)
                        .region(Region.of(region))
                        .credentialsProvider(credentialsProvider)
                        .build();
    }

    public void createTable(String tableName, String partitionKey, String sortKey) {
        client.createTable(
                CreateTableRequest.builder()
                        .tableName(tableName)
                        .attributeDefinitions(
                                AttributeDefinition.builder()
                                        .attributeName(partitionKey)
                                        .attributeType(ScalarAttributeType.S)
                                        .build(),
                                AttributeDefinition.builder()
                                        .attributeName(sortKey)
                                        .attributeType(ScalarAttributeType.S)
                                        .build())
                        .keySchema(
                                KeySchemaElement.builder()
                                        .attributeName(partitionKey)
                                        .keyType(KeyType.HASH)
                                        .build(),
                                KeySchemaElement.builder()
                                        .attributeName(sortKey)
                                        .keyType(KeyType.RANGE)
                                        .build())
                        .provisionedThroughput(
                                ProvisionedThroughput.builder()
                                        .readCapacityUnits(1000L)
                                        .writeCapacityUnits(1000L)
                                        .build())
                        .build());

        DynamoDbWaiter waiter = client.waiter();
        waiter.waitUntilTableExists(r -> r.tableName(tableName));
    }

    public int getItemsCount(String tableName) {
        return client.scan(ScanRequest.builder().tableName(tableName).build()).count();
    }

    public void deleteTable(String tableName) {
        client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
    }
}
