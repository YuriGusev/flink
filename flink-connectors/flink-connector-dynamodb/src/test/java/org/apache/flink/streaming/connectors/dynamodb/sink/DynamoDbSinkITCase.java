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

package org.apache.flink.streaming.connectors.dynamodb.sink;

import org.apache.flink.streaming.connectors.dynamodb.testutils.DynamoDbContainer;
import org.apache.flink.util.DockerImageVersions;

import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

/** Integration test for {@link DynamoDbSink}. */
public class DynamoDbSinkITCase {
    private static final String TEST_TABLE = "test_table";
    private static final String PARTITION_KEY = "partition_key";
    private static final String SORT_KEY = "sort_key";

    @ClassRule
    public static final DynamoDbContainer DYNAMODB =
            new DynamoDbContainer(DockerImageName.parse(DockerImageVersions.DYNAMODB))
                    .withNetwork(Network.newNetwork())
                    .withNetworkAliases("dynamodb");

    /*@BeforeClass
    public static void beforeClass() {
        dynamodb =
                new DynamoDBHelpers(
                        () ->
                                AwsBasicCredentials.create(
                                        localstack.getAccessKey(), localstack.getSecretKey()),
                        localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB),
                        localstack.getRegion());
    }

    @Before
    public void setUp() {
        dynamodb.createTable(TEST_TABLE, PARTITION_KEY, SORT_KEY);
    }

    @After
    public void tearDown() {
        dynamodb.deleteTable(TEST_TABLE);
    }*/

    /*private Properties getDynamoDBProperties() {
        Properties properties = new Properties();
        properties.put("aws.region", "us-east-1");
        properties.put("aws.endpoint", "http://localhost:" + dynamoDBLocal.getFirstMappedPort());
        properties.put("aws.credentials.provider.basic.accesskeyid", "x");
        properties.put("aws.credentials.provider.basic.secretkey", "y");
        return properties;
    }*/

    @Test
    public void test() {
        System.out.println("test");
    }
}
