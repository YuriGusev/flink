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

package org.apache.flink.streaming.connectors.dynamodb;

import org.apache.flink.streaming.connectors.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.streaming.connectors.dynamodb.sink.DynamoDbWriterStateSerializer;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Tests for serializing and deserialzing a collection of {@link DynamoDbWriteRequest} with {@link
 * DynamoDbWriterStateSerializer}.
 */
public class DynamoDbWriterStateSerializerTest {

    private static final DynamoDbWriterStateSerializer SERIALIZER =
            new DynamoDbWriterStateSerializer();

    @Test
    public void testStateSerDe() throws IOException {
        final Collection<WriteRequest> state = new ArrayList<>();
        WriteRequest dynamoDbWriteRequest = WriteRequest.builder().build();
        state.add(dynamoDbWriteRequest);
        byte[] serialize = SERIALIZER.serialize(state);
        Assertions.assertThat(state).isEqualTo(SERIALIZER.deserialize(1, serialize));
    }
}
