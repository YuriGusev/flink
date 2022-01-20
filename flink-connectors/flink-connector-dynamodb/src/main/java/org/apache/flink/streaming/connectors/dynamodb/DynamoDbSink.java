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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Flink Sink to produce data into a single or multiple DynamoDb tables.
 *
 * @param <IN> type of incoming records
 * @see DynamoDbSinkBuilder on how to construct a KafkaSink
 */
@PublicEvolving
public class DynamoDbSink<IN> extends AsyncSinkBase<IN, DynamoDbWriteRequest> {

    private DynamoDbClientProvider dynamoDbClientProvider;
    private DynamoDbTablesConfig dynamoDbTablesConfig;

    protected DynamoDbSink(
            DynamoDbClientProvider dynamoDbClientProvider,
            ElementConverter<IN, DynamoDbWriteRequest> elementConverter,
            DynamoDbTablesConfig dynamoDbTablesConfig,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.dynamoDbClientProvider = dynamoDbClientProvider;
        this.dynamoDbTablesConfig = dynamoDbTablesConfig;
    }

    /**
     * Create a {@link DynamoDbSinkBuilder} to construct a new {@link DynamoDbSink}.
     *
     * @param <IN> type of incoming records
     * @return {@link DynamoDbSinkBuilder}
     */
    public static <IN> DynamoDbSinkBuilder<IN> builder() {
        return new DynamoDbSinkBuilder<>();
    }

    @Override
    public SinkWriter<IN, Void, Collection<DynamoDbWriteRequest>> createWriter(
            InitContext context, List<Collection<DynamoDbWriteRequest>> states) throws IOException {
        return null;
    }

    @Override
    public Optional<SimpleVersionedSerializer<Collection<DynamoDbWriteRequest>>>
            getWriterStateSerializer() {
        return Optional.of(new DynamoDbWriterStateSerializer());
    }
}
