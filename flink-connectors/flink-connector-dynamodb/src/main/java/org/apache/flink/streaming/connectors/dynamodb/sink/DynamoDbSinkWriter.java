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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;
import org.apache.flink.streaming.connectors.dynamodb.util.AWSDynamoDbUtil;
import org.apache.flink.streaming.connectors.dynamodb.util.DynamoDbExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Sink writer created by {@link DynamoDbSink} to write to DynamoDB. More details on the operation
 * of this sink writer may be found in the doc for {@link DynamoDbSink}. More details on the
 * internals of this sink writer may be found in {@link AsyncSinkWriter}.
 *
 * <p>The {@link DynamoDbAsyncClient} used here may be configured in the standard way for the AWS
 * SDK 2.x. e.g. the provision of {@code AWS_REGION}, {@code AWS_ACCESS_KEY_ID} and {@code
 * AWS_SECRET_ACCESS_KEY} through environment variables etc.
 */
@Internal
class DynamoDbSinkWriter<InputT> extends AsyncSinkWriter<InputT, WriteRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbSinkWriter.class);

    /* A counter for the total number of records that have encountered an error during put */
    private final Counter numRecordsOutErrorsCounter;

    /* The sink writer metric group */
    private final SinkWriterMetricGroup metrics;

    private final DynamoDbTablesConfig tablesConfig;
    private final DynamoDbAsyncClient client;

    private final String tableName;

    private final boolean failOnError;

    public DynamoDbSinkWriter(
            ElementConverter<InputT, WriteRequest> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            String tableName,
            boolean failOnError,
            DynamoDbTablesConfig tablesConfig,
            Properties dynamoDbClientProperties) {
        super(
                elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.tableName = tableName;
        this.failOnError = failOnError;
        this.tablesConfig = tablesConfig;
        this.metrics = context.metricGroup();
        this.numRecordsOutErrorsCounter = metrics.getNumRecordsOutErrorsCounter();
        this.client = AWSDynamoDbUtil.createClient(dynamoDbClientProperties);
    }

    @Override
    protected void submitRequestEntries(
            List<WriteRequest> requestEntries,
            Consumer<Collection<WriteRequest>> requestResultConsumer) {

        TableRequestsContainer container = new TableRequestsContainer(tablesConfig);
        for (WriteRequest writeRequest : requestEntries) {
            container.put(tableName, writeRequest);
        }

        CompletableFuture<BatchWriteItemResponse> future =
                client.batchWriteItem(
                        BatchWriteItemRequest.builder()
                                .requestItems(container.getRequestItems())
                                .build());

        future.whenComplete(
                (response, err) -> {
                    if (err != null) {
                        handleFullyFailedRequest(err, requestEntries, requestResultConsumer);
                    } else if (response.unprocessedItems() != null
                            && !response.unprocessedItems().isEmpty()) {
                        handlePartiallyUnprocessedRequest(response, requestResultConsumer);
                    } else {
                        requestResultConsumer.accept(Collections.emptyList());
                    }
                });
    }

    private void handlePartiallyUnprocessedRequest(
            BatchWriteItemResponse response, Consumer<Collection<WriteRequest>> requestResult) {
        List<WriteRequest> unprocessed = response.unprocessedItems().get(tableName);

        LOG.warn("DynamoDB Sink failed to persist {} entries", unprocessed.size());
        numRecordsOutErrorsCounter.inc(unprocessed.size());

        requestResult.accept(unprocessed);
    }

    private void handleFullyFailedRequest(
            Throwable err,
            List<WriteRequest> requestEntries,
            Consumer<Collection<WriteRequest>> requestResult) {
        LOG.warn("DynamoDB Sink failed to persist {} entries", requestEntries.size(), err);
        numRecordsOutErrorsCounter.inc(requestEntries.size());

        if (DynamoDbExceptionUtils.isNotRetryableException(err.getCause())) {
            getFatalExceptionCons()
                    .accept(
                            new DynamoDbSinkException(
                                    "Encountered non-recoverable exception", err));
        } else if (failOnError) {
            getFatalExceptionCons()
                    .accept(new DynamoDbSinkException.DynamoDbSinkFailFastException(err));
        } else {
            requestResult.accept(requestEntries);
        }
    }

    @Override
    protected long getSizeInBytes(WriteRequest requestEntry) {
        // dynamodb calculates item size as a sum of all attributes and all values, but doing so on
        // every operation may be too expensive, so this is just an estimate
        return requestEntry.toString().getBytes(StandardCharsets.UTF_8).length;
    }
}
