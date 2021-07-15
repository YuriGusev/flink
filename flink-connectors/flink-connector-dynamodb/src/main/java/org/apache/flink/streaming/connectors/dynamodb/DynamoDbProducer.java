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
import org.apache.flink.streaming.connectors.dynamodb.batch.BatchRequest;
import org.apache.flink.streaming.connectors.dynamodb.batch.BatchResponse;

import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/** TODO. */
@PublicEvolving
public interface DynamoDbProducer {

    /** A listener for the execution. */
    interface Listener {
        /** Callback before the batch request is executed. */
        void beforeBatch(long executionId, BatchRequest request);

        /** Callback after a successful execution of batch write request. */
        void afterBatch(long executionId, BatchRequest request, BatchResponse response);

        /**
         * Callback after a failed execution of batch write request. Note that in case an instance
         * of <code>InterruptedException</code> is passed, which means that request processing has
         * been cancelled externally, the thread's interruption status has been restored prior to
         * calling this method.
         */
        void afterBatch(long executionId, BatchRequest request, Throwable failure);
    }

    /** Tear-down the producer. */
    void close() throws Exception;

    /** Get outstanding records in the in-memory queue. */
    long getOutstandingRecordsCount();

    /** Flush outstanding records in the producer. */
    void flush() throws Exception;

    /** Produce to DynamoDb. */
    void produce(PutItemRequest request);

    /** Produce to DynamoDb. */
    void produce(DeleteItemRequest request);

    /** Produce to DynamoDb. */
    void produce(UpdateItemRequest request);
}
