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

import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;

import java.util.List;

/**
 * The result of a {@link com.amazonaws.services.dynamodbv2.AmazonDynamoDB#batchWriteItem(BatchWriteItemRequest)}
 * operation.
 * A list of {@link Attempt}s is also provided with details about each
 * attempt made.
 *
 * @see Attempt
 */
public class BatchResponse {
    private final long batchRequestId;
    private final boolean successful;
    private final List<Attempt> attempts;

    public BatchResponse(List<Attempt> attempts, long batchRequestId, boolean successful) {
        this.attempts = attempts;
        this.successful = successful;
        this.batchRequestId = batchRequestId;
    }

    /**
     *
     * @return List of {@link Attempt}s, in the order they were made.
     */
    public List<Attempt> getAttempts() {
        return this.attempts;
    }

    /**
     *
     * @return Whether the record put was successful. If true, then the record
     *         has been confirmed by the backend.
     */
    public boolean isSuccessful() {
        return successful;
    }

    public Long getBatchRequestId() {
        return batchRequestId;
    }

}
