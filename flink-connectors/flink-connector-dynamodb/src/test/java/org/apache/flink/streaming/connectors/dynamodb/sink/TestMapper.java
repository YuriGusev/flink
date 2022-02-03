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

import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.HashMap;
import java.util.Map;

/** A test implementation of a mapper to be used in the DynamoDb sink tests. */
public class TestMapper extends RichMapFunction<String, Map<String, Object>> {

    private final String partitionKey;
    private final String sortKey;

    public TestMapper(String partitionKey, String sortKey) {
        this.partitionKey = partitionKey;
        this.sortKey = sortKey;
    }

    @Override
    public Map<String, Object> map(String data) throws Exception {
        final Map<String, Object> item = new HashMap<>();
        item.put(partitionKey, data);
        item.put(sortKey, data);
        return item;
    }
}
