package org.apache.flink.streaming.connectors.dynamodb.util;

import org.apache.flink.api.common.functions.RichMapFunction;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;

public class TestMapper extends RichMapFunction<String, Map<String, AttributeValue>> {

    private final String partitionKey;
    private final String sortKey;

    public TestMapper(String partitionKey, String sortKey) {
        this.partitionKey = partitionKey;
        this.sortKey = sortKey;
    }


    @Override
    public Map<String, AttributeValue> map(String data) throws Exception {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put(partitionKey, AttributeValue.builder().s(data).build());

        item.put(sortKey, AttributeValue.builder().s(data).build());
        return item;
    }
}
