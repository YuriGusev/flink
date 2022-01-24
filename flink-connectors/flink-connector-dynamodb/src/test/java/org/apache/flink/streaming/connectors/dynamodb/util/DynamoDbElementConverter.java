package org.apache.flink.streaming.connectors.dynamodb.util;

import org.apache.flink.api.connector.sink.SinkWriter;

import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.apache.flink.streaming.connectors.dynamodb.sink.DynamoDbWriteRequest;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.Map;

public class DynamoDbElementConverter implements ElementConverter<Map<String, AttributeValue>, DynamoDbWriteRequest> {
    private final String tableName;

    public DynamoDbElementConverter(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public DynamoDbWriteRequest apply(
            Map<String, AttributeValue> elements,
            SinkWriter.Context context) {
        return new DynamoDbWriteRequest(
                tableName,
                WriteRequest.builder()
                        .putRequest(PutRequest.builder().item(elements).build())
                        .build());
    }
}
