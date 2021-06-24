package org.apache.flink.streaming.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import java.util.List;
import java.util.Map;

/** XX. */
public class DynamoDBBatchWriter implements DynamoDBWriter {

    private final DynamoDBProducer dynamoDBProducer;

    public DynamoDBBatchWriter(DynamoDBProducer dynamoDBProducer) {
        this.dynamoDBProducer = dynamoDBProducer;
    }

    @Override
    public void add(Map<String, List<WriteRequest>> writeRequests) {
        dynamoDBProducer.add(writeRequests);
    }
}
