package org.apache.flink.streaming.connectors.dynamodb;

import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for serializing and deserialzing a collection of {@link DynamoDbWriteRequest} with {@link
 * DynamoDbWriterStateSerializer}.
 */
public class DynamoDbWriterStateSerializerTest {

    private static final DynamoDbWriterStateSerializer SERIALIZER =
            new DynamoDbWriterStateSerializer();

    @Test
    public void testStateSerDe() throws IOException {
        final Collection<DynamoDbWriteRequest> state = new ArrayList<>();
        DynamoDbWriteRequest dynamoDbWriteRequest =
                new DynamoDbWriteRequest("Table", WriteRequest.builder().build());
        state.add(dynamoDbWriteRequest);
        byte[] serialize = SERIALIZER.serialize(state);
        assertEquals(state, SERIALIZER.deserialize(1, serialize));
    }
}
