package org.apache.flink.streaming.connectors.dynamodb.sink;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.streaming.connectors.dynamodb.util.AWSItemUtil;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * An implementation of the {@link ElementConverter} that uses the AWS Kinesis SDK v2. The user only
 * needs to provide a {@link ItemConverter} of the {@code InputT}.
 */
@PublicEvolving
public class DynamoDbElementConverter<InputT> implements ElementConverter<InputT, WriteRequest> {

    /** An Item Converter to specify how the input element should be converted to an Item. */
    private final ItemConverter<InputT> itemConverter;

    private DynamoDbElementConverter(ItemConverter<InputT> itemConverter) {
        this.itemConverter = itemConverter;
    }

    @Experimental
    @Override
    public WriteRequest apply(InputT element, SinkWriter.Context context) {
        return WriteRequest.builder()
                .putRequest(
                        PutRequest.builder()
                                .item(AWSItemUtil.toAttributeValueMap(itemConverter.apply(element)))
                                .build())
                .build();
    }

    public static <InputT> Builder<InputT> builder() {
        return new Builder<>();
    }

    /** A builder for the KinesisDataStreamsSinkElementConverter. */
    @PublicEvolving
    public static class Builder<InputT> {

        private ItemConverter<InputT> itemConverter;

        public Builder<InputT> setItemConverter(ItemConverter<InputT> itemConverter) {
            this.itemConverter = itemConverter;
            return this;
        }

        @Experimental
        public DynamoDbElementConverter<InputT> build() {
            Preconditions.checkNotNull(
                    itemConverter,
                    "No ItemConverter was supplied to the DynamoDbElementConverter builder.");
            return new DynamoDbElementConverter<>(itemConverter);
        }
    }
}
