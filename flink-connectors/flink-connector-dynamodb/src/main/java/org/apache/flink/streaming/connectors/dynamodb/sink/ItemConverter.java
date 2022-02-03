package org.apache.flink.streaming.connectors.dynamodb.sink;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;

/**
 * This is a serializable function whose {@code accept()} method specifies how to convert from an
 * input element to the DynamoDb item, a {@code Map<String, Object>} where the key represents the
 * column name as a String, and the Object represents the value of this column.
 */
@PublicEvolving
@FunctionalInterface
public interface ItemConverter<InputT>
        extends Function<InputT, Map<String, Object>>, Serializable {}
