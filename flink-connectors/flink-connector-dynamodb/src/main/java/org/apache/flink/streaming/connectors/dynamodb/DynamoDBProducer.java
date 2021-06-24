package org.apache.flink.streaming.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import java.util.List;
import java.util.Map;

/** XX. */
public class DynamoDBProducer {

    /** A listener for the execution. */
    public interface Listener {

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

    private Listener listener;

    public DynamoDBProducer(Listener listener) {
        this.listener = listener;
    }

    public int getOutstandingRecordsCount() {
        return 0;
    }

    public void flush() {}

    public void close() {}

    public void add(Map<String, List<WriteRequest>> writeRequests) {}
}
