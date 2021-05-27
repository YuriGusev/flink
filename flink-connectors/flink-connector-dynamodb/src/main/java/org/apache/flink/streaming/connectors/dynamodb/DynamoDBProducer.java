package org.apache.flink.streaming.connectors.dynamodb;

/**
 * Mock for DynamoDBProducer class
 */
public class DynamoDBProducer {

    /**
     * A listener for the execution.
     */
    public interface Listener {

        /**
         * Callback before the batch request is executed.
         */
        void beforeBulk(long executionId, BatchRequest request);

        /**
         * Callback after a successful execution of batch write request.
         */
        void afterBulk(long executionId, BatchRequest request, BatchResponse response);

        /**
         * Callback after a failed execution of batch write request.
         *
         * Note that in case an instance of <code>InterruptedException</code> is passed,
         * which means that request processing has been cancelled externally, the thread's
         * interruption status has been restored prior to calling this method.
         */
        void afterBulk(long executionId, BatchRequest request, Throwable failure);
    }

    public int getOutstandingRecordsCount() {
        return 0;
    }

    public void flush() {}

    public void close() {

    }
}
