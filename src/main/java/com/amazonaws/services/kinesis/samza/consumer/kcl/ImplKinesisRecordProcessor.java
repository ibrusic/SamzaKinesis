package com.amazonaws.services.kinesis.samza.consumer.kcl;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.samza.consumer.KinesisSystemConsumerPrev;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;

import java.nio.charset.Charset;
import java.util.List;

/**
 * Implementation of AbstractKinesisRecordProcessor.
 */
public class ImplKinesisRecordProcessor extends AbstractKinesisRecordProcessor {

    // Message decoder
    private static Charset UTF8 = Charset.forName("UTF-8");

    // Next checkpoint time
    private long nextCheckpointTimeInMillis;

    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;

    // AmazonKinesis checkpointer
    private IRecordProcessorCheckpointer checkpointer = null;

    // Samza's SystemStreamPartition
    private final SystemStreamPartition stream;

    /**
     * Constructor
     *
     * @param stream
     * @param consumer
     */
    public ImplKinesisRecordProcessor(SystemStreamPartition stream, KinesisSystemConsumerPrev consumer) {
        this.stream = stream;
        this.consumer = consumer;
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Processing Kinesis records");

        if (this.checkpointer == null) {
            this.checkpointer = checkpointer;
        }

        for (Record record : records) {
            byte[] key = record.getPartitionKey().getBytes(UTF8);
            byte[] data = record.getData().array(); // This is safe, there's
            // nothing else in the array

            IncomingMessageEnvelope envelope = new IncomingMessageEnvelope(
                    stream, Long.toString(((UserRecord) record)
                    .getSubSequenceNumber()), key, data);
            LOG.debug(String.format("Processing Key:%s  - Value:%s", new String(key), new String(data)));
            consumer.putMessage(this, envelope);
        }

        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    @Override
    public AbstractKinesisRecordProcessor copy() throws Exception {
        return new ImplKinesisRecordProcessor(this.stream, this.consumer);
    }

    /**
     * external method to allow the Samza system to checkpoint once a message
     * for a stream has been delivered to the relevant task
     */
//    public void checkpoint(String sequence) throws Exception {
//        TODO needs retry on failure? see
//        AbstractKinesisRecordProcessor.checkpoint()
//        checkpointer.checkpoint(sequence);
//    }
}
