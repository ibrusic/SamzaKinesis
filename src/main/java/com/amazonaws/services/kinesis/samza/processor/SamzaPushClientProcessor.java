package com.amazonaws.services.kinesis.samza.processor;

import java.nio.charset.Charset;
import java.util.List;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.samza.KinesisSystemConsumer;

/**
 * Converts from Kinesis API to Samza API. This class is initially instantiated when
 * the consumer is set up. The Kinesis client library calls {@link #copy()} to obtain
 * a new instance every time it starts consuming a new shard. Within each shard, it
 * repeatedly calls {@link #processRecords(List, IRecordProcessorCheckpointer)} as
 * records are received.<p>
 *
 * This class takes those records, converts them into Samza
 * {@link IncomingMessageEnvelope}s, and hands them off to the
 * {@link KinesisSystemConsumer} where they are placed on a queue of messages to be
 * delivered to the application.
 */
public class SamzaPushClientProcessor extends ManagedClientProcessor {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final SystemStreamPartition stream;
    private final KinesisSystemConsumer consumer;

    public SamzaPushClientProcessor(SystemStreamPartition stream, KinesisSystemConsumer consumer) {
        this.stream = stream;
        this.consumer = consumer;
    }

    /**
     * Called every time new records are received from Kinesis.
     */
    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        for (Record record : records) {
            byte[] key = record.getPartitionKey().getBytes(UTF8);
            byte[] data = record.getData().array(); // This is safe, there's nothing else in the array

            IncomingMessageEnvelope envelope = new IncomingMessageEnvelope(
                stream, record.getSequenceNumber(), key, data);
            consumer.putMessage(envelope);
        }

        // FIXME Checkpointing here is wrong and has the potential for data loss.
        // At the moment we checkpoint messages after they've been placed in the
        // KinesisSystemConsumer's queue. Since this is an in-memory queue, it's lost
        // on crash. We need to change this to only checkpoint messages after they've
        // been successfully consumed by the job. This can be done by hooking into
        // the Samza CheckpointManager interface, and calling checkpointer.checkpoint()
        // with a sequence number.
        checkpoint(checkpointer);
    }

    /**
     * Called every time we start consuming a new shard. Returns a new instance
     * of this class, initialized appropriately.
     */
    @Override
    public ManagedClientProcessor copy() throws Exception {
        return new SamzaPushClientProcessor(stream, consumer);
    }
}
