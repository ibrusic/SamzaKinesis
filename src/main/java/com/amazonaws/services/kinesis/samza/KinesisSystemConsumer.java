package com.amazonaws.services.kinesis.samza;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import com.amazonaws.services.kinesis.samza.consumer.ManagedConsumer;
import com.amazonaws.services.kinesis.samza.processor.ManagedClientProcessor;
import com.amazonaws.services.kinesis.samza.processor.SamzaPushClientProcessor;

/**
 * Implements the Samza {@link SystemConsumer} interface using a queue. The
 * Kinesis client library threads add messages to the queue, and the Samza
 * container thread reads messages off the queue.
 */
public class KinesisSystemConsumer extends BlockingEnvelopeMap {

    private static final Log LOG = LogFactory.getLog(KinesisSystemConsumer.class);

    private final String systemName;

    private final Config config;

    /** One processor per Samza partition (and since we try to use one partition per
     * container, this map is normally expected to have one entry). This processor
     * is copied every time a new shard is started. */
    private Map<SystemStreamPartition, ManagedClientProcessor> templateProcessors =
            new HashMap<SystemStreamPartition, ManagedClientProcessor>();

    /** One processor per Kinesis shard that we start consuming. Key is shardId. */
    private Map<String, ManagedClientProcessor> processors =
            new HashMap<String, ManagedClientProcessor>();

    /** List of message sequence numbers delivered since the last checkpoint. */
    private Map<SystemStreamPartition, Queue<Delivery>> deliveries =
            new HashMap<SystemStreamPartition, Queue<Delivery>>();

    private Map<SystemStreamPartition, Thread> threads = new HashMap<SystemStreamPartition, Thread>();

    public KinesisSystemConsumer(String systemName, Config config) {
        this.systemName = systemName;
        this.config = config;
    }

    /**
     * We assume here that each container consumes only one "partition" of the
     * input stream, where "partition" has been artificially set up in
     * {@link KinesisSystemAdmin} to be mapped 1:1 to Samza containers. Each
     * partition may actually involve consuming multiple shards, but that is
     * handled by the Kinesis client library.
     * <p>
     * If the Samza job has multiple input streams, this method is called once
     * for each input stream.
     */
    @Override
    public void register(SystemStreamPartition systemStreamPartition, String offset) {
        super.register(systemStreamPartition, offset);

        ManagedClientProcessor processor = new SamzaPushClientProcessor(systemStreamPartition, this);
        this.templateProcessors.put(systemStreamPartition, processor);
    }

    /**
     * Start up a thread for each Kinesis stream we want to consume.
     */
    @Override
    public void start() {
        for (Map.Entry<SystemStreamPartition, ManagedClientProcessor> entry : templateProcessors.entrySet()) {
            ManagedConsumer consumer = new ManagedConsumer(entry.getKey().getStream(),
                    config.get("job.name"), entry.getValue());
            Thread thread = new Thread(consumer);
            thread.start();
            threads.put(entry.getKey(), thread);
        }
    }

    @Override
    public void stop() {
        // TODO Make this more graceful. Perhaps ManagedConsumer should expose
        // Worker.shutdown() to us.
        for (Thread thread : threads.values())
            thread.interrupt();
    }

    /**
     * Called by a {@link SamzaPushClientProcessor} when a message has been
     * received from an incoming Kinesis stream. The message is put on a queue,
     * and the Samza container will pick it up from there when polling for new
     * messages.
     */
    public synchronized void putMessage(SamzaPushClientProcessor caller,
                                        IncomingMessageEnvelope envelope) {
        if (!deliveries.containsKey(envelope.getSystemStreamPartition())) {
            deliveries.put(envelope.getSystemStreamPartition(), new LinkedList<Delivery>());
        }

        Queue<Delivery> queue = deliveries.get(envelope.getSystemStreamPartition());
        queue.add(new Delivery(envelope.getOffset(), caller));

        try {
            put(envelope.getSystemStreamPartition(), envelope);
        } catch (InterruptedException e) {
            LOG.info("Interrupted while enqueueing message", e);
        }
    }

    /**
     * Translates a Samza checkpoint into Kinesis Client Library checkpoints.
     */
    public synchronized void checkpoint(Checkpoint checkpoint) throws Exception {
        for (Map.Entry<SystemStreamPartition, String> entry : checkpoint.getOffsets().entrySet()) {
            Queue<Delivery> queue = deliveries.get(entry.getKey());
            if (queue == null) continue;

            HashMap<SamzaPushClientProcessor, String> latestSeqNos =
                    new HashMap<SamzaPushClientProcessor, String>();
            Delivery delivery;

            // Go through the history of messages delivered since the last checkpoint.
            // Find the most recent sequence number for each processor. Stop when
            // either the queue is empty or the current checkpoint is reached.
            while (true) {
                delivery = queue.poll();
                if (delivery == null) break;
                latestSeqNos.put(delivery.processor, delivery.sequenceNumber);
                if (delivery.sequenceNumber.equals(entry.getValue())) break;
            }

            for (Map.Entry<SamzaPushClientProcessor, String> seqNo : latestSeqNos.entrySet()) {
                seqNo.getKey().checkpoint(seqNo.getValue());
            }
        }
    }

    /**
     * Called by a {@link SamzaPushClientProcessor} when it starts consuming
     * messages from a new Kinesis shard. (Each shard gets its own processor
     * instance.)
     * 
     * @param shardId Kinesis identifier of the shard that's being consumed.
     * @param processor IRecordProcessor instance for that shard.
     */
    public void registerProcessor(String shardId, ManagedClientProcessor processor) {
        processors.put(shardId, processor);
    }


    private static class Delivery {
        private final String sequenceNumber;
        private final SamzaPushClientProcessor processor;

        public Delivery(String sequenceNumber, SamzaPushClientProcessor processor) {
            this.sequenceNumber = sequenceNumber;
            this.processor = processor;
        }
    }
}
