package com.amazonaws.services.kinesis.samza;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import com.amazonaws.services.kinesis.samza.consumer.ManagedConsumer;
import com.amazonaws.services.kinesis.samza.processor.ManagedClientProcessor;
import com.amazonaws.services.kinesis.samza.processor.SamzaPushClientProcessor;

/**
 * Implements the Samza {@link SystemConsumer} interface using a queue. The Kinesis
 * client library threads add messages to the queue, and the Samza container thread
 * reads messages off the queue.
 */
public class KinesisSystemConsumer extends BlockingEnvelopeMap {

    private static final Log LOG = LogFactory.getLog(KinesisSystemConsumer.class);

    private final String systemName;
    private final Config config;

    private Map<SystemStreamPartition, ManagedClientProcessor> processors =
            new HashMap<SystemStreamPartition, ManagedClientProcessor>();

    private Map<SystemStreamPartition, Thread> threads = new HashMap<SystemStreamPartition, Thread>();

    public KinesisSystemConsumer(String systemName, Config config) {
        this.systemName = systemName;
        this.config = config;
    }

    /**
     * We assume here that each container consumes only one "partition" of the input
     * stream, where "partition" has been artificially set up in {@link KinesisSystemAdmin}
     * to be mapped 1:1 to Samza containers. Each partition may actually involve consuming
     * multiple shards, but that is handled by the Kinesis client library.<p>
     *
     * If the Samza job has multiple input streams, this method is called once for each
     * input stream.
     */
    @Override
    public void register(SystemStreamPartition systemStreamPartition, String offset) {
        ManagedClientProcessor processor = new SamzaPushClientProcessor(systemStreamPartition, this);
        processors.put(systemStreamPartition, processor);
        super.register(systemStreamPartition, offset);
    }

    /**
     * Start up a thread for each Kinesis stream we want to consume.
     */
    @Override
    public void start() {
        for (Map.Entry<SystemStreamPartition, ManagedClientProcessor> entry : processors.entrySet()) {
            ManagedConsumer consumer = new ManagedConsumer(
                entry.getKey().getStream(),
                config.get("job.name"),
                entry.getValue());
            Thread thread = new Thread(consumer);
            thread.start();
            threads.put(entry.getKey(), thread);
        }
    }

    @Override
    public void stop() {
        // TODO Make this more graceful. Perhaps ManagedConsumer should expose
        // Worker.shutdown() to us.
        for (Thread thread : threads.values()) thread.interrupt();
    }

    /**
     * Called by a {@link SamzaPushClientProcessor} when a message has been received
     * from an incoming Kinesis stream. The message is put on a queue, and the Samza
     * container will pick it up from there when polling for new messages.
     */
    public void putMessage(IncomingMessageEnvelope envelope) {
        try {
            put(envelope.getSystemStreamPartition(), envelope);
        } catch (InterruptedException e) {
            LOG.info("Interrupted while enqueueing message", e);
        }
    }
}
