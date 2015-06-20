package com.amazonaws.services.kinesis.samza.consumer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.services.kinesis.samza.KinesisUtils;
import com.amazonaws.services.kinesis.samza.consumer.kcl.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;

import static com.amazonaws.services.kinesis.samza.consumer.Constants.*;

/**
 * Implements the Samza {@link SystemConsumer} interface using a queue. The
 * Kinesis client library threads add messages to the queue, and the Samza
 * container thread reads messages off the queue.
 */
public class KinesisSystemConsumer extends BlockingEnvelopeMap {
    /**
     * AWS credentials
     */
    private static AWSCredentialsProvider credentials;
    /**
     * App name
     */
    private final String appName;
    /**
     * Region where the Kinesis streams are
     */
    private final String region;
    /**
     * Logger for the KinesisSystemConsumer
     */
    private static final Log LOG = LogFactory.getLog(KinesisSystemConsumer.class);
    /**
     * System name
     */
    private final String systemName;
    /**
     * Initial Kinesis stream position
     */
    private String initialPos;

    /**
     * One processor per Samza partition (and since we try to use one partition per
     * container, this map is normally expected to have one entry). This processor
     * is copied every time a new shard is started.
     */
    private Map<SystemStreamPartition, AbstractKinesisRecordProcessor> templateProcessors =
            new HashMap<SystemStreamPartition, AbstractKinesisRecordProcessor>();

    /**
     * One processor per Kinesis shard that we start consuming. Key is shardId.
     */
    private Map<String, AbstractKinesisRecordProcessor> processors =
            new HashMap<String, AbstractKinesisRecordProcessor>();

    /**
     * Message sequence numbers delivered per partition since the last checkpoint.
     */
    private Map<SystemStreamPartition, Queue<Delivery>> deliveries =
            new ConcurrentHashMap<>();
    /**
     * Map for relating each SSP to the created threads
     */
    private Map<SystemStreamPartition, Thread> threads = new HashMap<SystemStreamPartition, Thread>();

    /**
     * Constructor
     *
     * @param systemName
     * @param config
     */
    public KinesisSystemConsumer(String systemName, Config config) {
        String awsCredentialsPath = config.get(String.format("systems.%s.%s", systemName, CONFIG_PATH_PARAM));
        String iniPos = config.get(String.format("systems.%s.%s", systemName, STREAM_POSITION_PARAM));
        String region = config.get(String.format("systems.%s.%s", systemName, AWS_REGION_PARAM));
        String appName = config.get("job.name");
        this.systemName = systemName;
        this.appName = appName;
        this.initialPos = iniPos;
        this.region = region.toUpperCase();
        this.credentials = KinesisUtils.loadAwsCredentials(awsCredentialsPath);
    }

    /**
     * We assume here that each container consumes only one "partition" of the
     * input stream, where "partition" has been artificially set up in
     * {@link com.amazonaws.services.kinesis.samza.KinesisSystemAdmin} to be mapped 1:1 to Samza containers. Each
     * partition may actually involve consuming multiple shards, but that is
     * handled by the Kinesis client library.
     * <p/>
     * If the Samza job has multiple input streams, this method is called once
     * for each input stream.
     */
    @Override
    public void register(SystemStreamPartition systemStreamPartition, String offset) {
        super.register(systemStreamPartition, offset);
        AbstractKinesisRecordProcessor processor = new ImplKinesisRecordProcessor(systemStreamPartition, this);
        this.templateProcessors.put(systemStreamPartition, processor);
    }

    /**
     * Start up a thread for each Kinesis stream to be consume.
     */
    @Override
    public void start() {
        for (Map.Entry<SystemStreamPartition, AbstractKinesisRecordProcessor> entry : templateProcessors.entrySet()) {
                    createKinesisConsumerThread(entry.getKey(), entry.getKey().getStream(), entry.getValue());
        }
    }

    /**
     * Creates Kinesis consumer thread
     *
     * @param ssp
     * @param streamName
     * @param processor
     */
    private void createKinesisConsumerThread(SystemStreamPartition ssp, String streamName, AbstractKinesisRecordProcessor processor) {
        LOG.info(String.format("Thread created for partition %s ", ssp.toString()));
        KinesisConsumerRunnable consumer = new KinesisConsumerRunnable(this.appName,
                streamName, processor, this.initialPos).withCredentialsProvider(credentials).withRegionName(region);
        Thread thread = new Thread(consumer);
        thread.start();
        threads.put(ssp, thread);
    }

    @Override
    public void stop() {
        // TODO Make this more graceful. Perhaps KinesisConsumerRunnable should expose
        // Worker.shutdown() to us.
        for (Thread thread : threads.values())
            thread.interrupt();
    }

//    /**
//     * Translates a Samza checkpoint into Kinesis Client Library checkpoints.
//     */
//    public synchronized void checkpoint(Checkpoint checkpoint) throws Exception {
//        for (Map.Entry<SystemStreamPartition, String> entry : checkpoint.getOffsets().entrySet()) {
//            Queue<Delivery> queue = deliveries.get(entry.getKey());
//            if (queue == null) continue;
//
//            HashMap<SamzaPushKinesisClientProcessor, String> latestSeqNos =
//                    new HashMap<SamzaPushKinesisClientProcessor, String>();
//            Delivery delivery;
//
//            // Go through the history of messages delivered since the last checkpoint.
//            // Find the most recent sequence number for each processor. Stop when
//            // either the queue is empty or the current checkpoint is reached.
//            while (true) {
//                delivery = queue.poll();
//                if (delivery == null) break;
//                latestSeqNos.put(delivery.processor, delivery.sequenceNumber);
//                if (delivery.sequenceNumber.equals(entry.getValue())) break;
//            }
//
//            for (Map.Entry<SamzaPushKinesisClientProcessor, String> seqNo : latestSeqNos.entrySet()) {
//                seqNo.getKey().checkpoint(seqNo.getValue());
//            }
//        }
//    }

    /**
     * Called by a SamzaPushKinesisClientProcessor when it starts consuming
     * messages from a new Kinesis shard. (Each shard gets its own processor
     * instance.)
     *
     * @param shardId   Kinesis identifier of the shard that's being consumed.
     * @param processor IRecordProcessor instance for that shard.
     */
    public void registerProcessor(String shardId, AbstractKinesisRecordProcessor processor) {
        processors.put(shardId, processor);
    }

    /**
     * Called by an implementation of AbstractKinesisRecordProcessor when a message has been
     * received from an incoming Kinesis stream. The message is put on a queue,
     * and the Samza container will pick it up from there when polling for new
     * messages.
     */
    public void putMessage(AbstractKinesisRecordProcessor kinesisRecordProcessor, IncomingMessageEnvelope envelope) {
        // keeping track of received messages
        this.trackDeliveries(kinesisRecordProcessor, envelope);
        // getting the message ready for Samza
        try {
            put(envelope.getSystemStreamPartition(), envelope);
        } catch (InterruptedException e) {
            LOG.info("Interrupted while enqueueing message", e);
        }
    }

    /**
     * Method to help us keep track of messages delivered.
     *
     * @param processor
     * @param envelope
     */
    private void trackDeliveries(AbstractKinesisRecordProcessor processor, IncomingMessageEnvelope envelope) {
        if (!deliveries.containsKey(envelope.getSystemStreamPartition())) {
            deliveries.put(envelope.getSystemStreamPartition(), new ConcurrentLinkedQueue<Delivery>());
        }

        Queue<Delivery> queue = deliveries.get(envelope.getSystemStreamPartition());
        queue.add(new Delivery(envelope.getOffset(), processor));
    }

    /**
     * Class to map between the sequenceNumbers obtained and the processors that did
     */
    private static class Delivery {
        /**
         * Message sequence number
         */
        private final String sequenceNumber;
        /**
         * Kinesis record processor
         */
        private final AbstractKinesisRecordProcessor processor;

        /**
         * Constructor
         *
         * @param sequenceNumber
         * @param processor
         */
        public Delivery(String sequenceNumber, AbstractKinesisRecordProcessor processor) {
            this.sequenceNumber = sequenceNumber;
            this.processor = processor;
        }
    }

}
