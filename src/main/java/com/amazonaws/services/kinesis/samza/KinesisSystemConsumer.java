package com.amazonaws.services.kinesis.samza;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.samza.consumer.kcl.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
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

    public static final String SAMPLE_APPLICATION_STREAM_NAME = "myFirstStream";

    public static final String SAMPLE_APPLICATION_NAME = "kinesisApp";

    private static AWSCredentialsProvider credentialsProvider;
    private final String appName;

    public static void main(String[] args) throws Exception {
        String path = "/Users/renatomarroquin/Documents/Libs/Amazon/rootkey.prod.csv";
        Map<String, String> m = new HashMap<>();
        m.put(String.format("systems.%s.%s", SAMPLE_APPLICATION_NAME, CONFIG_PATH_PARAM), path);
        m.put(String.format("systems.%s.%s", SAMPLE_APPLICATION_NAME, STREAM_POSITION_PARAM), SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM.toString());
        m.put(String.format("systems.%s.%s", SAMPLE_APPLICATION_NAME, APP_NAME_PARAM), SAMPLE_APPLICATION_NAME);
        m.put(String.format("systems.%s.%s", SAMPLE_APPLICATION_NAME, STREAM_NAME_PARAM), SAMPLE_APPLICATION_STREAM_NAME);
        MapConfig conf = new MapConfig(m);
        KinesisConsumerRunnable worker2 = new KinesisConsumerRunnable(SAMPLE_APPLICATION_NAME,
                SAMPLE_APPLICATION_STREAM_NAME,
                new ImplKinesisRecordProcessor(null, new KinesisSystemConsumer(SAMPLE_APPLICATION_NAME, conf)), "LATEST"
        ).withCredentialsProvider(credentialsProvider);

        Thread thread = new Thread(worker2);
        thread.start();
        Thread t2 = new Thread(worker2);
        t2.start();
    }

    private static void runAmazon() throws UnknownHostException {
        credentialsProvider = new PropertiesFileCredentialsProvider("/Users/renatomarroquin/Documents/Libs/Amazon/rootkey.prod.csv");

        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(SAMPLE_APPLICATION_NAME,
                        SAMPLE_APPLICATION_STREAM_NAME,
                        credentialsProvider,
                        workerId);
        kinesisClientLibConfiguration.withInitialPositionInStream(SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM);

//        IRecordProcessorFactory recordProcessorFactory = new AmazonKinesisApplicationRecordProcessorFactory();
        IRecordProcessorFactory recordProcessorFactory = new KinesisRecordProcessorFactory(new ImplKinesisRecordProcessor(null, new KinesisSystemConsumer("System", null)));
        Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

        System.out.printf("Running %s to process stream %s as worker %s...\n",
                SAMPLE_APPLICATION_NAME,
                SAMPLE_APPLICATION_STREAM_NAME,
                workerId);

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            System.err.println("Caught throwable while processing data.");
            t.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    private static final Log LOG = LogFactory.getLog(KinesisSystemConsumer.class);

    private final String systemName;

    private final Config config;

    private String initialPos;

    // Initial position in the stream when the application starts up for the first time.
    // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
    private static final InitialPositionInStream SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM =
            InitialPositionInStream.LATEST;

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
            new HashMap<SystemStreamPartition, Queue<Delivery>>();

    private Map<SystemStreamPartition, Thread> threads = new HashMap<SystemStreamPartition, Thread>();

    /**
     * Constructor
     * @param systemName
     * @param config
     */
    public KinesisSystemConsumer(String systemName, Config config) {
        System.out.println(config);
        String awsCredentialsPath = config.get(String.format("systems.%s.%s", systemName, CONFIG_PATH_PARAM));
        String iniPos = config.get(String.format("systems.%s.%s", systemName, STREAM_POSITION_PARAM));
        String appName = config.get("job.name");
        this.systemName = systemName;
        this.config = config;
        this.appName = appName;
        this.initialPos = iniPos;

        credentialsProvider = awsCredentialsPath == null ? new DefaultAWSCredentialsProviderChain() :
                new PropertiesFileCredentialsProvider(awsCredentialsPath);
    }

    /**
     * We assume here that each container consumes only one "partition" of the
     * input stream, where "partition" has been artificially set up in
     * {@link KinesisSystemAdmin} to be mapped 1:1 to Samza containers. Each
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
            // TODO right now number of shards are specified in conf file, but they could be obtained from Amazon.
            String [] streamShards = entry.getKey().getStream().split("#");
            if (streamShards.length == 2) {
                int numShards = Integer.parseInt(streamShards[1]);
                LOG.info(String.format("Creating %d threads for %s stream.", numShards, streamShards[0]));
                for (int cnt = 0; cnt < numShards; cnt ++){
                    createKinesisConsumerThread(entry.getKey(), streamShards[0], entry.getValue());
                }
            } else {
                createKinesisConsumerThread(entry.getKey(), streamShards[0], entry.getValue());
            }

        }
    }

    /**
     * Creates Kinesis consumer thread
     * @param ssp
     * @param streamName
     * @param processor
     */
    private void createKinesisConsumerThread(SystemStreamPartition ssp, String streamName, AbstractKinesisRecordProcessor processor) {
        LOG.info(String.format("Thread created for partition %s ", ssp.toString()));
        KinesisConsumerRunnable consumer = new KinesisConsumerRunnable(this.appName,
                streamName, processor, this.initialPos).withCredentialsProvider(credentialsProvider);
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
    //TODO does it need to be synchronized?
    public synchronized void putMessage(AbstractKinesisRecordProcessor kinesisRecordProcessor, IncomingMessageEnvelope envelope) {
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
            deliveries.put(envelope.getSystemStreamPartition(), new LinkedList<Delivery>());
        }

        Queue<Delivery> queue = deliveries.get(envelope.getSystemStreamPartition());
        queue.add(new Delivery(envelope.getOffset(), processor));
    }

    private static class Delivery {
        private final String sequenceNumber;
        private final AbstractKinesisRecordProcessor processor;

        public Delivery(String sequenceNumber, AbstractKinesisRecordProcessor processor) {
            this.sequenceNumber = sequenceNumber;
            this.processor = processor;
        }
    }

}
