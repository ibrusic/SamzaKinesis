package com.amazonaws.services.kinesis.samza;

import java.util.HashMap;

import com.amazonaws.services.kinesis.samza.consumer.KinesisSystemConsumer;
import com.amazonaws.services.kinesis.samza.producer.KinesisSystemProducer;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;

/**
 * Reference this factory class in your Samza job config to consume Kinesis streams
 * in your job. Example:
 * 
 * <pre>
 * systems.kinesis.samza.factory=com.amazonaws.services.kinesis.samza.KinesisSystemFactory
 * task.inputs=kinesis.stream-name
 * </pre>
 */
public class KinesisSystemFactory implements SystemFactory {

    private static final HashMap<String, KinesisSystemConsumer> consumers =
            new HashMap<String, KinesisSystemConsumer>();

    @Override
    public SystemAdmin getAdmin(String systemName, Config config) {
        return new KinesisSystemAdmin(systemName, config);
    }

    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
        KinesisSystemConsumer consumer = new KinesisSystemConsumer(systemName, config);
        consumers.put(systemName, consumer);
        return consumer;
    }

    @Override
    public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
        KinesisSystemProducer producer = new KinesisSystemProducer(systemName, config);
        return producer;
        //throw new SamzaException("Sending messages to Kinesis is not yet supported");
    }

    /**
     * Needed so that the CheckpointManager can find the SystemConsumer instance.
     * Package visibility since the rest of the world shouldn't have to worry
     * about this.
     */
    static KinesisSystemConsumer getConsumerBySystemName(String systemName) {
        return consumers.get(systemName);
    }
}
