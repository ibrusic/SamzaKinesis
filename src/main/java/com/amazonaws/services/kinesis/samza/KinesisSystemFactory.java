package com.amazonaws.services.kinesis.samza;

import java.util.HashMap;

import com.amazonaws.services.kinesis.samza.consumer.KinesisSystemConsumer;
import com.amazonaws.services.kinesis.samza.producer.KinesisSystemProducer;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;

/**
 * Kinesis System factory
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

    //TODO check metrics
    //TODO serialization
    @Override
    public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
        KinesisSystemProducer producer = new KinesisSystemProducer(systemName, config);
        return producer;
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
