package com.amazonaws.services.kinesis.samza;

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

    @Override
    public SystemAdmin getAdmin(String systemName, Config config) {
        return new KinesisSystemAdmin(systemName, config);
    }

    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
        return new KinesisSystemConsumer(systemName, config);
    }

    @Override
    public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
        throw new SamzaException("Sending messages to Kinesis is not yet supported");
    }
}
