package com.amazonaws.services.kinesis.samza;

import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;

/**
 * Reference this factory class in your Samza job config to make sure that the
 * consumer's position in the input stream is correctly checkpointed. The Kinesis
 * client library actually writes checkpoints to DynamoDB, but we call it
 * KinesisCheckpointManager anyway, because it's meant to be used with Kinesis.<p>
 *
 * Note that this checkpoint manager only allows Kinesis input streams to be
 * checkpointed. If your job is also consuming from some other system, that system
 * will not be checkpointed.<p>
 *
 * Usage example:
 *
 * <pre>
 * task.checkpoint.factory=com.amazonaws.services.kinesis.samza.KinesisCheckpointManagerFactory
 * </pre>
 */
public class KinesisCheckpointManagerFactory implements CheckpointManagerFactory {

    @Override
    public CheckpointManager getCheckpointManager(Config config, MetricsRegistry registry) {
        return new KinesisCheckpointManager(config);
    }
}
