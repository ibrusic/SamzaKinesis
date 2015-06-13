package com.amazonaws.services.kinesis.samza;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;

public class KinesisCheckpointManager implements CheckpointManager {

    private static final Log LOG = LogFactory.getLog(KinesisCheckpointManager.class);

    public KinesisCheckpointManager(Config config) {
    }

    @Override
    public void start() {
    }

    @Override
    public void register(TaskName taskName) {
    }

    @Override
    public void writeCheckpoint(TaskName taskName, Checkpoint checkpoint) {
        Set<String> systemNames = new HashSet<String>();
        for (SystemStreamPartition ssp : checkpoint.getOffsets().keySet()) {
            systemNames.add(ssp.getSystem());
        }

//        try {
//            for (String systemName : systemNames) {
//                KinesisSystemFactory.getConsumerBySystemName(systemName).checkpoint(checkpoint);
//            }
//        } catch (Exception e) {
//            // TODO handle this properly
//            LOG.error("Cannot write checkpoint", e);
//        }
    }

    @Override
    public Checkpoint readLastCheckpoint(TaskName taskName) {
        return null;
    }

    @Override
    public Map<TaskName, Integer> readChangeLogPartitionMapping() {
        return null; // TODO may need to return dummy map
    }

    @Override
    public void writeChangeLogPartitionMapping(Map<TaskName, Integer> mapping) {
    }

    @Override
    public void stop() {
    }
}
