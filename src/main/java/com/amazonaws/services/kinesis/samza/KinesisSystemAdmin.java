package com.amazonaws.services.kinesis.samza;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.kinesis.model.StreamDescription;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;

import static com.amazonaws.services.kinesis.samza.consumer.Constants.AWS_REGION_PARAM;
import static com.amazonaws.services.kinesis.samza.consumer.Constants.CONFIG_PATH_PARAM;

/**
 * Simple placeholder SystemAdmin. Normally this would be used by Samza to find out what
 * input stream partitions exist for a stream, so that Samza can assign partitions to
 * containers. However, we're leaving the assignment of Kinesis shards to workers to the
 * Kinesis client library, so this SystemAdmin just does the minimum necessary to satisfy
 * Samza's API requirements.<p>
 *
 * Currently, we read the requested number of containers from the job config, and
 * create that many "partitions". The Samza ApplicationMaster then assigns one partition
 * numbers to each container, so we end up with one StreamTask instance per container
 * (regardless of how many Kinesis shards that container is actually consuming).<p>
 *
 * A nicer solution would be to integrate the Kinesis client library's shard assignment
 * mechanism with Samza, so that we could have one StreamTask instance per Kinesis shard.
 * However, that would require rethinking some of the Kinesis client library.
 */
public class KinesisSystemAdmin implements SystemAdmin {

    // Number of containers is yarn-specific, it will have to reviewed
//    private static final String CONTAINER_COUNT_CONFIG = "yarn.container.count";

    // TODO can we define partition metadata meaningfully for Kinesis?
    // (This would be needed to detect whether a stream has caught up to the head --
    // i.e. bootstrap streams)
    private static final SystemStreamPartitionMetadata emptyMetadata =
            new SystemStreamPartitionMetadata(null, null, null);
    // Credentials for determining the number of shards
    private final String credentialsPath;
    // Region where the streams reside TODO different streams might be in different regions
    private final String region;

    /**
     * Constructor
     * @param systemName
     * @param config
     */
    KinesisSystemAdmin(String systemName, Config config) {
        credentialsPath = config.get(String.format("systems.%s.%s", systemName, CONFIG_PATH_PARAM));
        region = config.get(String.format("systems.%s.%s", systemName, AWS_REGION_PARAM));
    }

    @Override
    public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
        Map<String, SystemStreamMetadata> metadata = new HashMap<String, SystemStreamMetadata>();

        for (String streamName : streamNames) {
            // Get the number of shards
            int numShards = setShardNumber(streamName,credentialsPath,region);
            if (numShards > 0) {
                // Create Metadata for each shard found
                Map<Partition, SystemStreamPartitionMetadata> partitionMeta =
                        new HashMap<Partition, SystemStreamPartitionMetadata>();
                for (int partition = 0; partition < numShards; partition++) {
                    partitionMeta.put(new Partition(partition), emptyMetadata);
                }
                metadata.put(streamName, new SystemStreamMetadata(streamName, partitionMeta));
            } else {
                throw new IllegalArgumentException(streamName + " has no shards");
            }
        }
        return metadata;
    }

    @Override
    public void createChangelogStream(String s, int i) {
        //TODO
    }

    @Override
    public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
        Map<SystemStreamPartition, String> offsetsAfter = new HashMap<SystemStreamPartition, String>();
        for (SystemStreamPartition systemStreamPartition : offsets.keySet()) {
            offsetsAfter.put(systemStreamPartition, null);
        }
        return offsetsAfter;
    }

    /**
     * Gets the number of shards a Kinesis stream has.
     * @param streamName
     */
    private int setShardNumber(String streamName, String credentialsPath, String region) {
        int numShards = 0;
        StreamDescription descriptionStream = KinesisUtils.getDescriptionStream(streamName, credentialsPath, region);
        if (descriptionStream != null) {
            numShards = descriptionStream.getShards().size();
        }
        return numShards;
    }
}
