/**
 * Amazon Kinesis Aggregators Copyright 2014, Amazon.com, Inc. or its
 * affiliates. All Rights Reserved. Licensed under the Amazon Software License
 * (the "License"). You may not use this file except in compliance with the
 * License. A copy of the License is located at http://aws.amazon.com/asl/ or in
 * the "license" file accompanying this file. This file is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.amazonaws.services.kinesis.samza.consumer.kcl;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.UUID;

import com.amazonaws.services.kinesis.samza.consumer.InvalidConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.samza.system.SystemStreamPartition;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

import static com.amazonaws.services.kinesis.samza.consumer.Constants.DEFAULT_FAILURES_TOLERATED;
import static com.amazonaws.services.kinesis.samza.consumer.Constants.DEFAULT_MAX_RECORDS;

/**
 * Runnable class to consumer from Kinesis.
 */
public class KinesisConsumerRunnable implements Runnable {
    private static final String version = ".9.0";

    private static final Log LOG = LogFactory.getLog(KinesisConsumerRunnable.class);

    /** Stream name */
    private String streamName;
    /** App name */
    private String appName;
    /** Region name */
    private String regionName;
    /** Environment name */
    private String environmentName;
    /** Partition inStream */
    private String positionInStream;
    /** Kinesis endPoint */
    private String kinesisEndpoint;

    private AWSCredentialsProvider credentialsProvider;

    private InitialPositionInStream streamPosition;

    private int failuresToTolerate = DEFAULT_FAILURES_TOLERATED;

    private int maxRecords = DEFAULT_MAX_RECORDS;

    private KinesisClientLibConfiguration config;

    private boolean isConfigured = false;

    private AbstractKinesisRecordProcessor templateProcessor;

    private SystemStreamPartition systemStreamPartition;

    /**
     * Constructor
     * @param streamName
     * @param appName
     * @param processor
     */
    public KinesisConsumerRunnable(String appName, String streamName,
                                   AbstractKinesisRecordProcessor processor, String streamPosition) {
        this.appName = appName;
        this.streamName = streamName;
        this.templateProcessor = processor;
        this.positionInStream = streamPosition;
        this.failuresToTolerate = DEFAULT_FAILURES_TOLERATED;
    }

    @Override
    public void run() {
        try {
           runWorker();
        } catch (Exception e) {
            System.out.println("Error in worker loop");
            LOG.error("Error in worker loop", e);
            e.printStackTrace();
        }
    }

    public int runWorker() throws Exception {
        configure();

        System.out.println(String.format("Starting %s", appName));
        LOG.info(String.format("Running %s to process stream %s", appName, streamName));

        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(appName,
                        streamName,
                        credentialsProvider,
                        workerId);
        kinesisClientLibConfiguration.withInitialPositionInStream(InitialPositionInStream.LATEST);

        IRecordProcessorFactory recordProcessorFactory = new KinesisRecordProcessorFactory(this.templateProcessor);
        Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);
        worker.run();

        int exitCode = 0;
        int failures = 0;

        // run the worker, tolerating as many failures as is configured
        while (failures < failuresToTolerate || failuresToTolerate == -1) {
            try {
                worker.run();
            } catch (Throwable t) {
                System.out.println("error");
                LOG.error("Caught throwable while processing data.", t);

                failures++;

                if (failures < failuresToTolerate) {
                    LOG.error("Restarting...");
                }
                exitCode = 1;
            }
        }

        return exitCode;
    }

    private void assertThat(boolean condition, String message) throws Exception {
        if (!condition) {
            throw new InvalidConfigurationException(message);
        }
    }

    private void validateConfig() throws InvalidConfigurationException {
        System.out.println("validating");
        try {
            assertThat(this.streamName != null, "Must Specify a Stream Name");
            assertThat(this.appName != null, "Must Specify an Application Name");
        } catch (Exception e) {
            throw new InvalidConfigurationException(e.getMessage());
        }
    }

    public void configure() throws Exception {
        if (!isConfigured) {
            validateConfig();

            try {
                String userAgent = "AWSKinesisManagedConsumer/" + this.version;

                if (this.positionInStream != null) {
                    streamPosition = InitialPositionInStream.valueOf(this.positionInStream);
                } else {
                    streamPosition = InitialPositionInStream.LATEST;
                }

                // append the environment name to the application name
                if (environmentName != null) {
                    appName = String.format("%s-%s", appName, environmentName);
                }

                // ensure the JVM will refresh the cached IP values of AWS
                // resources
                // (e.g. service endpoints).
                java.security.Security.setProperty("networkaddress.cache.ttl", "60");

                String workerId = NetworkInterface.getNetworkInterfaces() + ":" + UUID.randomUUID();
                LOG.info("Using Worker ID: " + workerId);
                System.out.println("Using Worker ID: " + workerId);

                // obtain credentials using the default provider chain or the
                // credentials provider supplied
                AWSCredentialsProvider credentialsProvider = this.credentialsProvider == null ? new DefaultAWSCredentialsProviderChain()
                        : this.credentialsProvider;

                LOG.info("Using credentials with Access Key ID: "
                        + credentialsProvider.getCredentials().getAWSAccessKeyId());

                System.out.println("Using credentials with Access Key ID: "
                        + credentialsProvider.getCredentials().getAWSAccessKeyId());

                config = new KinesisClientLibConfiguration(appName, streamName,
                        credentialsProvider, workerId).withInitialPositionInStream(streamPosition).withKinesisEndpoint(
                        kinesisEndpoint);

                config.getKinesisClientConfiguration().setUserAgent(userAgent);

                if (regionName != null) {
                    Region region = Region.getRegion(Regions.fromName(regionName));
                    config.withRegionName(region.getName());
                }

                if (this.maxRecords != -1)
                    config.withMaxRecords(maxRecords);

                if (this.positionInStream != null)
                    config.withInitialPositionInStream(InitialPositionInStream.valueOf(this.positionInStream));

                LOG.info(String.format(
                        "Amazon Kinesis Aggregators Managed Client prepared for %s on %s in %s (%s) using %s Max Records",
                        config.getApplicationName(), config.getStreamName(),
                        config.getRegionName(), config.getWorkerIdentifier(),
                        config.getMaxRecords()));

                isConfigured = true;
            } catch (Exception e) {
                throw new InvalidConfigurationException(e);
            }
        }
    }

    public KinesisConsumerRunnable withKinesisEndpoint(String kinesisEndpoint) {
        this.kinesisEndpoint = kinesisEndpoint;
        return this;
    }

    public KinesisConsumerRunnable withToleratedWorkerFailures(int failuresToTolerate) {
        this.failuresToTolerate = failuresToTolerate;
        return this;
    }

    public KinesisConsumerRunnable withMaxRecords(int maxRecords) {
        this.maxRecords = maxRecords;
        return this;
    }

    public KinesisConsumerRunnable withRegionName(String regionName) {
        this.regionName = regionName;
        return this;
    }

    public KinesisConsumerRunnable withEnvironment(String environmentName) {
        this.environmentName = environmentName;
        return this;
    }

    public KinesisConsumerRunnable withCredentialsProvider(AWSCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
        return this;
    }

    public KinesisConsumerRunnable withInitialPositionInStream(String positionInStream) {
        this.positionInStream = positionInStream;
        return this;
    }
}
