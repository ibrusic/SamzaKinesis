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
import org.apache.commons.lang.StringUtils;
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
//    private static final String version = ".9.0";

    /**
     * Logger for KinesisConsumerRunnable
     */
    private static final Log LOG = LogFactory.getLog(KinesisConsumerRunnable.class);

    /**
     * Stream name
     */
    private String streamName;
    /**
     * App name
     */
    private String appName;
    /**
     * Region name
     */
    private String regionName;
    /**
     * Environment name
     */
    private String environmentName;
    /**
     * Kinesis endPoint
     */
    private String kinesisEndpoint;
    /**
     * AWS credentials holder
     */
    private AWSCredentialsProvider credentialsProvider;
    /**
     * Position in the stream
     */
    private InitialPositionInStream streamPosition;
    /**
     * Number of failures a worker can tolerate
     */
    private int failuresToTolerate;
    /**
     * Maximum number of records a worker can consume
     */
    private int maxRecords;
    /**
     * KLC configuration
     */
    private KinesisClientLibConfiguration config;
    /**
     * Flag set once KLC properties are set
     */
    private boolean isConfigured = false;
    /**
     * Template processor for consuming a Kinesis stream.
     */
    private AbstractKinesisRecordProcessor templateProcessor;

    private SystemStreamPartition systemStreamPartition;

    /**
     * Constructor
     *
     * @param streamName
     * @param appName
     * @param processor
     */
    public KinesisConsumerRunnable(String appName, String streamName,
                                   AbstractKinesisRecordProcessor processor, String streamPosition) {
        this.appName = appName;
        this.streamName = streamName;
        this.templateProcessor = processor;
        this.failuresToTolerate = DEFAULT_FAILURES_TOLERATED;
        this.maxRecords = DEFAULT_MAX_RECORDS;
        this.streamPosition = InitialPositionInStream.LATEST;
        if (!StringUtils.isEmpty(streamPosition)) {
            this.streamPosition = InitialPositionInStream.valueOf(streamPosition);
        }
    }

    @Override
    public void run() {
        try {
            runWorker();
        } catch (Exception e) {
            LOG.error("Error in worker loop", e);
            e.printStackTrace();
        }
    }

    /**
     * Runs an AWS worker and returns 0 if successful or -1 if an exception was found
     *
     * @return
     * @throws Exception
     */
    public int runWorker() throws Exception {
        int exitCode = 0;
        int failures = 0;

        // Configure KLC options and worker properties
        configure();
        LOG.info(String.format("Running %s to process stream %s", appName, streamName));
        IRecordProcessorFactory recordProcessorFactory = new KinesisRecordProcessorFactory(this.templateProcessor);

        // Starting worker
        Worker worker = new Worker(recordProcessorFactory, this.config);
        worker.run();

        // run the worker, tolerating as many failures as is configured
        while (failures < failuresToTolerate || failuresToTolerate == -1) {
            try {
                worker.run();
            } catch (Throwable t) {
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

    /**
     * Asserts if a condition is valid or not for configuration properties
     *
     * @param condition
     * @param message
     * @throws Exception
     */
    private void assertThat(boolean condition, String message) throws Exception {
        if (!condition) {
            throw new InvalidConfigurationException(message);
        }
    }

    /**
     * Validates that the minimum configuration options are set.
     *
     * @throws InvalidConfigurationException
     */
    private void validateConfig() throws InvalidConfigurationException {
        LOG.debug("Validating configuration");
        try {
            assertThat(this.streamName != null, "Must Specify a Stream Name");
            assertThat(this.appName != null, "Must Specify an Application Name");
        } catch (Exception e) {
            LOG.error("Error validating configuration properties.");
            throw new InvalidConfigurationException(e.getMessage());
        }
    }

    /**
     * Configures the KLC for usage.
     *
     * @throws Exception
     */
    public void configure() throws Exception {
        if (!isConfigured) {
            validateConfig();

            try {
                // Setting the user agent's name as our app name
                // String userAgent = "AWSKinesisManagedConsumer/" + this.version;
                String userAgent = this.appName;

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

                // obtain credentials using the default provider chain or the
                // credentials provider supplied
                AWSCredentialsProvider credentialsProvider = this.credentialsProvider == null ? new DefaultAWSCredentialsProviderChain()
                        : this.credentialsProvider;

                LOG.info("Using credentials with Access Key ID: "
                        + credentialsProvider.getCredentials().getAWSAccessKeyId());

                config = new KinesisClientLibConfiguration(appName, streamName,
                        credentialsProvider, workerId).withInitialPositionInStream(streamPosition).withKinesisEndpoint(
                        kinesisEndpoint);

                config.getKinesisClientConfiguration().setUserAgent(userAgent);

                if (regionName != null) {
                    config.withRegionName(Regions.valueOf(regionName).getName());
                }

                if (this.maxRecords != -1)
                    config.withMaxRecords(maxRecords);

                config.withInitialPositionInStream(this.streamPosition);

                LOG.info(String.format(
                        "Amazon Kinesis Aggregators Managed Client prepared for %s on %s in %s (%s) using %s Max Records",
                        config.getApplicationName(), config.getStreamName(),
                        config.getRegionName(), config.getWorkerIdentifier(),
                        config.getMaxRecords()));

                isConfigured = true;
            } catch (Exception e) {
                LOG.error("Error while configuring KLC properties.");
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
        this.regionName = regionName.toUpperCase();
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
        this.streamPosition = InitialPositionInStream.LATEST;
        if (!StringUtils.isEmpty(positionInStream)) {
            this.streamPosition = InitialPositionInStream.valueOf(positionInStream);
        }
        return this;
    }
}
