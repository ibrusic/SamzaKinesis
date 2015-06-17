/**
 * Amazon Kinesis Aggregators
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.samza.consumer;

/**
 * Contains all the constants needed by the Kinesis' consumers/producers.
 */
public class Constants {
    // Configurations coming from the kinesis-stream.properties
    /**
     * AWS credentials path
     */
    public static final String CONFIG_PATH_PARAM = "config-file-path";

    /**
     * AWS stream name
     */
    public static final String STREAM_NAME_PARAM = "stream-name";

    /**
     * Application that will consume such stream
     */
    public static final String APP_NAME_PARAM = "job.name";

    /**
     * Position in the AWS stream from where we start consuming
     */
    public static final String STREAM_POSITION_PARAM = "position-in-stream";

    /**
     * Aws region parameter
     */
    public static final String AWS_REGION_PARAM = "aws-region";

    // TODO this still need to be set up
    /**
     * Maximum number of records consumed by record processor.
     */
    public static final String MAX_RECORDS_PARAM = "max-records";

    /**
     * Default failures to be tolerated when reading from a kinesis stream
     */
    public static final String FAILURES_TOLERATED_PARAM = "failures-tolerated";

    /**
     * Environment from where the application is being executed
     */
    public static final String ENVIRONMENT_PARAM = "environment";

    // Default configuration values values
    /**
     * Default failures tolerated when consuming Kinesis streams.
     */
    public static final int DEFAULT_FAILURES_TOLERATED = -1;

    /**
     * Default number of checkpoint retires.
     */
    public static final int DEFAULT_NUM_RETRIES = 10;

    /**
     * Backoff time when trying to checkpoint
     */
    public static final long DEFAULT_BACKOFF_TIME_IN_MILLIS = 100L;

    /**
     * Default max number of recors to be consumed
     */
    public static final int DEFAULT_MAX_RECORDS = -1;
}
