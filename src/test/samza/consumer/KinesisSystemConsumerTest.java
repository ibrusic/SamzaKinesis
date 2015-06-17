package samza.consumer;

import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.samza.consumer.KinesisSystemConsumer;
import com.amazonaws.services.kinesis.samza.consumer.kcl.ImplKinesisRecordProcessor;
import com.amazonaws.services.kinesis.samza.consumer.kcl.KinesisConsumerRunnable;
import org.apache.samza.config.MapConfig;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.services.kinesis.samza.consumer.Constants.*;

/**
 * API test
 */
public class KinesisSystemConsumerTest {

    public static final String SAMPLE_APPLICATION_STREAM_NAME = "myFirstStream";

    public static final String SAMPLE_APPLICATION_NAME = "kinesisApp";

    private static final InitialPositionInStream SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM =
            InitialPositionInStream.LATEST;

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
        ).withCredentialsProvider(new PropertiesFileCredentialsProvider(path)).withRegionName(Regions.US_EAST_1.toString());

        Thread thread = new Thread(worker2);
        thread.start();
//        Thread t2 = new Thread(worker2);
//        t2.start();
    }

}
