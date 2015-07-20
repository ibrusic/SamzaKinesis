package samza.examples;

import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by renatomarroquin on 2015-07-13.
 */
public class ShardRetriever {
    public static void main(String []args) {
        final String stream = "teste";
        final byte[] testData = "testData".getBytes();


        PropertiesFileCredentialsProvider credentialsProvider = new PropertiesFileCredentialsProvider("/Users/renatomarroquin/Documents/Libs/Amazon/rootkey.prod.csv");

        AmazonKinesis ak = new AmazonKinesisClient(credentialsProvider);


        String shardIterator;
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setStreamName(stream);
//        getShardIteratorRequest.setShardId(shard.getShardId());
        getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");

        GetShardIteratorResult getShardIteratorResult = ak.getShardIterator(getShardIteratorRequest);
        shardIterator = getShardIteratorResult.getShardIterator();
        System.out.println(shardIterator);


//        System.out.println(ak.describeStream(stream).getStreamDescription().getShards());

//        ak.setRegion(Region.getRegion(Regions.US_EAST_1));
//        PutRecordResult prr = ak.putRecord(stream, ByteBuffer.wrap(testData), "partitionKey");
//
//        GetShardIteratorResult gsir = ak.getShardIterator(new GetShardIteratorRequest()
//                .withStreamName(stream));
//                .withShardId("shardId-000000000001")//.withShardId(prr.getShardId())
//                .withShardIteratorType("AT_SEQUENCE_NUMBER")
//                .withStartingSequenceNumber("49552544318949412405479205085097563623345662905406193682"));//.withStartingSequenceNumber(prr.getSequenceNumber()));
//
//        GetRecordsResult grr = ak.getRecords(new GetRecordsRequest()
//                .withLimit(1)
//                .withShardIterator(gsir.getShardIterator()));
//
//        Record record = grr.getRecords().get(0);
//        byte[] b = new byte[record.getData().remaining()];
//        record.getData().get(b);
//        System.out.println(new String(b));
//        if (Arrays.equals(b, testData)) {
//            System.err.println("Data was equal");
//        } else {
//            System.err.println("Data was NOT equal");
//        }
    }
}
