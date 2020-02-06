import org.apache.kafka.clients.producer.*;

import java.text.*;
import java.util.*;

public class ProducerApp {

    public static void main(String[] args){

        // Create the Properties class to instantiate the Consumer with the desired settings:
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");//建议给两个有助于容灾
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "2");//0 Producer不需要等待任何确认消息；1 需要leader将数据持久化到log；2 leader将数据持久化到所有replications中
        props.put("buffer.memory", 33554432);
        props.put("compression.type", "none");//压缩
        props.put("retries", 0);//RecordBatch发送失败允许重新发送次数
        props.put("batch.size", 16384);//单次与broker交互的数据量设置
        props.put("client.id", "");
        props.put("linger.ms", 0);//RecordBatch在没满时，数据发送到Broker等待时长设置
        props.put("max.block.ms", 60000);
        props.put("max.request.size", 1048576);
        props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        props.put("request.timeout.ms", 30000);
        props.put("timeout.ms", 30000);
        props.put("max.in.flight.requests.per.connection", 5);//最大连接数
        props.put("retry.backoff.ms", 5);//发送失败，重新发送的延迟时间

        KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(props);
        DateFormat dtFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");
        String topic = "my-topic";

        int numberOfRecords = 100; // number of records to send
        long sleepTimer = 0; // how long you want to wait before the next record to be sent

        try {
                for (int i = 0; i < numberOfRecords; i++ )
                    myProducer.send(new ProducerRecord<String, String>(topic, String.format("Message: %s  sent at %s", Integer.toString(i), dtFormat.format(new Date()))));
                    Thread.sleep(sleepTimer);
                    // Thread.sleep(new Random(5000).nextLong()); // use if you want to randomize the time between record sends
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            myProducer.close();
        }

    }
}
