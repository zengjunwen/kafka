import org.apache.kafka.common.*;
import org.apache.kafka.clients.consumer.*;

import java.util.*;

public class ConsumerApp {
    public static void main(String[] args){

        // Create the Properties class to instantiate the Consumer with the desired settings:
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("fetch.min.bytes", 1);//fetch的数据没达到设定值时，不会fetch数据
        props.put("group.id", "");
        props.put("heartbeat.interval.ms", 3000);
        props.put("max.partition.fetch.bytes", 1048576);
        props.put("session.timeout.ms", 30000);
        props.put("auto.offset.reset", "latest");//第一次读取需要消费的topic的offset读取位置配置
        props.put("connections.max.idle.ms", 540000);
        props.put("enable.auto.commit", true);//是否自动提交last commit log
        props.put("exclude.internal.topics", true);
        props.put("max.poll.records", 2147483647);//poll从cluster获取的最大数据量，保证数据能正常的被customer处理
        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        props.put("request.timeout.ms", 40000);
        props.put("auto.commit.interval.ms", 5000);//自动提交last commit log 的间隔时间
        props.put("fetch.max.wait.ms", 500);//fetch topic数据的等待时间，类似produce的linger.ms
        props.put("metadata.max.age.ms", 300000);
        props.put("reconnect.backoff.ms", 50);
        props.put("retry.backoff.ms", 100);//非自动同步提交last commit log 时（异步提交失效），提交last commit log 失败时，重新发起提交的间隔时间
        props.put("client.id", "");


        // Create a KafkaConsumer instance and configure it with properties.
        KafkaConsumer<String, String> myConsumer = new KafkaConsumer<String, String>(props);

        // Create a topic subscription list:
        ArrayList<TopicPartition> partitions = new ArrayList<TopicPartition>();
        partitions.add(new TopicPartition("my-topic", 0)); // Adds a TopicPartition instance representing a topic and a partition.
        partitions.add(new TopicPartition("my-topic", 2)); // Adds an additional TopicPartition instance representing a different partition within the topic. Change as desired.
        // Assign partitions to the Consumer:
        myConsumer.assign(partitions);

        // Retrieves the topic subscription list from the SubscriptionState internal object:
        Set<TopicPartition> assignedPartitions = myConsumer.assignment();

        // Print the partition assignments:
        printSet(assignedPartitions);

        // Start polling for messages:
        try {
            while (true){
                ConsumerRecords records = myConsumer.poll(1000);
                printRecords(records);
            }
        } finally {
            myConsumer.close();
        }

    }

    private static void printSet(Set<TopicPartition> collection){
        if (collection.isEmpty()) {
            System.out.println("I do not have any partitions assigned yet...");
        }
        else {
            System.out.println("I am assigned to following partitions:");
            for (TopicPartition partition: collection){
                System.out.println(String.format("Partition: %s in Topic: %s", Integer.toString(partition.partition()), partition.topic()));
            }
        }
    }

    private static void printRecords(ConsumerRecords<String, String> records)
    {
        for (ConsumerRecord<String, String> record : records) {
            System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }
    }
}
