package consumer;

import kafkacommon.KafkaConsumerInit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by DRAGON on 2019/7/28.
 * Kafka消费分区数据
 */
public class KafkaConsumerPartition {
    public static final String brokerList = "192.168.56.101:9092";
    public static final String topic="topic_test";
    public static final String group="group_topic_partition";

    public static void main(String[] args) {


    }

    /**
     *  订阅全部分区
     */
    public static void subscribeTopicPartition(){
        Properties props=KafkaConsumerInit.consumerInit(brokerList,group);//初始化Kafka参数
        KafkaConsumer<String,String> consumer=new KafkaConsumer(props);   // 创建Kafkaconsumer对象

//        consumer.subscribe(Arrays.asList(topic));//订阅topic
        List<TopicPartition> partitions=new ArrayList<>(); //创建partition的list

        List<PartitionInfo> partitionInfos=consumer.partitionsFor(topic); //获取topic的分区信息
        if(partitionInfos !=null){
            for(PartitionInfo partitionInfo:partitionInfos){
                //把分区加入partitions
                partitions.add(new TopicPartition(partitionInfo.topic(),partitionInfo.partition()));
            }
        }
        consumer.assign(partitions);//订阅分区的消息

        while(true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord record:records){
                System.out.println(record.value());
            }
        }
    }

    /**
     * 订阅topic，通过partition 消费数据
     */
    public static void recordPartiton(){
        Properties props=KafkaConsumerInit.consumerInit(brokerList,group);//初始化Kafka参数
        KafkaConsumer<String,String> consumer=new KafkaConsumer(props);   // 创建Kafkaconsumer对象
        consumer.subscribe(Arrays.asList(topic));//订阅topic
        ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1000));//消费topic
        for (TopicPartition tp:records.partitions()){//records获取分区
            for (ConsumerRecord record:records.records(tp)){//消费分区
                System.out.println(record.partition()+": "+record.value());
            }
        }

    }
}
