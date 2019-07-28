package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by DRAGON on 2019/7/17.
 */
public class KafkaConsumerBase {

    public static final String brokerList = "192.168.56.101:9092";
    public static final String topic="topic_test";

    public static final String groupId="group_test";

    public static void main(String[] args) {
        Properties props =new Properties();
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("bootstrap.servers", brokerList);
        props.put("group.id",groupId);

        //创建消费者实例
        KafkaConsumer<String,String> consumer=new KafkaConsumer(props);

        //订阅主题
        consumer.subscribe(Collections.singletonList(topic));


        while(true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String,String> record:records){
                System.out.println(record.value());
            }
        }
    }

}
