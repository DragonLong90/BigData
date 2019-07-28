package producer;

import kafkacommon.ProducerInterceptorPrefix;
import org.apache.kafka.clients.producer.*;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Created by DRAGON on 2019/7/17.
 */
public class KafkaProducerBase {

    private static Logger logger = Logger.getLogger(KafkaProducerBase.class);

    public static final String brokerList = "192.168.56.101:9092";
    public static final String topic = "topic_test";

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", brokerList);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("interceptor.classes", ProducerInterceptorPrefix.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 5; i++){
            ProducerRecord record=new ProducerRecord<String, String>(topic, Integer.toString(i), Integer.toString(i));
            producer.send(record, new Callback() {
                //消息回调
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception!=null){
                        System.out.println("this message is send failed");
                    }else{
                        System.out.println("topic: "+metadata.topic()+" partition:"+metadata.partition()+" message is success");
                    }
                }
            });
        }

        producer.close();
    }
}
