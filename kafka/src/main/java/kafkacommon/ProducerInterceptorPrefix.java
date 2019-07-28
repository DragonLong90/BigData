package kafkacommon;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Created by DRAGON on 2019/7/21.
 * 生产者拦截器
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String, String> {
    public volatile long sendSuccess = 0;
    public volatile long sendFailure = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue = "prefix1-" + record.value();
        return new ProducerRecord<String, String>(record.topic(), record.partition(), record.timestamp(), record.key(), modifiedValue, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception==null){
            sendSuccess++;
        }else {
            sendFailure++;
        }
    }

    @Override
    public void close() {
        double successRadio=(double)sendSuccess/(sendFailure+sendSuccess);
        System.out.println("[INFO]发送成功率："+String.format("%f",successRadio*100)+"%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
