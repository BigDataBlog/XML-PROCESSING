package org.thegreenseek.samples.kafka.xml;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by Macphil11 on 15/07/2016.
 */
public class KafkaStringProducer {
    private Producer<String,String> stringProducer;
    private Properties lProps ;

    public KafkaStringProducer(Properties props) {
        lProps = props;
    }

    public int sendRecord(String topic, String message) {
        stringProducer = new KafkaProducer(lProps);
        try {
            stringProducer.send(new ProducerRecord<String, String>(topic, message));
        } catch (Exception ex) {
            return -1;
        } finally {
            stringProducer.close();
        }
        return 0;
    }
}
