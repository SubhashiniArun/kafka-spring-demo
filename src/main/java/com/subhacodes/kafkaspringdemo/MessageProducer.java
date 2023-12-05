package com.subhacodes.kafkaspringdemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class MessageProducer {

    public static final String TOPIC_NAME = "topic-1";

    private static Logger logger = LoggerFactory.getLogger(KafkaSpringDemoApplication.class);

    private final KafkaTemplate<String, String> template;

    public MessageProducer(KafkaTemplate<String, String> template){
        this.template = template;
    }

    public void send(String topic, String message){
        this.template.send(topic, message);
        logger.info("Sent Message: "+message+" to topic: "+topic);
    }
    
}


/* JAVA CLIENT API code
 * public static void main(String[] args){
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");

        Producer<Long, String> producer = new KafkaProducer<>(producerProperties);

        try{
            Long counter = 1L;
            while(true){
                Headers headers = new RecordHeaders();
                headers.add(new RecordHeader("header-1", "header-value-1".getBytes()));
                headers.add(new RecordHeader("header-2", "header-value-2".getBytes()));
                ProducerRecord<Long,String> record = new ProducerRecord<Long,String>(TOPIC_NAME, null, counter, "A sample message", headers);

                producer.send(record);
                System.out.println("Send Record#"+counter);
                counter++;
                TimeUnit.SECONDS.sleep(3);
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            producer.close();
        }
    }
 */