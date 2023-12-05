package com.subhacodes.kafkaspringdemo;

import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import static com.subhacodes.kafkaspringdemo.KafkaSpringDemoApplication.TOPIC_TEST_1;
import static com.subhacodes.kafkaspringdemo.KafkaSpringDemoApplication.TOPIC_TEST_2;

@Component
public class MessageConsumer {

    private static Logger logger = LoggerFactory.getLogger(KafkaSpringDemoApplication.class);

    private CountDownLatch latch = new CountDownLatch(1);

    public CountDownLatch geLatch(){
        return latch;
    }

    @KafkaListener(topics = TOPIC_TEST_1)
    @SendTo(TOPIC_TEST_2)
    public String listen(ConsumerRecord<?, ?> record){
        logger.info(TOPIC_TEST_1+" Received: "+record.toString());
        return String.valueOf(record.value());
    }

    @KafkaListener(topics = TOPIC_TEST_2)
    public void listenTopic2(ConsumerRecord<?,?> record){
        logger.info(TOPIC_TEST_2, "Received: "+record.toString());
        latch.countDown();
    }

}

/* JAVA CLIENT API code
 * public static final String TOPIC_NAME = "topic-1";
    public static void main(String[] args){
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-group");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        

        Consumer<Long, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        try{
            while(true){
                ConsumerRecords<Long, String> records=consumer.poll(Duration.ofMillis(3000));
                System.out.println("Fetched: "+records.count()+" records");
                for(ConsumerRecord<Long, String> record:records){
                    System.out.println("Received: "+record.key()+" : "+record.value());
                    record.headers().forEach(header -> {
                        System.out.println("Header key: "+header.key()+", value: "+header.value());
                    });
                }
                consumer.commitSync();
                TimeUnit.SECONDS.sleep(5);
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            consumer.close();
        }
    }
 */
