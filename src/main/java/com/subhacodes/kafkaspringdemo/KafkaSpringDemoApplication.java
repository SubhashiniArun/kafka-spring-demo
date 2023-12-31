package com.subhacodes.kafkaspringdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaSpringDemoApplication {
	static final String TOPIC_TEST_1 = "test_1";
	static final String TOPIC_TEST_2 = "test_2";
	public static void main(String[] args) {
		SpringApplication.run(KafkaSpringDemoApplication.class, args);
	}

}
