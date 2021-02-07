package com.mohit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohit.model.Student;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.io.FileWriter;
import java.io.IOException;

public class Consumer {

    public static void main(String[] args) {
        ConsumerListener c = new ConsumerListener();
        Thread thread = new Thread(c);
        thread.start();
    }

    public static void consumer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");

        KafkaConsumer<String, Student> kafkaConsumer = new KafkaConsumer(properties);
        List topics = new ArrayList();

        topics.add("student");
        kafkaConsumer.subscribe(topics);

        try {
            while (true) {
                FileWriter fileWriter = new FileWriter("output.txt", true);
                ConsumerRecords<String, Student> records = kafkaConsumer.poll(10);
                ObjectMapper mapper = new ObjectMapper();
                for (ConsumerRecord<String, Student> record : records) {
                    fileWriter.append(mapper.writeValueAsString(record.value()) + "\n");
                }
                fileWriter.close();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }
}

class ConsumerListener implements Runnable {


    @Override
    public void run() {
        Consumer.consumer();
    }
}