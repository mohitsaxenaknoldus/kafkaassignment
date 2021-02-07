package com.mohit;

import com.mohit.model.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.mohit.StudentSerialization");

        KafkaProducer<String, Student> kafkaProducer = new KafkaProducer(properties);
        Random random = new Random();
        int minAge = 18;
        int maxAge = 24;

        try {
            for (int index = 0; index < 10; index++) {
                Student studentObj = new Student(index, "Mohit Saxena", random.nextInt(maxAge - minAge + 1) + minAge, "MCA");
                kafkaProducer.send(new ProducerRecord<String, Student>("student", Integer.toString(index), studentObj));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }
}