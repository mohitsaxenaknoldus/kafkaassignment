package com.mohit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohit.model.Student;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;


public class StudentDeserialization implements Deserializer {
    @Override
    public void close() {
    }

    @Override
    public void configure(Map arg0, boolean arg1) {
    }

    @Override
    public Student deserialize(String arg0, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        Student user = null;
        try {
            user = mapper.readValue(arg1, Student.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return user;
    }
}