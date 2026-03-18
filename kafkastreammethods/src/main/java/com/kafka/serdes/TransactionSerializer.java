package com.kafka.serdes;

import com.kafka.event.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;


public class TransactionSerializer implements Serializer<Transaction> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    
    @Override
    public byte[] serialize(String topic, Transaction data) {
        try {
            if (data == null) return null;
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing Transaction", e);
        }
    }

    
}