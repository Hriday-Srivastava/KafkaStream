package com.kafka.serdes;

import com.kafka.event.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class TransactionDeserializer implements Deserializer<Transaction> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    
    @Override
    public Transaction deserialize(String topic, byte[] data) {
        try {
            if (data == null || data.length == 0) return null;
            return objectMapper.readValue(data, Transaction.class);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing Transaction", e);
        }
    }

    
}