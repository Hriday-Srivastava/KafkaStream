package com.kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.event.Transaction;

import tools.jackson.databind.ObjectMapper;

@RestController
@RequestMapping("/transactions")
public class TransactionProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper; 

    public TransactionProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
    }

    @PostMapping
    public String send(@RequestBody Transaction transaction) {
    	
    	String txnJson = mapper.writeValueAsString(transaction);

        kafkaTemplate.send("transactions", transaction.transactionId(), txnJson);

        return "Transaction sent";
    }
}