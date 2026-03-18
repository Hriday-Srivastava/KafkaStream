package com.kafka.producer;

import java.util.List;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.event.Transaction;

@RestController
@RequestMapping("/transactions")
public class TransactionProducer {

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    
    public TransactionProducer(KafkaTemplate<String, Transaction> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public String send(@RequestBody List<Transaction> transactions) {

        transactions.forEach(transaction -> {
            kafkaTemplate.send(
                "transactions",
                transaction.transactionId(),
                transaction
            );
        });

        return "All transactions sent";
    }
}