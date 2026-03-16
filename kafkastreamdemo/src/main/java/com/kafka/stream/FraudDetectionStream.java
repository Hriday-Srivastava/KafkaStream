package com.kafka.stream;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import com.kafka.event.Transaction;

import tools.jackson.databind.ObjectMapper;

@Configuration
@EnableKafkaStreams
public class FraudDetectionStream {

	//create bean
    //-> read the topic
    //-> process filter
    //-> write to dest
	private final ObjectMapper objectMapper;

    public FraudDetectionStream(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Bean
    public KStream<String, String> fraudDetectStream(StreamsBuilder builder) {

        // Step 1: Read messages from the input topic
        KStream<String, String> transactionsStream = builder.stream("transactions");
        
        // Step 2: Filter suspicious transactions
        KStream<String, String> fraudTransactionStream = transactionsStream
                .filter((key, value) -> isSuspicious(value))
                .peek((key, value) -> {
                    System.out.println(String.format("⚠️ FRAUD ALERT - transactionId=%s , value=%s", key, value));
                });

        // Step 3: Write suspicious transactions to another topic
        fraudTransactionStream.to("fraud-alerts");

        return transactionsStream;
    }

    private boolean isSuspicious(String value) {
        try {
            // Convert JSON string to Transaction object
            Transaction transaction = objectMapper.readValue(value, Transaction.class);

            // Simple fraud rule: amount > 10,000
            return transaction.amount() > 10000;
        } catch (Exception e) {
            // Invalid JSON, ignore
            return false;
        }
    }

}