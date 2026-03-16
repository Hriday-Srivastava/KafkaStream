package com.kafka.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.kafka.event.Transaction;

import tools.jackson.databind.ObjectMapper;

@Configuration
@EnableKafkaStreams
public class FraudDetectionStream {

	//create bean
    //-> read the topic
    //-> process filter
    //-> write to dest
	
  

	@Bean
	public KStream<String, Transaction> fraudDetectStream(StreamsBuilder builder) {

	    // Step 1: Create a JSON Serde for Transaction object
	    // This will serialize/deserialize JSON messages to/from Transaction class
	    var transactionSerde = new JsonSerde<>(Transaction.class);

	    // Step 2: Read messages from the "transactions" topic
	    // Key -> String
	    // Value -> Transaction object (converted from JSON using JsonSerde)
	    KStream<String, Transaction> transactionsStream =
	            builder.stream("transactions", Consumed.with(Serdes.String(), transactionSerde));

	    // Step 3: Process the stream
	    // Filter transactions where amount > 10000 (simple fraud detection rule)
	    // Log the suspicious transaction using peek()
	    // Then send the suspicious transactions to "fraud-alerts" topic
	    transactionsStream
	            .filter((key, tnx) -> tnx.amount() > 10000)
	            .peek((key, tx) ->
	                    System.out.println("⚠️ FRAUD ALERT for TRANSACTION: " + tx))
	            .to("fraud-alerts", Produced.with(Serdes.String(), transactionSerde));

	    // Step 4: Return the original stream (Spring Boot requires returning the stream bean)
	    return transactionsStream;
	}

}