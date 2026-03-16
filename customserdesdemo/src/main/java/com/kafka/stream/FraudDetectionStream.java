package com.kafka.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.kafka.event.Transaction;
import com.kafka.serdes.TransactionSerdes;

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

	    
	    var transactionSerde = new TransactionSerdes();

	    
	    KStream<String, Transaction> transactionsStream =
	            builder.stream("transactions", Consumed.with(Serdes.String(), transactionSerde));

	    
	    transactionsStream
	            .filter((key, tnx) -> tnx.amount() > 10000)
	            .peek((key, tx) ->
	                    System.out.println("⚠️ FRAUD ALERT for TRANSACTION: " + tx))
	            .to("fraud-alerts", Produced.with(Serdes.String(), transactionSerde));

	    // Step 4: Return the original stream (Spring Boot requires returning the stream bean)
	    return transactionsStream;
	}

}