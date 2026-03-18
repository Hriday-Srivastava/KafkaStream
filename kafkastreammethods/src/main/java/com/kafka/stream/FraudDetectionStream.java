package com.kafka.stream;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.apache.kafka.streams.KeyValue;

import com.kafka.event.Transaction;
import com.kafka.serdes.TransactionSerdes;

@Configuration
@EnableKafkaStreams
public class FraudDetectionStream {

	//create bean
    //-> read the topic
    //-> process filter
    //-> write to dest
	
  

	@Bean
	public KStream<String, Transaction> windowedTransactionStream(StreamsBuilder builder) {

	    var transactionSerde = new TransactionSerdes();

	    // Step 1: Read transactions topic
	    KStream<String, Transaction> transactionsStream =
	            builder.stream("transactions",
	                    Consumed.with(Serdes.String(), transactionSerde));

	    // Step 2: Group by userId (important for counting per user)
	    KGroupedStream<String, Transaction> groupedStream =
	            transactionsStream.groupBy(
	                    (key, txn) -> txn.userId(),
	                    Grouped.with(Serdes.String(), transactionSerde)
	            );

	    // Step 3: Apply 1-minute window and count transactions
	    KTable<Windowed<String>, Long> transactionCounts =
	            groupedStream
	                    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
	                    .count(Materialized.as("txn-count-store"));

	    // Step 4: Convert KTable to KStream and filter suspicious activity
	    KStream<String, Long> suspiciousUsers =
	            transactionCounts
	                    .toStream()
	                    .filter((windowedKey, count) -> count >= 4) // 🔥 Rule: 4 transactions
	                    .peek((windowedKey, count) -> {
	                        System.out.println("⚠️ HIGH FREQUENCY FRAUD ALERT: User="
	                                + windowedKey.key()
	                                + " Count=" + count
	                                + " Window=" + windowedKey.window());
	                    })
	                    .map((windowedKey, count) ->
	                            KeyValue.pair(windowedKey.key(), count));

	    // Step 5: Send alert to Kafka topic
	    suspiciousUsers.to("fraud-alerts",
	            Produced.with(Serdes.String(), Serdes.Long()));

	    return transactionsStream;
	}

}