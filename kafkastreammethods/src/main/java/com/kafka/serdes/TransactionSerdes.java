package com.kafka.serdes;

import org.apache.kafka.common.serialization.Serdes;
import com.kafka.event.Transaction;

public class TransactionSerdes extends Serdes.WrapperSerde<Transaction> {

	public TransactionSerdes() {
		super(new TransactionSerializer(), new TransactionDeserializer());
		
	}

}
