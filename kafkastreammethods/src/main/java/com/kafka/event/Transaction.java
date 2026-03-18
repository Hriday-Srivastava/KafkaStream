package com.kafka.event;

public record Transaction(
		String transactionId,
        String userId,
        double amount,
        String timestamp) {}


