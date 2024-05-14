package com.oarmour.datasource;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.web3j.protocol.core.methods.response.Transaction;

public class MockSource {
    public static DataStream<Transaction> MockTransactions(StreamExecutionEnvironment env) {
        return env.fromData(
                SimpleTransaction("0x123", "0x456", "1000", "0x", "0x"),
                SimpleTransaction("0x456", "0x123", "1000", "0x","0x")
        );
    }

    public static Transaction SimpleTransaction(String from, String to, String value, String input, String creates) {
        Transaction tx = new Transaction();
        tx.setFrom(from);
        tx.setTo(to);
        tx.setValue(value);
        tx.setInput(input);
        tx.setCreates(creates);
        return tx;
    }

    public static String StringOfTX(Transaction tx) {
        return String.format("from: %s, to: %s, value: %s, input: %s", tx.getFrom(), tx.getTo(), tx.getValue(), tx.getInput());
    }
}
