package com.oarmour.cep;

import org.web3j.protocol.core.methods.response.Transaction;
public class MockSource {
    public static Transaction SimpleTransaction(String blockNumber, String from, String to, String value, String input, String creates) {
        Transaction tx = new Transaction();
        tx.setFrom(from);
        tx.setTo(to);
        tx.setValue(value);
        tx.setInput(input);
        tx.setCreates(creates);
        tx.setBlockNumber(blockNumber);
        return tx;
    }

    public static String StringOfTX(Transaction tx) {
        return String.format("from: %s, to: %s, value: %s, input: %s", tx.getFrom(), tx.getTo(), tx.getValue(), tx.getInput());
    }
}
