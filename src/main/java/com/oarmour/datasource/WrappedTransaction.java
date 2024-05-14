package com.oarmour.datasource;

import org.web3j.protocol.core.methods.response.Transaction;

public class WrappedTransaction {
    public Transaction transaction;
    public String Key;

    public WrappedTransaction() {
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public WrappedTransaction(Transaction transaction, String Key) {
        this.transaction = transaction;
        this.Key = Key;
    }
    public static WrappedTransaction FromKey(Transaction transaction, String key) {
        return new WrappedTransaction(transaction, key);
    }

    public String toString() {
        return String.format("from: %s, to: %s, value: %s, input: %s createAt: %s", this.getTransaction().getFrom(), this.getTransaction().getTo(), this.getTransaction().getValue(), this.getTransaction().getInput(), this.getTransaction().getCreates());
    }

}
