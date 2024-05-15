package com.oarmour.blockAnalysis.cep.datasource;

import org.web3j.protocol.core.methods.response.Transaction;

public class WrappedTransaction {
    public Transaction transaction;
    public String Key;
    public Long timestamp;

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

    public boolean isERC20TransferTo(String address) {
        if (!this.getTransaction().getInput().startsWith("0xa9059cbb")) {
            return false;
        }
        if (address.indexOf("0x") == 0) {
            address = address.substring(2, address.length());
        }
        if (this.transaction.getInput().indexOf(address) > 0) {
            return true;
        }
        return false;
    }

    public String toString() {
        return String.format("from: %s, to: %s, value: %s, input: %s createAt: %s", this.getTransaction().getFrom(),
                this.getTransaction().getTo(),
                this.getTransaction().getValue(),
                this.getTransaction().getInput(),
                this.getTransaction().getCreates());
    }

}
