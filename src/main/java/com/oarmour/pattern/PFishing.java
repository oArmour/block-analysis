package com.oarmour.pattern;

import com.oarmour.datasource.WrappedTransaction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

public class PFishing {
    public static Pattern getPattern() {
        return Pattern.<WrappedTransaction>begin("token-send")
                .where(new IterativeCondition<WrappedTransaction>() {
                    @Override
                    public boolean filter(WrappedTransaction value, Context<WrappedTransaction> ctx) throws Exception {
                        if (value.getTransaction().getValue().longValue() > 1000L)
                            return true;
                        return false;
                    }
                }).followedBy("poisoning-address").where(new IterativeCondition<WrappedTransaction>() {
                    @Override
                    public boolean filter(WrappedTransaction value, Context<WrappedTransaction> ctx) throws Exception {
                        if (value.transaction.getTo().equals("0x123") && value.getTransaction().getValue().longValue() == 0)
                            return true;
                        return false;
                    }
                }).followedBy("fishing-success").where(new IterativeCondition<WrappedTransaction>() {
                    @Override
                    public boolean filter(WrappedTransaction value, Context<WrappedTransaction> ctx) throws Exception {
                        return false;
                    }
                });
    }
}
