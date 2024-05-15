package com.oarmour.blockAnalysis.cep.pattern;

import com.oarmour.blockAnalysis.cep.datasource.WrappedTransaction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

import static com.oarmour.common.Utils.headTailSimilar;

/*
 * Attack steps:
 * 1.victim address1 --> correct address2 with value > 1000
 * 2.scammer poisoned address3 --> address victim address1 with value = 0, and poisoned address3 has the same head and tail as address1.
 * 3.victim address1 --> address3 with value > 1000, phishing attack successful.
 */
public class PFishing {
    public static Pattern getPattern() {
        return Pattern.<WrappedTransaction>begin("discovery").where(new IterativeCondition<WrappedTransaction>() {
            @Override
            public boolean filter(WrappedTransaction value, Context<WrappedTransaction> ctx) throws Exception {
                return value.getTransaction().getValue().longValue() > 10000000000000000L;
            }
        }).followedBy("poisoning").where(new IterativeCondition<WrappedTransaction>() {
            @Override
            public boolean filter(WrappedTransaction currTx, Context<WrappedTransaction> ctx) throws Exception {
                Iterable<WrappedTransaction> txs = ctx.getEventsForPattern("discovery");
                for (WrappedTransaction tx : txs) {
                    if (tx.getTransaction().getFrom().equals(currTx.getTransaction().getTo()) && currTx.getTransaction().getValue().longValue() == 0 && headTailSimilar(tx.getTransaction().getFrom(), currTx.getTransaction().getTo())) {
                        return true;
                    }
                }
                return false;
            }
        }).followedBy("success").where(new IterativeCondition<WrappedTransaction>() {
            @Override
            public boolean filter(WrappedTransaction currTx, Context<WrappedTransaction> ctx) throws Exception {
                Iterable<WrappedTransaction> txs = ctx.getEventsForPattern("poisoning");
                for (WrappedTransaction tx : txs) {
                    if (tx.getTransaction().getFrom().equals(currTx.getTransaction().getTo()) && tx.getTransaction().getTo().equals(currTx.getTransaction().getFrom()) && tx.getTransaction().getValue().longValue() > 10000000000000000L) {
                        return true;
                    }
                    if (currTx.getTransaction().getFrom().equals(tx.getTransaction().getTo()) && currTx.isERC20TransferTo(tx.getTransaction().getFrom())) {
                        return true;
                    }
                }
                return false;
            }
        });   //10 minutes window;
    }


}

