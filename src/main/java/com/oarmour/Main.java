package com.oarmour;

import com.oarmour.datasource.EthereumSource;
import com.oarmour.datasource.WrappedTransaction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.web3j.protocol.core.methods.response.Transaction;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EthereumSource source = new EthereumSource();

        DataStream<Transaction> stream = env.addSource(source);
        DataStream keyedStream = stream.process(new ProcessFunction<Transaction, WrappedTransaction>() {
            @Override
            public void processElement(Transaction value, ProcessFunction<Transaction, WrappedTransaction>.Context ctx, Collector<WrappedTransaction> out) throws Exception {
                out.collect(WrappedTransaction.FromKey(value, value.getFrom()));
                out.collect(WrappedTransaction.FromKey(value, value.getTo()));
            }
        }).keyBy(tx -> tx.Key).process(new KeyedProcessFunction<String, WrappedTransaction, Object>() {
            @Override
            public void processElement(WrappedTransaction value, Context ctx, Collector<Object> out) throws Exception {
                out.collect(value);
                System.out.println(value);
            }
        });
        env.execute();
    }
}