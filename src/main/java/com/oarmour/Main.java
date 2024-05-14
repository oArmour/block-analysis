package com.oarmour;

import com.oarmour.datasource.EthereumSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.web3j.protocol.core.methods.response.Transaction;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Create an instance of EthereumSource
        EthereumSource source = new EthereumSource();

        // Create a DataStream using the EthereumSource
        DataStream<Transaction> stream = env.addSource(source);
        stream.process(new ProcessFunction<Transaction, Object>() {
            @Override
            public void processElement(Transaction value, ProcessFunction<Transaction, Object>.Context ctx, Collector<Object> out) throws Exception {
                System.out.println(value.getFrom() + " -> " + value.getTo() + " : " + value.getValue());
            }
        }).print();
        env.execute();
    }
}