package com.oarmour.datasource;

import com.oarmour.pattern.PFishing;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import org.web3j.protocol.core.methods.response.Transaction;

import java.util.List;
import java.util.Map;

import static org.apache.flink.cep.CEP.pattern;

public class EthereumSourceTest {

    StreamExecutionEnvironment env;
    DataStream<Transaction> mockStream;
    @Before
    public void setUpClass() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        mockStream = env.fromData(
                MockSource.SimpleTransaction("0x123", "0x456", "1000", "0x",    "0x"),
                MockSource.SimpleTransaction("0x456", "0x123", "1000", "0x", "0x"  )
        );
    }

    @Test
    public void testConnectStream() throws Exception {
        DataStream keyedStream = mockStream.process(new ProcessFunction<Transaction, WrappedTransaction>() {
            @Override
            public void processElement(Transaction value, ProcessFunction<Transaction, WrappedTransaction>.Context ctx, Collector<WrappedTransaction> out) throws Exception {
                out.collect(WrappedTransaction.FromKey(value, value.getFrom()));
                out.collect(WrappedTransaction.FromKey(value, value.getTo()));
            }
        }).keyBy(tx -> tx.Key).process(new KeyedProcessFunction<String, WrappedTransaction, WrappedTransaction>() {
            @Override
            public void processElement(WrappedTransaction value, Context ctx, Collector<WrappedTransaction> out) throws Exception {
                out.collect(value);
            }
        });


        CEP.pattern(keyedStream, PFishing.getPattern()).select(new PatternSelectFunction<WrappedTransaction, String>() {
            @Override
            public String select(Map<String, List<WrappedTransaction>> pattern) throws Exception {
                return "Fishing Alert";
            }
        }).print();
        env.execute();

    }
}
