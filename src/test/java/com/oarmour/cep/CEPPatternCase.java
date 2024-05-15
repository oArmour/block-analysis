package com.oarmour.cep;

import com.oarmour.alert.Alert;
import com.oarmour.blockAnalysis.cep.datasource.EthereumSource;
import com.oarmour.blockAnalysis.cep.datasource.WrappedTransaction;
import com.oarmour.blockAnalysis.cep.pattern.PFishing;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.web3j.protocol.core.methods.response.Transaction;
import java.util.List;
import java.util.Map;

public class CEPPatternCase {
    StreamExecutionEnvironment env;
    DataStream<Transaction> mockStream;

    //by rpc provider
    EthereumSource onchainSource;

    @Before
    public void setUpClass() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        mockStream = env.fromData(
                MockSource.SimpleTransaction("1","0x1e227979f0b5bc691a70deaed2e0f39a6f538fd5", "0xd9a1b0b1e1ae382dbdc898ea68012ffcb2853a91", "50000000000000000", "0x",    "0x"),
                MockSource.SimpleTransaction("2","0xd9a1c3788d81257612e2581a6ea0ada244853a91", "0x1e227979f0b5bc691a70deaed2e0f39a6f538fd5", "0", "0x", "0x"  ),
                MockSource.SimpleTransaction("3","0x1e227979f0b5bc691a70deaed2e0f39a6f538fd5", "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", "0", "0xa9059cbb000000000000000000000000d9a1c3788d81257612e2581a6ea0ada244853a910000000000000000000000000000000000000000000000000000001ae60da1cf", "0x"  ),
                MockSource.SimpleTransaction("10","0x1e227979f0b5bc691a70deaed2e0f39a6f538fd5", "0x1e227979f0b5bc691a70deaed2e0000000000000", "0", "0x", "0x"  )
        );

        onchainSource = new EthereumSource();

    }

    @Test
    public void testFishingPattern() throws Exception {
        DataStream keyedStream = mockStream.process(new ProcessFunction<Transaction, WrappedTransaction>() {
            @Override
            public void processElement(Transaction value, ProcessFunction<Transaction, WrappedTransaction>.Context ctx, Collector<WrappedTransaction> out) throws Exception {
                WrappedTransaction wt1 = WrappedTransaction.FromKey(value, value.getFrom());
                WrappedTransaction wt2 = WrappedTransaction.FromKey(value, value.getTo());
                wt1.timestamp = value.getBlockNumber().longValue();
                wt2.timestamp = value.getBlockNumber().longValue();
                out.collect(wt1);
                out.collect(wt2);
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<WrappedTransaction>() {
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(WrappedTransaction lastElement, long extractedTimestamp) {
                return new Watermark(lastElement.timestamp - 5);
            }

            @Override
            public long extractTimestamp(WrappedTransaction element, long recordTimestamp) {
                return element.timestamp;
            }
        }).keyBy(tx -> tx.Key);

        CEP.pattern(keyedStream, PFishing.getPattern()).inEventTime().process(
                new PatternProcessFunction<WrappedTransaction, Alert>() {
                    @Override
                    public void processMatch(
                            Map<String, List<WrappedTransaction>> pattern,
                            Context ctx,
                            Collector<Alert> out) throws Exception {
                        System.out.println(pattern);
                        out.collect(new Alert());
                    }
                });

        env.execute("Fishing Scam Test");
    }

    @Test
    public void testCEPWithPattern() throws Exception {
        DataStream<Transaction> stream = env.addSource(onchainSource);

        DataStream keyedStream = stream.process(new ProcessFunction<Transaction, WrappedTransaction>() {
            @Override
            public void processElement(Transaction value, ProcessFunction<Transaction, WrappedTransaction>.Context ctx, Collector<WrappedTransaction> out) throws Exception {
                if (value.getTo() == null || value.getFrom() == null) return;
                WrappedTransaction wt1 = WrappedTransaction.FromKey(value, value.getFrom());
                WrappedTransaction wt2 = WrappedTransaction.FromKey(value, value.getTo());
                wt1.timestamp = value.getBlockNumber().longValue();
                wt2.timestamp = value.getBlockNumber().longValue();
                out.collect(wt1);
                out.collect(wt2);
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<WrappedTransaction>() {
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(WrappedTransaction lastElement, long extractedTimestamp) {
                return new Watermark(lastElement.timestamp - 5);
            }

            @Override
            public long extractTimestamp(WrappedTransaction element, long recordTimestamp) {
                return element.timestamp;
            }
        }).keyBy(tx -> tx.Key);

        CEP.pattern(keyedStream, PFishing.getPattern()).inEventTime().process(
                new PatternProcessFunction<WrappedTransaction, Alert>() {
                    @Override
                    public void processMatch(
                            Map<String, List<WrappedTransaction>> pattern,
                            Context ctx,
                            Collector<Alert> out) throws Exception {
                        System.out.println(pattern);
                        out.collect(new Alert());
                    }
                });

        env.execute("Fishing Scam Test");
    }
}
