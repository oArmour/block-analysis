package com.oarmour.datasource;
import io.reactivex.disposables.Disposable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.methods.response.Transaction;
import org.web3j.protocol.http.HttpService;

public class EthereumSource extends RichSourceFunction<Transaction>  {
    Disposable disposable;
    Web3j web3j;
    Object waitLock;
    String rpcUrl;

    public EthereumSource() {
    }

    public EthereumSource(String rpcUrl) {
        this.rpcUrl = rpcUrl;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (this.rpcUrl != null) {
            web3j = Web3j.build(new HttpService(this.rpcUrl));
            return;
        }
        web3j = Web3j.build(new HttpService("https://little-few-paper.quiknode.pro/b5d1d2678912de9078cba3c29d6180a685732418/"));
    }

    @Override
    public void run(SourceContext<Transaction> sourceContext) throws InterruptedException {
        waitLock = new Object();
        disposable =  web3j.transactionFlowable().subscribe(
                tx -> {
                    sourceContext.collect(tx);
                },
                error -> {
                    System.out.println(error.getMessage());
                },
                () -> {
                    System.out.println("complete");
                }
        );

        while (!disposable.isDisposed()) {
            synchronized (waitLock) {
                waitLock.wait(100L);
            }
        }
    }

    @Override
    public void cancel() {
        if (disposable != null) {
            disposable.dispose();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (disposable != null) {
            disposable.dispose();
        }
    }
}