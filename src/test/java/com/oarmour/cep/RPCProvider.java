package com.oarmour.cep;

import io.reactivex.disposables.Disposable;
import org.junit.Before;
import org.junit.Test;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;

public class RPCProvider {
    Object waitLock;
    Disposable disposable;
    Web3j web3j;

    @Before
    public void setUpClass() throws Exception {
        web3j = Web3j.build(new HttpService("https://little-few-paper.quiknode.pro/b5d1d2678912de9078cba3c29d6180a685732418/"));
    }

    @Test
    public void testBlock() throws InterruptedException {
        waitLock = new Object();
        disposable =  web3j.blockFlowable(true).subscribe(
                blk -> {
                    System.out.println(blk.getBlock().getNumber());
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

    @Test
    public void testTransaction() throws InterruptedException {
        waitLock = new Object();
        disposable =  web3j.transactionFlowable().subscribe(
                tx -> {
                    System.out.println(tx.getFrom() + " -> " + tx.getTo() + " : " + tx.getValue());
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
}
