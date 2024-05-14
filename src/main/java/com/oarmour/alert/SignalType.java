package com.oarmour.alert;


public enum SignalType {
    SIGNAL_TOKEN_TRANSFER("Signal token transfer"),
    SIGNAL_SUSPICIOUS_TRANSFER("Signal Suspicious Transfer");

    private final String signalValue;

    SignalType(String signalValue) {
        this.signalValue = signalValue;
    }

    public String getSignalValue() {
        return signalValue;
    }
}