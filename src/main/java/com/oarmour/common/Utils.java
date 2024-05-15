package com.oarmour.common;

public class Utils {
    //Check if the head and tail of two transactions are similar, check first 6 characters of the hash
    public static boolean headTailSimilar(String address1, String address2) {
        return address1.substring(0, 6).equals(address2.substring(0, 6)) && address1.substring(address1.length() - 6).equals(address2.substring(address2.length() - 6));
    }
}
