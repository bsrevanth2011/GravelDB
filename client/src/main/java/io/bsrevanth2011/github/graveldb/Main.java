package io.bsrevanth2011.github.graveldb;

import io.bsrevanth2011.github.graveldb.client.SimpleBlockingDBClient;

public class Main {
    public static void main(String[] args) {
        SimpleBlockingDBClient client = new SimpleBlockingDBClient();
        client.put("hello", "world");
        client.put("hi", "revanth");
        client.put("henlo", "shriya");
        String val1 = client.get("hello");
        String val2 = client.get("hi");
        String val3 = client.get("henlo");
        client.delete("hi");
        String val4 = client.get("hi");
        System.out.println(val1);
        System.out.println(val2);
        System.out.println(val3);
        System.out.println(val4);
    }
}
