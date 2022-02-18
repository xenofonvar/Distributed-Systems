package gr.aueb.dsa2020.nodes;

public interface Broker extends Node {
    boolean readConfig();
    void startServer();

}
