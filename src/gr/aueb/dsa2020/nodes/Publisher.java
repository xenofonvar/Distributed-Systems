package gr.aueb.dsa2020.nodes;

public interface Publisher extends Node {
    boolean readConfig();
    boolean readDatasetPool();
    void notifyBrokers();
    void startServer();
}
