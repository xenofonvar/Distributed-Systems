package gr.aueb.dsa2020.data.exchange.handlers;

import gr.aueb.dsa2020.nodes.BrokerNode;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class BrokerServerHandler implements Runnable {
    BrokerNode broker;

    public BrokerServerHandler(BrokerNode broker) {
        this.broker = broker;
    }

    @Override
    public void run() {
        InetAddress serverAddress = null;
        try {
            serverAddress = InetAddress.getByName(this.broker.getConfig().getIp());
        } catch (UnknownHostException e) {
            System.err.printf("[x]> Failed to start Broker Server %s, cause the following error: %s\n",
                    this.broker.getConfig().getIp(), e.getMessage());
            System.exit(1);
        }

        try (ServerSocket server = new ServerSocket(this.broker.getConfig().getInport(), 50, serverAddress)) {
            System.out.printf("[i]> -|-|- Broker Server %s started successfully on [%s:%d] and is waiting requests -|-|-\n",
                    this.broker.getConfig().getName(), this.broker.getConfig().getIp(), this.broker.getConfig().getInport());
            while (true) { // ! ! ! RUN FOR EVER AS A SERVER ! ! !
                try {
                    Socket connection = server.accept(); // <----- ACCEPT CONNECTIONS
                    // The connection will be served from the following thread!!
                    new Thread( new RequestToBrokerHandler(this.broker, connection) ).start();
                } catch (IOException ex) {
                    System.err.printf("[x]> An error occurred while trying to accept an incoming connection, cause: %\n",
                        ex.getMessage());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}