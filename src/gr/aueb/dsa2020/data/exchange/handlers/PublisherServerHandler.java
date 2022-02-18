package gr.aueb.dsa2020.data.exchange.handlers;

import gr.aueb.dsa2020.nodes.PublisherNode;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class PublisherServerHandler implements Runnable {
    PublisherNode publisher;

    public PublisherServerHandler(PublisherNode publisher){ this.publisher = publisher; }

    @Override
    public void run() {
        InetAddress serverAddress = null;
        try { serverAddress = InetAddress.getByName(this.publisher.getConfig().getIp());
        } catch (UnknownHostException e) {
            System.err.printf("[x]> Failed to start Publisher Server %s, cause the following error: %s\n",
                    this.publisher.getConfig().getIp(), e.getMessage());
            System.exit(1);
        }

        try ( ServerSocket server = new ServerSocket(this.publisher.getConfig().getInport(), 50, serverAddress) ) {
            System.out.printf("[i]> -|-|- Publisher Server %s start successfully on [%s:%d] and is waiting requests -|-|-\n",
                this.publisher.getConfig().getName(), this.publisher.getConfig().getIp(), this.publisher.getConfig().getInport());
            while (true) { // ! ! ! RUN FOR EVER AS A SERVER ! ! !
                try {
                    Socket connection = server.accept(); // <----- ACCEPT CONNECTIONS
                    new Thread(new BrokerRequestHandler(this.publisher, connection)).start();
                } catch (IOException ex) { /*IGNORE*/}
            }

        } catch (IOException e) {
            System.err.printf("[x]> Failed to start Publisher Server %s, cause the following error: %s\n",
                    this.publisher.getConfig().getIp(), e.getMessage());
        }

    }
}
