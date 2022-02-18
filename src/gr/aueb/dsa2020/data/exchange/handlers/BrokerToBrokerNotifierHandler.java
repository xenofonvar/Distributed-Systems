package gr.aueb.dsa2020.data.exchange.handlers;

import gr.aueb.dsa2020.config.BrokerInfo;
import gr.aueb.dsa2020.config.ConsumerInfo;
import gr.aueb.dsa2020.data.exchange.ExchangeableMessage;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

/**
 * This class implemets the notification to a single Broker for the existence of a new
 * consumer. So this class works as a client to another broker server (ServerSocket) sending
 * the appropriate information to notify him (the broker server) for the new consumer in the
 * neighbor (local network).
 */
public class BrokerToBrokerNotifierHandler implements Runnable {
    private BrokerInfo broker; // the broker to be notified
    private ConsumerInfo consumer; // the consumer for registration
    private Socket connection;

    public BrokerToBrokerNotifierHandler(BrokerInfo broker, ConsumerInfo consumer){
        this.broker = broker; this.consumer = consumer; this.connection = null;
    }

    @Override
    public void run() {
        try{
            this.connection = this.initializeConnection(this.broker);
            ObjectOutputStream oos = new ObjectOutputStream( this.connection.getOutputStream() );

            ExchangeableMessage message = new ExchangeableMessage();
            message.setMessageType(ExchangeableMessage.Types.BB_CONSUMER_REGISTRATION)
                    .setExchangedObject(this.consumer);
            oos.writeUnshared(message);
            oos.flush(); oos.close();
            System.out.printf("[i]> Broker [ %s ] notified successfully for the existence of consumer [ %s ]\n",
                    this.broker, this.consumer);
        } catch (IOException e) {
            System.err.printf("[x]> An error occurred will try notifying the broker [ %s ] for the existence of "+
                    "the consumer [ %s ], cause: %s\n", this.broker, this.consumer, e.getMessage());
        } finally {
            try{ this.connection.close(); }catch(IOException e){ /* IGNORE IT */ }
        }
    }

    private Socket initializeConnection(BrokerInfo broker) throws IOException {
        Socket socket = new Socket();
        SocketAddress address = new InetSocketAddress(broker.getIp(), broker.getInport());
        socket.connect(address);
        if (socket.isBound() && socket.isConnected()) return socket;
        else throw new IOException("Couldn't connect to broker: "+broker.toString());
    }
}
