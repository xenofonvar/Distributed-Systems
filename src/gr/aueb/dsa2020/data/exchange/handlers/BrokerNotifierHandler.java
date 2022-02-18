package gr.aueb.dsa2020.data.exchange.handlers;

import gr.aueb.dsa2020.config.BrokerInfo;
import gr.aueb.dsa2020.config.PublisherConfig;
import gr.aueb.dsa2020.data.exchange.ExchangeableMessage;
import gr.aueb.dsa2020.data.mp3.MP3MusicFile;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Hashtable;

/**
 * This class is used to notify a given broker for the existence of a publisher.
 * Actually it performs the registration of the calle Publisher to the caller
 * Broker. It transfers the information that calle Publisher is responsible for
 * as a serializeble object of type Hashtable< String, Hashtable< String, MP3MusicFile > >.
 */
public class BrokerNotifierHandler implements Runnable {
    private BrokerInfo broker; // the broker to be notified
    private Hashtable< String, Hashtable< String, MP3MusicFile > > dataset;
    private PublisherConfig config;

    public BrokerNotifierHandler(BrokerInfo broker,
                                 Hashtable< String, Hashtable< String, MP3MusicFile>> dataset,
                                 PublisherConfig config){
        this.broker = broker; this.dataset = dataset; this.config = config;
    }

    /**
     * First sends an ExchangeableMessage of Type PB_PUBLISHER_REGISTRATION_CONNECT_INFO including
     * a PublisherInfo Object with information of the current publisher and then after the successful
     * response of the broker sends an ExchangeableMessage of Type PB_PUBLISHER_REGISTRATION_ARTISTS_LIST
     * including a Hashtable< String, Hashtable< String, MP3MusicFile>> object which represents the
     * dataset with the songs.
     */
    @Override
    public void run() {
        Socket socket = new Socket();
        SocketAddress address = new InetSocketAddress(broker.getIp(), broker.getInport());
        try { Thread.sleep(200); } // sleep for 200 milliseconds so the publisher's server wakes up
        catch (InterruptedException e) { System.err.printf("[x]> Failed to sleep for unknown reason\n"); }
        try {
            socket.setSoTimeout(7000);
            socket.connect(address);
            if( socket.isBound() && socket.isConnected() )
            {
                ObjectOutputStream oos = new ObjectOutputStream( socket.getOutputStream() );
                ObjectInputStream ois = new ObjectInputStream( socket.getInputStream() );
                // Send request to register
                ExchangeableMessage message = new ExchangeableMessage();
                message.setExchangedObject( this.config.getPublisher() )
                       .setMessageType( ExchangeableMessage.Types.PB_PUBLISHER_REGISTRATION_CONNECT_INFO );
                oos.writeUnshared(message);
                oos.flush();
                // Get the response to request for registration
                message = (ExchangeableMessage) ois.readUnshared();
                if( message==null || message.getMessageType() != ExchangeableMessage.Types.GP_EXCHANGE_ACHIEVED)
                    throw new IOException("Failed to register connect info");
                // Send request to register the artists list
                message = new ExchangeableMessage();
                message.setExchangedObject( this.dataset )
                        .setMessageType(ExchangeableMessage.Types.PB_PUBLISHER_REGISTRATION_ARTISTS_LIST);
                oos.writeUnshared(message);
                oos.flush();
                // Get the response to request for registration
                message = (ExchangeableMessage) ois.readUnshared();
                if( message==null || message.getMessageType() != ExchangeableMessage.Types.GP_EXCHANGE_ACHIEVED)
                    throw new IOException("Failed to register artists list");

                ois.close(); oos.close();
                // print an informative massage to output that everything is OK
                System.out.printf("[i]> Publisher [ %s:%d ] is registered successfully on [%s] - [ %s:%d ]\n",
                        this.config.getPublisher().getIp(), this.config.getPublisher().getInport(),
                        this.broker.getName(), this.broker.getIp(), this.broker.getInport());
            } else  System.err.printf("[x]> Failed to bound to socket: %s\n", socket.toString()); // in case of error

        } catch (IOException | ClassNotFoundException ex) {
            System.err.printf("[x]> Failed to notifiy broker %s, cause the following error: %s\n", this.broker, ex.getMessage());
        } finally {
            // try to close the socket connection
            try { socket.close(); } catch (IOException ex) { /* ignore */ }
        }
    }
}
