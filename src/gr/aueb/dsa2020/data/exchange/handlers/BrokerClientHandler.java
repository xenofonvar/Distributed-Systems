package gr.aueb.dsa2020.data.exchange.handlers;

import gr.aueb.dsa2020.config.PublisherInfo;
import gr.aueb.dsa2020.data.exchange.ExchangeableMessage;
import gr.aueb.dsa2020.data.exchange.MP3ChunkDataExchanger;
import gr.aueb.dsa2020.data.mp3.MP3Chunk;
import gr.aueb.dsa2020.data.mp3.MP3SearchPair;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

/**
 * This class is a handler class for outgoing requests from Broker to Publisher. This class instances
 * requests the specific mp3 file from Publisher and handles the transmission of the mpe chunks.
 */
public class BrokerClientHandler implements Runnable {
    private MP3ChunkDataExchanger exchanger; // this thread is the writer of the data exchanger object
    private PublisherInfo publisher;
    private MP3SearchPair mp3Search;

    public BrokerClientHandler(PublisherInfo publisher, MP3SearchPair mp3Search, MP3ChunkDataExchanger exchanger){
        this.exchanger = exchanger; this.mp3Search = mp3Search; this.publisher = publisher;
    }

    @Override
    public void run() {
        Socket socket = new Socket();
        SocketAddress address = new InetSocketAddress(this.publisher.getIp(), this.publisher.getInport());
        try {
            socket.setSoTimeout(4000);
            socket.connect(address);

            if (socket.isBound() && socket.isConnected()) {
                // Send request to register
                ObjectOutputStream oos = new ObjectOutputStream( socket.getOutputStream() );
                ObjectInputStream ois = new ObjectInputStream( socket.getInputStream() );
                ExchangeableMessage message = new ExchangeableMessage();
                message .setExchangedObject( this.mp3Search  )
                        .setMessageType( ExchangeableMessage.Types.BP_SONG_REQUEST );
                oos.writeUnshared(message);
                oos.flush();
                message = (ExchangeableMessage) ois.readUnshared();
                if( message.getMessageType() == ExchangeableMessage.Types.GP_EXCHANGE_FAILED ){
                    System.out.println("[i]> Requested song doesn't exists");
                    ois.close(); oos.close();
                    throw new IOException("Publisher ["+this.publisher.getIp()+"] responsed with failure");
                } else if( message.getMessageType() == ExchangeableMessage.Types.PB_CHUNK_TRANSMISSION) {
                    System.out.printf("\tIncoming message is of type PB_CHUNK_TRANSMISSION\n");
                    MP3Chunk receivedChunk = (MP3Chunk) message.getExchangedObject();
                    int totalChunks = receivedChunk.getTotalPartitions();
                    this.exchanger.write(receivedChunk); // lock and write the chunk
                    if (totalChunks!=1) { // if has more than one chunk
                        for(int i=1; i<totalChunks; i++) {
                            message = (ExchangeableMessage) ois.readUnshared();
                            if(message.getMessageType() == ExchangeableMessage.Types.PB_CHUNK_TRANSMISSION) {
                                receivedChunk = (MP3Chunk) message.getExchangedObject();
                                while( ! exchanger.write(receivedChunk) ){
                                    // wait specific time and try again to write new data
                                    try { Thread.sleep(40); }
                                    catch (InterruptedException e) { e.printStackTrace(); }
                                }
                            }
                        }
                    }
                    ois.close(); oos.close();
                // in any other case throw an error for unknown incoming message type
                } else throw new IOException("Unknown Message Type");
            } else throw new IOException("Failed to bound to the publisher node");

        } catch ( IOException | ClassNotFoundException e) {
            System.err.printf("[x]> Error while trying to communicate with publisher [%s:%d] cause: %s\n",
                    this.publisher.getIp(),this.publisher.getInport(), e.getMessage());
            e.printStackTrace();
            // on error write an empty MP3Chunk so the reader can understand that the writer is encountered an error
            // lock and write the empty chunk
            while( !this.exchanger.write(new MP3Chunk(null,0,0)) ){
                // wait specific time and try again to write new data
                try { Thread.sleep(60); }
                catch (InterruptedException ie) { ie.printStackTrace(); }
            }
        } finally { try { socket.close(); } catch (IOException ex) { /* ignore */ } }

    }
}
