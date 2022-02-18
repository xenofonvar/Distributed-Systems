package gr.aueb.dsa2020.data.exchange.handlers;

import gr.aueb.dsa2020.config.BrokerInfo;
import gr.aueb.dsa2020.data.exchange.ExchangeableMessage;
import gr.aueb.dsa2020.data.mp3.MP3Chunk;
import gr.aueb.dsa2020.data.mp3.MP3MusicFile;
import gr.aueb.dsa2020.data.mp3.MP3SearchPair;
import gr.aueb.dsa2020.nodes.PublisherNode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Hashtable;


/**
 * This class is a handler for requests coming from broker to calle publisher
 */
public class BrokerRequestHandler implements Runnable {
    private PublisherNode publisher; // the publisher node
    private Socket connection; //  the connection to handle

    public BrokerRequestHandler(PublisherNode publisher, Socket connection){
        this.publisher = publisher; this.connection = connection;
    }

    @Override
    public void run()
    {
        if( this.isClientAccepted(connection.getInetAddress()) ) {
            System.out.printf("\tA connection from %s broker is accepted\n", connection.getInetAddress().getHostAddress());
            try {
                ObjectInputStream ois = new ObjectInputStream( this.connection.getInputStream() );
                ObjectOutputStream oos = new ObjectOutputStream( this.connection.getOutputStream() );

                ExchangeableMessage message = (ExchangeableMessage) ois.readUnshared(); //ois.readObject(); // deserialization
                if(  message!=null && message.getMessageType() == ExchangeableMessage.Types.BP_SONG_REQUEST ) {
                    System.out.printf("\tA song request is requested\n");
                    MP3SearchPair pair = (MP3SearchPair)  message.getExchangedObject();
                    String artistTosearch = pair.getArtistName().toLowerCase().trim(); // artist to search
                    String songToSearch = pair.getSongTitle().toLowerCase().trim(); // song to search
                    System.out.printf("\tRequested song is: %s - %s \n", artistTosearch, songToSearch);
                    Hashtable<String, MP3MusicFile> songs = this.publisher.getDataset().get(artistTosearch);
                    if(songs == null){ // in case the given artist for search doesn't exists
                        message = new ExchangeableMessage(); // answer with failure
                        message.setMessageType(ExchangeableMessage.Types.PB_REQUESTED_ARTIST_NOT_FOUND);
                        oos.writeUnshared(message); oos.flush();
                        throw new  IOException("Requested search pair: "+pair.toString()+" doesn't exists.");
                    }
                    MP3MusicFile mp3file = songs.get(songToSearch);
                    if(mp3file==null){ // in case the given song not found
                        message = new ExchangeableMessage(); // answer with failure
                        message.setMessageType(ExchangeableMessage.Types.PB_REQUESTED_SONG_NOT_FOUND);
                        oos.writeUnshared(message); oos.flush();
                        throw new  IOException("Requested search pair: "+pair.toString()+" doesn't exists.");
                    } // ---- else the song exists ! ! ! ! -----
                    System.out.printf("\tRequested song is [ %s - %s ] is found\n", artistTosearch, songToSearch);
                    ArrayList<MP3Chunk> mp3chunks =  mp3file.createChunks(); // create chunks for the current mp3 song
                    int i= 0;
                    for(MP3Chunk mp3chunk : mp3chunks){ // for each chunk - send it to the requested broker!
                        message = new ExchangeableMessage();
                        message.setExchangedObject(mp3chunk).setMessageType(ExchangeableMessage.Types.PB_CHUNK_TRANSMISSION);
                        oos.writeUnshared(message);
                        System.out.printf("\t\t%d\t%d bytes\n", ++i, mp3chunk.getActualSize());
                    }
                    oos.flush();
//                    try { Thread.sleep(5000); } // sleep for 1000 milliseconds
//                    catch (InterruptedException e) { System.err.printf("[x]> Failed to sleep for unknown reason\n"); }
                    System.out.printf("\tChunks [%d] transmitted successfully to Broker\n", mp3chunks.get(0).getTotalPartitions());
                    ois.close(); oos.close();
                } else {
                    message = new ExchangeableMessage(); // answer with failure
                    message.setMessageType(ExchangeableMessage.Types.GP_EXCHANGE_FAILED);
                    oos.writeUnshared(message); oos.flush(); oos.close(); ois.close();
                    throw new IOException("Invalid type of incoming message type");
                }

            } catch (IOException | ClassNotFoundException e) {
                System.err.printf("[x]> Failed on connection with broker host: [ %s ] cause: %s\n",
                        connection.getInetAddress().getHostAddress(), e.getMessage() );
            }
            finally { try { connection.close(); } catch (IOException e) { /*IGNORE_IT*/ } }
        } else {
            try { connection.close(); } catch (IOException e) { /*IGNORE_IT*/ }
            System.err.printf("[!]> Requested connection from [%s] is rejected, cause host isn't a registered broker\n",
                    connection.getInetAddress().getHostAddress() );
        }
    }

    private boolean isClientAccepted(InetAddress address){ // is client a registered broker!!!
        for(BrokerInfo b : this.publisher.getConfig().getBrokers())
            if( b.getIp().equals(address.getHostAddress()) ) return true;
        return false;
    }
}
