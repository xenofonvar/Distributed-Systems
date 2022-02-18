package gr.aueb.dsa2020.data.exchange.handlers;

import gr.aueb.dsa2020.config.BrokerInfo;
import gr.aueb.dsa2020.config.ConsumerInfo;
import gr.aueb.dsa2020.config.PublisherInfo;
import gr.aueb.dsa2020.data.exchange.ExchangeableMessage;
import gr.aueb.dsa2020.data.exchange.MP3ChunkDataExchanger;
import gr.aueb.dsa2020.data.mp3.MP3Chunk;
import gr.aueb.dsa2020.data.mp3.MP3MusicFile;
import gr.aueb.dsa2020.data.mp3.MP3SearchPair;
import gr.aueb.dsa2020.nodes.BrokerNode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Hashtable;

/**
 * A handler class for incoming requests to the broker
 */
public class RequestToBrokerHandler implements Runnable {
    private BrokerNode broker;
    private Socket connection;
    private MP3ChunkDataExchanger exchanger; // this is the writer

    public RequestToBrokerHandler(BrokerNode broker, Socket connection){
        this.exchanger = null;
        this.broker = broker; this.connection = connection;
    }

    @Override
    public void run() {
        try {
            String clientsIP = connection.getInetAddress().getHostAddress();
            System.out.printf("[i]> Incoming request from %s is accepted\n", clientsIP);
            ObjectInputStream ois = new ObjectInputStream( this.connection.getInputStream() );
            ObjectOutputStream oos = new ObjectOutputStream( this.connection.getOutputStream() );

            ExchangeableMessage message = (ExchangeableMessage) ois.readUnshared(); // deserialization

            //is a publisher's request to be registered
            if(  message.getMessageType() == ExchangeableMessage.Types.PB_PUBLISHER_REGISTRATION_CONNECT_INFO ){
                this.servePublishersRequestForRegistration(message, ois, oos, clientsIP);
            } else if( message.getMessageType() == ExchangeableMessage.Types.BB_CONSUMER_REGISTRATION ){
                ConsumerInfo consumer = (ConsumerInfo) message.getExchangedObject();
                System.out.printf("[i]> Notification from broker [ %s ]. New consumer [ %s ] appeared\n",
                        clientsIP, consumer);
                if( this.broker.registerConsumer(consumer) )
                    System.out.printf("[i]> New consumer [ %s ] registered successfully.\n", consumer);
                else  System.out.printf("[i]> Consumer [ %s ] is already registered.\n", consumer);
            }
            else if( message.getMessageType() == ExchangeableMessage.Types.CB_ARTISTS_TABLE_REQUEST ){
                ConsumerInfo consumer = new ConsumerInfo().setIp(clientsIP);
                this.registerConsumerAndNotifyTheNeighborhood(consumer, oos);
                System.out.printf("[i]> The incoming request from [ %s ] is a consumer's request for artists list.\n",
                        consumer);
                this.serveConsumersRequestForArtistsTable(message, ois, oos);
            }
            // is consumer's request for a song
            else if( message.getMessageType() == ExchangeableMessage.Types.CB_SONG_REQUEST ) {
                ConsumerInfo consumer = new ConsumerInfo().setIp(clientsIP);
                this.registerConsumerAndNotifyTheNeighborhood(consumer, oos);
                System.out.printf("\t[i]> A song request is came from consumer [ %s ]\n", consumer);
                MP3SearchPair mp3Search = (MP3SearchPair) message.getExchangedObject();
                BrokerInfo inChargeBroker = this.broker.findResponsibleBroker(mp3Search);
                // if in charge broker is not the current broker notify consumer who is the in charge broker
                message = new ExchangeableMessage(); // answer who is the in charge broker
                message.setMessageType(ExchangeableMessage.Types.BC_INCHARGE_BROKER);
                message.setExchangedObject(inChargeBroker);
                oos.writeUnshared(message); oos.flush();
                // print an information message for the broker incharge
                System.out.printf("\tIncharge broker: [ %s ] \tcurrent broker: [ %s ]\n"+
                                "\tIncharge broker's hashCode: [ %d ] \tcurrent broker's hashCode: [ %d ]\n",
                                inChargeBroker, this.broker.getConfig().getBroker(),
                                inChargeBroker.hashCode(), this.broker.getConfig().getBroker().hashCode());
                // if the current broker is in charge for the requested song then find the publisher
                // that has the requested song and begin the transmission inside method: serveConsumersRequestForSong
                if(inChargeBroker.hashCode() == this.broker.getConfig().getBroker().hashCode()){
                //if( inChargeBroker.equals(this.broker.getConfig().getBroker()) ) {
                    //else the current broker is responsible for the requested song
                    System.out.printf("\tThe current broker [ %s ] is in charge\n", inChargeBroker.toString());
                    PublisherInfo inChargePublisher = this.broker.getInChargePublisherOf(mp3Search);
                    if (inChargePublisher == null) { // there is no publisher providing that mp3, answer with failure
                        System.out.printf("\tThere is no publisher for the requested song [ %s ]\n", mp3Search.toString());
                        message = new ExchangeableMessage();
                        message.setMessageType(ExchangeableMessage.Types.BC_NO_RESULTS);
                        message.setExchangedObject(inChargeBroker);
                        oos.writeUnshared(message); oos.flush();
                    }
                    // else the song exists and call the publisher to get it!!!
                    else{
                        System.out.printf("\tThe incharge publisher is [ %s ]\n", inChargePublisher);
                        this.serveConsumersRequestForSong(inChargePublisher, mp3Search, ois, oos);
                    }
                }
            } else { // in any other case return a fail message to sender
                message = new ExchangeableMessage(); // answer with failure
                message.setMessageType(ExchangeableMessage.Types.GP_EXCHANGE_FAILED);
                oos.writeUnshared(message); oos.flush();
                System.err.printf("[x]> Error message type at incoming request from: %s\n", clientsIP);
            }

        } catch (IOException | ClassNotFoundException e) {
            System.err.printf("[x]> Failed to read the incoming data, cause: %s\n", e.getMessage());
            //e.printStackTrace();
        }
        finally {
            try { connection.close(); } catch (IOException e) { /*IGNORE_IT*/ }
        }
    }

    private void serveConsumersRequestForSong(PublisherInfo inChargePublisher,
                                              MP3SearchPair mp3Search,
                                              ObjectInputStream ois, // <--- it is not use to the current version
                                              ObjectOutputStream oos) throws IOException {
        this.exchanger = new MP3ChunkDataExchanger(); // create the thread safe data exchanger
        // start a thread to communicate with the publisher to get the chunks of the requested song
        new Thread( new BrokerClientHandler(inChargePublisher,mp3Search, this.exchanger)).start();
        try { Thread.sleep(100); } // sleep for 0.1 sec, give time to client to get the first data
        catch (InterruptedException e) { System.err.printf("[x]> Failed to sleep for unknown reason\n"); }
        // below this thread is the READER of the data exchanger object
        MP3Chunk receivedChunk;
        while( (receivedChunk = this.exchanger.read())==null ){
            System.out.printf("\t\t[READER] [x]> failed to read new data, try again later...\n");
            // wait specific time and try again to read for new data
            try { Thread.sleep(80); }
            catch (InterruptedException e) { e.printStackTrace(); }
        }
        ExchangeableMessage message;
        // if the mp3 is empty, that means that the writer encountered with and error/issue
        if(receivedChunk.isEmpty() || receivedChunk.getTotalPartitions()<=0){
            message = new ExchangeableMessage();
            message.setMessageType(ExchangeableMessage.Types.GP_EXCHANGE_FAILED);
            oos.writeUnshared(message); oos.flush();
            return;
        } // ----- else we have a chunk to send!!!!!!
        int totalChunks = receivedChunk.getTotalPartitions();

        message = new ExchangeableMessage();
        message.setMessageType(ExchangeableMessage.Types.BC_CHUNK_TRANSMISSION);
        message.setExchangedObject(receivedChunk);
        oos.writeUnshared(message); oos.flush();
        System.out.printf("\n\t>>>> Chunck 1 transmitted. [ %d/%d ] with size [ %d ]\n",
                receivedChunk.getPartitionNumber(), receivedChunk.getTotalPartitions(),
                receivedChunk.getPartition().length);
        if(totalChunks==1) return; // if we have only one chunk
        // ----- else we have more chunks to receive so we must send them !!!
        for(int i=1; i<totalChunks; i++) {
            while( (receivedChunk = this.exchanger.read())==null ) {
                System.out.printf("\t\t[READER] [x]> failed to read new data, try again later...\n");
                // wait specific time and try again to read for new data
                try { Thread.sleep(90); }
                catch (InterruptedException e) { e.printStackTrace(); }
            }
            // if the mp3 is empty, that means that the writer encountered with and error/issue
            if(receivedChunk.isEmpty()){
                message = new ExchangeableMessage();
                message.setMessageType(ExchangeableMessage.Types.GP_EXCHANGE_FAILED);
                oos.writeUnshared(message);
                return;
            } // ----- else we have a chunk to send!!!!!!
            message = new ExchangeableMessage();
            message.setMessageType(ExchangeableMessage.Types.BC_CHUNK_TRANSMISSION);
            message.setExchangedObject(receivedChunk);
            oos.writeUnshared(message); //oos.flush();
            try { Thread.sleep(50); } // sleep for 1.5 sec, give time to client to get the first data
            catch (InterruptedException e) { System.err.printf("[x]> Failed to sleep for unknown reason\n"); }
            System.out.printf("\t>>>> Chunck %d transmitted. [ %d/%d ] with size: [ %d ]\n",
                    (i+1),receivedChunk.getPartitionNumber(), receivedChunk.getTotalPartitions(),
                    receivedChunk.getPartition().length);
        }
        oos.flush();
//        try { Thread.sleep(100); } // sleep for 1.5 sec, give time to client to get the first data
//        catch (InterruptedException e) { System.err.printf("[x]> Failed to sleep for unknown reason\n"); }
        System.out.println("[i]> Transmission completed successfully");

    }

    private void servePublishersRequestForRegistration(ExchangeableMessage message,
                                                       ObjectInputStream ois,
                                                       ObjectOutputStream oos,
                                                       String clientsIP) throws IOException, ClassNotFoundException {
        PublisherInfo publisher = (PublisherInfo) message.getExchangedObject(); // store temporary the publisher
        // then answer with OK ! !
        message = new ExchangeableMessage(); // answer with succeed
        message.setMessageType(ExchangeableMessage.Types.GP_EXCHANGE_ACHIEVED);
        oos.writeUnshared(message); oos.flush();
        //System.out.println( "The incoming publisher is: " + publisher );

        // publisher the sends the list of the artists-songs
        message = (ExchangeableMessage) ois.readUnshared(); //ois.readObject(); // deserialization
        if( message.getMessageType() == ExchangeableMessage.Types.PB_PUBLISHER_REGISTRATION_ARTISTS_LIST ){
            Hashtable< String, Hashtable< String, MP3MusicFile>> dataset =
                    (Hashtable< String, Hashtable< String, MP3MusicFile>>) message.getExchangedObject();

            message = new ExchangeableMessage(); // answer with succeed
            message.setMessageType(ExchangeableMessage.Types.GP_EXCHANGE_ACHIEVED);
            oos.writeUnshared(message); oos.flush();

            if( this.broker.registerPublisher(publisher, dataset) )
                System.out.printf("[i]> The publisher node %s is registered successfully.\n", publisher.toString());
            else
                System.out.printf("[!]> The requested registration from publisher node %s has been refused, cause is already registered.\n", publisher.toString());


        } else {  // answer with failure !!!
            message = new ExchangeableMessage();
            message.setMessageType(ExchangeableMessage.Types.GP_EXCHANGE_FAILED);
            oos.writeUnshared(message); oos.flush();
            System.err.printf("[x]> Error message type at incoming request from: %s\n", clientsIP);
        }
    }

    private void serveConsumersRequestForArtistsTable(ExchangeableMessage message,
                                                      ObjectInputStream ois,
                                                      ObjectOutputStream oos)throws IOException {
        message.setMessageType(ExchangeableMessage.Types.BC_ARTISTS_TABLE_RESPONSE);
        message.setExchangedObject(this.broker.getArtistsHashtable());
        oos.writeUnshared(message);
        oos.flush();
    }

    /**
     * This function registers the consumer to the current broker and also to the brokers of the neighborhood.
     * Additionally notifies the consumer for the existence of the brokers in the neighborhood.
     * @param consumer the consumer to be registered and notified
     * @param oos the ObjectOutputStream of the socket connection to the consumer to notify him for the
     *            brokers in the neighborhood
     */
    private void registerConsumerAndNotifyTheNeighborhood(ConsumerInfo consumer, ObjectOutputStream oos){
        // if the consumer is a new one notify the brokers for the existence of the consumer and also notify the
        // consumer for the existence of all brokers in the neighborhood.
        if( this.broker.registerConsumer(consumer) ){
            for( BrokerInfo b : this.broker.getConfig().getNeighbors() )
                new Thread( new BrokerToBrokerNotifierHandler(b, consumer) ).start();
            ExchangeableMessage message = new ExchangeableMessage();
            // construct a list that includes the current broker.
            ArrayList<BrokerInfo> neighborhood = new ArrayList<>(this.broker.getConfig().getNeighbors());
            neighborhood.add(this.broker.getConfig().getBroker());
            message.setMessageType(ExchangeableMessage.Types.BC_BROKERS_LIST)
                    .setExchangedObject(neighborhood);
            try { // try to notify the consumer
                oos.writeUnshared(message);  oos.flush();
            } catch(IOException e){
                System.err.printf("[x]> Failed to notify the consumer [ %s ] for the brokers "+
                        "in the neighborhood\n", consumer);
            }
        } // else do not do anything
        else System.out.printf("[i]> Consumer [ %s ] is already registered\n", consumer);

    }

}
