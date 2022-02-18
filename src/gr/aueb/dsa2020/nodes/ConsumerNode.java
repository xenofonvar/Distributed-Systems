package gr.aueb.dsa2020.nodes;

import gr.aueb.dsa2020.config.BrokerInfo;
import gr.aueb.dsa2020.data.exchange.ExchangeableMessage;
import gr.aueb.dsa2020.data.mp3.MP3Chunk;
import gr.aueb.dsa2020.data.mp3.MP3SearchPair;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;

public class ConsumerNode implements Consumer {
    private Hashtable<String, BrokerInfo> brokers; // Example <key, value> = <"ip:port", brokerInfoObject>
    private Mode mode;
    private String workingDir;

    public ConsumerNode(Mode mode){
        this.mode = mode;
        this.brokers = new Hashtable<>();
        this.workingDir = ConsumerNode.DEFAULT_WORKING_DIRECTORY;
    }

    public ConsumerNode(){ this(Mode.ONLINE); }

    private Socket initializeConnection(BrokerInfo broker) throws IOException {
        Socket socket = new Socket();
        SocketAddress address = new InetSocketAddress(broker.getIp(), broker.getInport());
        socket.connect(address);
        if (socket.isBound() && socket.isConnected()){
            System.out.printf("\tConnection to broker [ %s ] initialized successfully\n",broker);
            return socket;
        }
        else throw new IOException("Couldn't connect to broker: "+broker.toString());
    }


    private ArrayList<MP3Chunk> request(MP3SearchPair mp3, Socket socket, BrokerInfo broker)
                                                    throws ClassNotFoundException, IOException {
        if(mp3==null || socket==null || !(socket.isBound() && socket.isConnected()) )
            throw new IOException("Failed to perform the request to the broker");
        System.out.printf("\tConnection to broker [ %s ] is established\n", broker);
        // Send request to register
        ObjectOutputStream oos = new ObjectOutputStream( socket.getOutputStream() );
        ObjectInputStream ois = new ObjectInputStream( socket.getInputStream() );
        ExchangeableMessage message = new ExchangeableMessage();
        message .setExchangedObject( mp3 )
                .setMessageType( ExchangeableMessage.Types.CB_SONG_REQUEST );
        oos.writeUnshared(message);
        oos.flush();
        // read from socket the response
        message = (ExchangeableMessage) ois.readUnshared();
        // if is the first communication between current consumer and broker, then the tries
        // to register the consumer and notify the other brokers in the neighborhood for the
        // existence of the current consumer.
        if( message.getMessageType() == ExchangeableMessage.Types.BC_BROKERS_LIST ){
            ArrayList<BrokerInfo> brokers = (ArrayList<BrokerInfo>) message.getExchangedObject();
            for(BrokerInfo b : brokers )
                if(this.registerBroker(b))
                    System.out.printf("[i]> Broker [ %s ] is new and is registered\n", b);
            // read from socket the next message from the broker
            message = (ExchangeableMessage) ois.readUnshared();
        }

        if( message.getMessageType() == ExchangeableMessage.Types.BC_INCHARGE_BROKER ){
            BrokerInfo inChargeBroker = (BrokerInfo) message.getExchangedObject();
            //if( !inChargeBroker.getIp().equals(ConsumerClient.brokerIp) ){
//            if( inChargeBroker.hashCode() = ConsumerClient.brokerIp.hashCode() ){
//            if( !inChargeBroker.equals(broker) ){
            if( !(inChargeBroker.getIp().equals(broker.getIp()) && inChargeBroker.getInport()==broker.getInport()) ){
                System.out.println("[!]> Current connection is going to close cause another broker is in charge: "+
                        inChargeBroker.toString());
                // try to close the connection cause the another broker is in charge for that mp3 search
                oos.close(); ois.close();
                try { socket.close(); } catch (IOException ex) { /* ignore */ }
                // perform the new request to the right broker
                return performRequestFor(inChargeBroker, mp3);
            } else {
                System.out.printf("[i]> The initial broker [ %s ] is in charge.\n",broker);
            }
        } else if( message.getMessageType() == ExchangeableMessage.Types.GP_EXCHANGE_FAILED ){
            System.out.println("[x]> Connection is going to close cause broker encountered with an failure...");
            oos.close(); ois.close();
            try { socket.close(); } catch (IOException ex) { /* ignore */ }
            return null;
        } else {
            System.out.println("[x]> Connection is going to close cause broker encountered with an failure...");
            oos.close(); ois.close();
            try { socket.close(); } catch (IOException ex) { /* ignore */ }
            return null;
        }
        // ---------- else ----------- MAYBE WE HAVE A CHUNK TRANSMISSION --------------
        // ---the current broker is in charge and is going to start transmitting the chunks!!!
        message = (ExchangeableMessage) ois.readUnshared(); // read the next object
        // initialize the structure to store chunks if we have chunks that are transmitted
        ArrayList<MP3Chunk> receivedChunks = new ArrayList<>();
        if( message.getMessageType() == ExchangeableMessage.Types.BC_NO_RESULTS ){
            System.out.println("[x]> Connection is going to close cause broker doens't have that song");
            oos.close(); ois.close();
            try { socket.close(); } catch (IOException ex) { /* ignore */ }
            return null;
        } else if( message.getMessageType() == ExchangeableMessage.Types.GP_EXCHANGE_FAILED ){
            System.out.println("[x]> Connection is going to close cause broker encountered with an failure...");
            oos.close(); ois.close();
            try { socket.close(); } catch (IOException ex) { /* ignore */ }
            return null;
        }
        ///// BELOW WE RECEIVE THE FIRST CHUNK SEPARATELY CAUSE WE NEED TO READ THE METADATA TRANSFERRED WITH IT
        ///// AND ACTUAL WE NEED TO READ THE totalPartitions FROM WHICH CONSISTS THE TRANSMISSION.
        else if( message.getMessageType() == ExchangeableMessage.Types.BC_CHUNK_TRANSMISSION ){
            receivedChunks.add( (MP3Chunk) message.getExchangedObject() ); // add the first received chunk to the list
            System.out.printf( "\tThe requested song will be transferred in [ %d ] chunks.\n", receivedChunks.get(0).getTotalPartitions() );
            System.out.printf( "\t\tpartition received: [%d] %d/%d\n", receivedChunks.get(0).getPartition().length,
                    receivedChunks.get(0).getPartitionNumber(), receivedChunks.get(0).getTotalPartitions());
        } else { // in any other case we have error
            System.out.println("[x]> Connection is going to close cause broker encountered with an failure...");
            oos.close(); ois.close();
            try { socket.close(); } catch (IOException ex) { /* ignore */ }
            return null;
        }
        // get and store the totalPartitions metadata information
        int totalPartitions = receivedChunks.get(0).getTotalPartitions();
        // this loop, bellow, will be executed if and only if the transmission has more than one chunk
        for(int i=1; i<totalPartitions; i++){
            //System.out.print("Loop");
            message = (ExchangeableMessage) ois.readUnshared(); // read the next object
            if( message.getMessageType() == ExchangeableMessage.Types.BC_CHUNK_TRANSMISSION ){
                receivedChunks.add( (MP3Chunk) message.getExchangedObject() );
            }else if(message.getMessageType() == ExchangeableMessage.Types.GP_EXCHANGE_FAILED){
                System.out.println("[x]> Connection is going to close cause broker"+
                        " encountered with a failure, received until now: " + receivedChunks.size()+" chunks");
                oos.close(); ois.close();
                try { socket.close(); } catch (IOException ex) { /* ignore */ }
                return null;
            } else {
                System.out.println("[x]> Unknown message code is received from the broker :"+message.getMessageType());
                System.out.println("[x]> Connection is going to close cause the previous error...");
                oos.close(); ois.close();
                try { socket.close(); } catch (IOException ex) { /* ignore */ }
            }
            System.out.printf( "\t\tpartition received: [%d] %d/%d\n", receivedChunks.get(i).getPartition().length,
                    receivedChunks.get(i).getPartitionNumber(), receivedChunks.get(i).getTotalPartitions());
        }
        System.out.printf("[i]> Received successfully %d chunks of the requested song\n",totalPartitions);
        return receivedChunks; // finally return the received chunks
    }

    public void writeChunks(ArrayList<MP3Chunk> chunks) throws IOException {
        BufferedOutputStream bos;
        if(this.mode == Mode.ONLINE){
             for(MP3Chunk chunk : chunks){
                bos = new BufferedOutputStream( new FileOutputStream(new File(
            this.workingDir+"/"+chunk.getArtistName()+"-"+chunk.getTitle()+
                    "-"+chunk.getPartitionNumber()+"-"+chunk.getTotalPartitions()+".mp3"
                )));
                bos.write(chunk.getPartition()); bos.flush(); bos.close();

             }
            System.out.printf("[i]> The chunks of received song [ %s - %s ]  written successfully to %s\n",
                    chunks.get(0).getArtistName(), chunks.get(0).getTitle(), this.workingDir );
        } else { // offline mode
            bos = new BufferedOutputStream( new FileOutputStream(new File(
                    this.workingDir+"/"+chunks.get(0).getArtistName()+"-"+chunks.get(0).getTitle()+".mp3"
            )));
            for(MP3Chunk chunk: chunks) bos.write(chunk.getPartition());
            bos.flush(); bos.close();
            System.out.printf("[i]> The received song [ %s - %s ]  written successfully to %s\n",
                    chunks.get(0).getArtistName(), chunks.get(0).getTitle(), this.workingDir );
        }

    }

    public Mode getMode() { return mode; }
    public String getWorkingDir(){ return this.workingDir; }

    public ConsumerNode setMode(Mode mode) { this.mode = mode; return this;}
    public ConsumerNode setWorkingDir(String workingDir){ this.workingDir = workingDir; return this; }

    @Override
    public boolean init() { return true; }

    @Override
    public ArrayList<MP3Chunk> performRequestFor(BrokerInfo broker, MP3SearchPair mp3)
                                                throws IOException, ClassNotFoundException {
        System.out.printf("[i]> A request to broker [ %s ] for song [ %s ] will performed.\n", broker, mp3);
        System.out.printf("[i]> Try to initialize connection with broker [ %s ]...\n",broker);
        Socket socket = this.initializeConnection(broker);
        return this.request(mp3, socket, broker);
    }

    public Hashtable<String, ArrayList<String>> requestArtistsTable(BrokerInfo broker)
                                                throws IOException, ClassNotFoundException {
        System.out.printf("[i]> Try to initialize connection with [ %s ]...\n",broker);
        Socket socket = this.initializeConnection(broker);
        // the streams of the bound socket
        ObjectOutputStream oos = new ObjectOutputStream( socket.getOutputStream() );
        ObjectInputStream ois = new ObjectInputStream( socket.getInputStream() );
        // create the request message
        ExchangeableMessage message = new ExchangeableMessage();
        message.setMessageType(ExchangeableMessage.Types.CB_ARTISTS_TABLE_REQUEST);
        // Send request to register
        oos.writeUnshared(message);
        oos.flush();
        // try to get the response
        message = (ExchangeableMessage) ois.readUnshared();
        // check if the brokers tries to notify the consumer for the neighbor brokers
        if( message.getMessageType() == ExchangeableMessage.Types.BC_BROKERS_LIST ){
            ArrayList<BrokerInfo> brokers = (ArrayList<BrokerInfo>) message.getExchangedObject();
            for(BrokerInfo b : brokers )
                if(this.registerBroker(b))
                    System.out.printf("[i]> Broker [ %s ] is new and is registered\n", b);
            // read from socket the next message from the broker
            message = (ExchangeableMessage) ois.readUnshared();
        }
        if(message.getMessageType() == ExchangeableMessage.Types.BC_ARTISTS_TABLE_RESPONSE)
            return (Hashtable<String, ArrayList<String>>) message.getExchangedObject();
        else throw new IOException("Unknown response message from broker");
    }

    public static void printArtistsTable(Hashtable<String, ArrayList<String>> artistsTable, PrintStream out){
        Set<String> artistsNames = artistsTable.keySet();
        //ArrayList<String>
        for(String artist : artistsNames){
            ArrayList<String> artistsList= artistsTable.get(artist);
            out.printf("/\\----------------------------------/\\\n");
            out.printf("[~]> %s [%d]\n", artist, artistsList.size());
            for(String songTitle : artistsList) out.printf("\t%s\n", songTitle);
            out.printf("\\/----------------------------------\\/\n\n");
        }
    }

    /**
     * This function registers a broker to the hashtable of the current consumer.
     * @param broker the broker to be registered
     * @return true if broker doesn't exists and is registered false otherwise
     */
    private boolean registerBroker(BrokerInfo broker){
        String brokerStringKey = broker.getIp()+":"+broker.getInport();
        if( this.brokers.get(brokerStringKey) == null ){ // broker doesn't exists
            this.brokers.put(brokerStringKey, broker); return true;
        } else return false;
    }

    public enum Mode{
        ONLINE,
        OFFLINE
    }

    private static final String DEFAULT_WORKING_DIRECTORY = ".";
}
