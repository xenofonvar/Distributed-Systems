package gr.aueb.dsa2020.nodes;

import gr.aueb.dsa2020.config.BrokerConfig;
import gr.aueb.dsa2020.config.BrokerInfo;
import gr.aueb.dsa2020.config.ConsumerInfo;
import gr.aueb.dsa2020.config.PublisherInfo;
import gr.aueb.dsa2020.data.exchange.handlers.BrokerServerHandler;
import gr.aueb.dsa2020.data.mp3.MP3MusicFile;
import gr.aueb.dsa2020.data.mp3.MP3SearchPair;

import java.util.*;

public class BrokerNode implements Broker {
    private BrokerConfig config;
    // hashtable is thread safe
    // a registered consumer means that has the information of all brokers
    // the String key is a string representation of an ip
    // the ConsumerInfo a consumer info  object of the consumer
    private Hashtable<String, ConsumerInfo> consumers;

    private ArrayList<BrokerInfo> orderedBrokers;

    /**
     * This is the data structure that broker keeps the information
     * of the registered publishers.
     */
    private Hashtable<
            // this is the registered publisher
            PublisherInfo,
            // this is corresponding data set of the publisher
            Hashtable< String, Hashtable< String, MP3MusicFile> >
                    > publishersDatasets;


    public BrokerNode(String configFile){
        config = new BrokerConfig(configFile);
        this.consumers = new Hashtable<>();
        this.publishersDatasets = new Hashtable<>();
        this.orderedBrokers = new ArrayList<>();
    }

    @Override
    public boolean readConfig() { return this.config.readConfig(); }

    @Override
    public void startServer() { new Thread( new BrokerServerHandler(this) ).start(); }

    /**
     * This function registers the consumer to local registered info table.
     * A registered consumer means that has the information of the brokers.
     * Actually knows the existence of all brokers of the system core.
     *
     * @return true if the consumer registered successfully false if the
     *         consumer already exists in the the hashtable of the consumers
     */
    public synchronized boolean registerConsumer(ConsumerInfo c){
        if( !this.consumers.containsKey(c.getIp()) ){
            this.consumers.put(c.getIp(), c);
//            System.out.println("Consumer registered Successfully to consumers table: ");
//            this.consumers.forEach( (k,v)->System.out.println(v) );
            return true;
        }
        return false;
    }

    /**
     * This function registers (thread safe) a new publisher with the corresponding dataset
     * @param publisherInfo the publisher to be registered
     * @param dataset the corresponding dataset of the publisher
     * @return true if publisher successfully registered to the publisher's data structure
     */
    public synchronized boolean registerPublisher(
            PublisherInfo publisherInfo,
            Hashtable< String, Hashtable< String, MP3MusicFile> > dataset)
    {
        System.out.printf("\tInside BrokerNode.registerPublisher, try to register publisher [ %s ]\n",
                publisherInfo.toString());
        // check first if the publisher exists
        if( !this.publishersDatasets.containsKey(publisherInfo) ){
            System.out.printf("\tPublisher [ %s ] registered successfully\n", publisherInfo.toString());
            System.out.printf("\tThe new publisher [ %s ] has the following songs:\n", publisherInfo.toString());
            Set<String> artists = dataset.keySet();
            Set<String> songs;
            for(String artist : artists){
                System.out.printf("\t\tArtist: [ %s ] has the following songs:\n",artist);
                songs = dataset.get(artist).keySet();
                for(String song : songs)
                    System.out.printf("\t\t\t[ %s ]\n",dataset.get(artist).get(song).getTitle());
            }
            this.publishersDatasets.put(publisherInfo, dataset);
            return true;
        }
        System.out.printf("\tPublisher [ %s ] failed to be registered\n", publisherInfo.toString());
        return false; // else return false
    }

    public BrokerConfig getConfig() { return config; }

    public Hashtable<PublisherInfo, Hashtable<String, Hashtable<String, MP3MusicFile>>>
    getPublishersDatasets() { return publishersDatasets; }

    @Override
    public boolean init() {
        System.out.printf("[i]> Reading configuration file :[ %s ]\n", this.config.getConfigFile());
        if( !this.readConfig() ){ System.err.println("[x]>  Failed to read configuration file\n"); return false; }
        this.orderBrokers();
        System.out.printf("[i]> Configuration file reade successfully\n");
        this.startServer();
        return true;
    }

    /**
     * This function searchs for a given pair of arist and song title
     * @param mp3 the search pair to search for
     * @return the publisher who is responsible for the searched song or null in case the song doesn't exists
     */
    public synchronized PublisherInfo getInChargePublisherOf(MP3SearchPair mp3){
        String artistName = mp3.getArtistName();
        String songTitle = mp3.getSongTitle();
        Set<PublisherInfo> publishers = this.publishersDatasets.keySet();
        System.out.printf("\tInside BrokerNode.getInChargePublisherOf(MP3SearchPair),"+
                        " trying to find a publisher for [ %s ]\n",
                mp3.toString());
        for(PublisherInfo p : publishers){// for each registered publisher
            System.out.printf("\tChecking publisher: [ %s ]\n",p.toString());
            // check if the publisher is responsible for the given artistName (YES or NO)
            if( this.publishersDatasets.get(p).containsKey(artistName) ) // true --> YES
                // check if the publisher is responsible for the given songTitle (YES or NO)
                if( this.publishersDatasets.get(p).get(artistName).containsKey(songTitle)) { // true --> YES

                    return p; // return the publisher who is responsible for the given song cause we find him
                }
        }
        System.out.printf("\tFailed to find a publisher for song [ %s ]\n",mp3.toString());
        return null; // in any other case we return null an this means that we didn't find the search
    }

    /**
     * This function returns a hash table that the key represents the artist's name
     * and the key represents a list of songs that the artist has.
     * @return the hashtable with the artists and corresponding songs
     */
    public synchronized Hashtable< String, ArrayList<String> > getArtistsHashtable(){
        // aritstsHashtable is a hashtable that maps an arist's name (string) with an array
        // list containing the songs' titles of that artist.
        Hashtable <String, ArrayList<String> > artistsHashtable  = new Hashtable<>();
        Set<PublisherInfo> publishers = this.publishersDatasets.keySet(); // the registered publishers
        Set<String> artistsNames; Set<String> songTitles; // auxiliary references
        for(PublisherInfo p : publishers) { // for each registered publisher
            artistsNames = this.publishersDatasets.get(p).keySet(); //
            for(String artistName : artistsNames ){
                // get a list with song titles of the current artist
                songTitles = this.publishersDatasets.get(p).get(artistName).keySet();
                // check if the current artist exists in the aristsList (hashtable)
                if(artistsHashtable.containsKey(artistName))
                    // (TRUE) in that case we append the songs to the lists of existing artist's song list
                    artistsHashtable.get(artistName).addAll(songTitles); // append the songs
                else // (FALSE) the artist doesn't exists in the artistsList hashtable
                    artistsHashtable.put(artistName, new ArrayList(songTitles));

            }
        }
        return artistsHashtable;
    }

    private void orderBrokers(){
        // use tree map to order the brokers by its hash code. so all brokers instances sort the
        // brokers with the same way, so the hashring will be consistent between all the instances of
        // brokers.
        Map<Integer, BrokerInfo> brokersMap = new TreeMap<>(Collections.reverseOrder()); // descending order
        for(BrokerInfo b:this.config.getNeighbors()) brokersMap.put(b.hashCode(), b); // add all neighbors
        brokersMap.put(this.config.getBroker().hashCode(), this.config.getBroker()); // add this broker
        // get brokers with order and add them to orderedBrokers list
        for(Map.Entry m:brokersMap.entrySet()){
            System.out.println( ((BrokerInfo) m.getValue()).toString() + " --> " + ((BrokerInfo) m.getValue()).hashCode() );
            this.orderedBrokers.add( (BrokerInfo) m.getValue() );
        }
    }

    // hashiring https://www.ably.io/blog/implementing-efficient-consistent-hashing/
    public BrokerInfo findResponsibleBroker(MP3SearchPair mp3){
        int index = Math.abs( mp3.hashCode() % this.orderedBrokers.size() );
        System.out.println("\t[BrokerInfo.findResponsibleBroker --> index]: "+index+" The responsible broker is "+this.orderedBrokers.get(index));
        return this.orderedBrokers.get(index);
    }
}
