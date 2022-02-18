package gr.aueb.dsa2020.nodes;

import gr.aueb.dsa2020.config.BrokerInfo;
import gr.aueb.dsa2020.config.PublisherConfig;
import gr.aueb.dsa2020.data.exchange.handlers.BrokerNotifierHandler;
import gr.aueb.dsa2020.data.exchange.handlers.PublisherServerHandler;
import gr.aueb.dsa2020.data.mp3.MP3MusicFile;

import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;

public class PublisherNode implements Publisher {
    private PublisherConfig config;

    /**
     * This is the data structure that host the information that current publisher is responsible for.
     * String key to external hash table represents the artist name
     * The Hashtable as value to the external hashtable represents the list of songs of the key artist
     * The string key to the internal hashtable represents the song title and the
     * MP3MusicFile as values to the internal hashtable represents the actual music song with it's metadata
     */
    private Hashtable< String, // <--- artist name
                        Hashtable< String, MP3MusicFile > // <--- <key, value> = <SongTitle, MP3MusicFile>
            > dataset;  // O(1)

    public PublisherNode(String configFile){
        config = new PublisherConfig(configFile);
        this.dataset = new Hashtable<>();
    }

    @Override
    public boolean readConfig() { return this.config.readConfig(); }

    @Override
    public boolean readDatasetPool() {
        // filter the list of files to keep only directories
        File [] tmp = new File(config.getDatasetPoolPath()).listFiles();
        if(tmp==null | tmp.length==0) return false; // the dataset doesn't contains anything
        ArrayList<File> folders = new ArrayList<>();
        for(File f : tmp) if(f.isDirectory()) folders.add(f); // keep only files
        MP3MusicFile tempMusicFile; String category;
        for(File folder: folders) { // for each included folder in the dataset
            category = folder.getName(); // keep category to use it if missing from metadata
            File [] includedFiles = new File(folder.getAbsolutePath()).listFiles();
            for(File f : includedFiles){ // for each included file
                if(f.getName().startsWith(".")) continue; // skip the hidden files
                tempMusicFile = new MP3MusicFile(f.getAbsolutePath());
                if(!tempMusicFile.loadMetaDataInfo()) continue;  // if the metadata failed to load then skip the song
                if( this.isArtistInResponsibilityRange(tempMusicFile.getArtistName()) )
                {   // if the genre (category) is unknown then add the folder category
                    if( tempMusicFile.getGenreInfo().equals(MP3MusicFile.UNKNOWN_FIELD)) tempMusicFile.setGenreInfo(category);
                    // if the hashtable of the artist already exists get the hashtable of the artist and add the song to that structure
                    if( this.dataset.containsKey(tempMusicFile.getArtistName()) )
                        this.dataset.get(tempMusicFile.getArtistName()).put(tempMusicFile.getTitle(), tempMusicFile);
                    else { // in other case create the Hashtable for the new artist add the new song to it and add the artist to the dataset
                        Hashtable< String, MP3MusicFile > artistHashtable = new Hashtable<>();
                        artistHashtable.put(tempMusicFile.getTitle(), tempMusicFile);
                        this.dataset.put(tempMusicFile.getArtistName(), artistHashtable);
                    }
                }
            }
        }
        System.out.println();
        Set<String> artistsSet = this.dataset.keySet();
        System.out.println("\nPublishers is responsible for the following "+this.dataset.size()+" artists:");
        for(String a : artistsSet){
            Hashtable<String, MP3MusicFile> songs = this.dataset.get(a);
            System.out.println(a + " - number of songs: " + songs.size());
            Set<String> songsTitles = songs.keySet();
            for(String title : songsTitles) System.out.println("\t"+title);
            System.out.println("---------------------");
        }
        System.out.println();
        return true;
    }

    @Override
    public void notifyBrokers() {
        for(BrokerInfo b : this.config.getBrokers())
            new Thread(new BrokerNotifierHandler(b, this.dataset, this.config)).start();
    }

    @Override
    public void startServer() {
        new Thread( new PublisherServerHandler(this) ).start();
    }

    @Override
    public boolean init() {
        System.out.printf("[i]> Reading configuration file :[ %s ]\n", this.config.getConfigFile());
        if( !this.readConfig() ){ System.err.println("[x]>  Failed to read configuration file"); return false; }
        System.out.printf("[i]> Reading dataset on: [ %s ]\n", this.config.getDatasetPoolPath());
        if( !this.readDatasetPool() ){ System.err.println("[x]>  Failed to read dataset pool"); return false;  }
        System.out.printf("[i]> Starting server on: [%s:%d]\n", this.config.getIp(), this.config.getInport());
        this.startServer();
        try { Thread.sleep(200); } // sleep for 200 milliseconds so the publisher's server can executed first
        catch (InterruptedException e) { System.err.printf("[x]> Failed to sleep for unknown reason\n"); }
        System.out.printf("[i]> Notifying brokers...\n", this.config.getIp(), this.config.getInport());
        this.notifyBrokers();
        return true;
    }

    public PublisherConfig getConfig() { return config; }

    public Hashtable<String, Hashtable<String, MP3MusicFile>> getDataset() { return dataset; }

    private boolean isArtistInResponsibilityRange(String artistName){
        if(artistName==null||artistName.length()==0) return false;
        Character firstChar = artistName.charAt(0);
        //System.out.println("\t"+firstChar+" "+this.config.getMinRange()+" "+this.config.getMaxRange());
        return firstChar.compareTo(this.config.getMinRange()) >= 0 && firstChar.compareTo(this.config.getMaxRange()) <= 0;
    }

}
