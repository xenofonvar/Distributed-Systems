package gr.aueb.dsa2020.config;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.*;
import java.util.ArrayList;

public class PublisherConfig {
    private String configFile;
    private ArrayList<BrokerInfo> brokers;
    private String datasetPoolPath;
    private PublisherInfo publisher;

    public PublisherConfig(String configFile){
        this.configFile = configFile;
        this.publisher = new PublisherInfo();
        this.brokers = new ArrayList<>();
        this.datasetPoolPath = null;
    }

    public boolean readConfig(){ // if something goes wrong returns false!!!
        InputStream fis;
        File configFileHandler = new File(this.configFile);
        if( ! (configFileHandler.exists() & configFileHandler.isFile()) ) return false;
        try { fis = new FileInputStream(this.configFile); }
        catch (FileNotFoundException e) { e.printStackTrace(); return false; }
        JsonReader jsonReader = Json.createReader(fis);
        JsonObject jsonObject = jsonReader.readObject();
        //we can close IO resource and JsonReader now
        jsonReader.close();
        try { fis.close(); } catch (IOException e) { e.printStackTrace(); /*--IGNORE--*/ }
        if(!jsonObject.containsKey("name")) return false;
        String name = jsonObject.getString("name");
        if(!jsonObject.containsKey("ip")) return false;
        String ip = jsonObject.getString("ip");
        if(!jsonObject.containsKey("inport")) return false;
        int inport = jsonObject.getInt("inport");
        this.publisher.setName(name).setIp(ip).setInport(inport);
        if(!jsonObject.containsKey("artistsRange") || jsonObject.getJsonArray("artistsRange").size()!=2) return false;
        char minRange = jsonObject.getJsonArray("artistsRange").getString(0).toLowerCase().charAt(0);
        char maxRange = jsonObject.getJsonArray("artistsRange").getString(1).toLowerCase().charAt(0);
        publisher.setMaxRange(maxRange).setMinRange(minRange);
        if(!jsonObject.containsKey("datasetPoolPath")) return false;
        this.datasetPoolPath = jsonObject.getString("datasetPoolPath");
        if(!jsonObject.containsKey("brokers")) return false;
        JsonArray brokersJsonArray = jsonObject.getJsonArray("brokers");
        for(int i=0; i<brokersJsonArray.size(); ++i){
            if( !brokersJsonArray.getJsonObject(i).containsKey("ip") ||
                !brokersJsonArray.getJsonObject(i).containsKey("inport") ) return false;
            this.brokers.add( new BrokerInfo(
                (!brokersJsonArray.getJsonObject(i).containsKey("name")?"UNKNOWN":brokersJsonArray.getJsonObject(i).getString("name")),
                brokersJsonArray.getJsonObject(i).getString("ip"),
                brokersJsonArray.getJsonObject(i).getInt("inport")));
        }
        return true;
    }

    public String getConfigFile() { return configFile; }
    public String getName() { return this.publisher.getName(); }
    public ArrayList<BrokerInfo> getBrokers() { return brokers; }
    public char getMinRange() { return this.publisher.getMinRange(); }
    public char getMaxRange() { return this.publisher.getMaxRange(); }
    public String getDatasetPoolPath() { return datasetPoolPath; }
    public String getIp() { return this.publisher.getIp(); }
    public int getInport() { return this.publisher.getInport(); }
    public PublisherInfo getPublisher() { return publisher; }

    public String toString(){
        String prefix  = "============ PUB-CONFIG-START ==============\n";
        String postfix = "\n============= PUB-CONFIG-END ===============";
        String tmp = this.configFile + "\n" +
                this.datasetPoolPath + "\n[" +
                this.getName() + "] - [" +
                this.getIp() + "]:[" +
                this.getInport() + "] - [" +
                this.getMinRange() + "-" + this.getMaxRange()+"]";
        for(BrokerInfo cbi : this.brokers)
            tmp += "\n" + cbi.toString();
        return prefix+tmp+postfix;
    }
}
