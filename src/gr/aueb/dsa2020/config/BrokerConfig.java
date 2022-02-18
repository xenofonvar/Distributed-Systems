package gr.aueb.dsa2020.config;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.*;
import java.util.ArrayList;

public class BrokerConfig {
    private String configFile;
    private ArrayList<BrokerInfo> neighbors;
    private BrokerInfo broker;

    public BrokerConfig(String configFile){
        this.configFile = configFile;
        this.neighbors = new ArrayList<>();
        this.broker = new BrokerInfo();
    }


    public boolean readConfig() { // if something goes wrong returns false!!!
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
        this.broker.setName(name).setIp(ip).setInport(inport);
        if(!jsonObject.containsKey("neighbors")) return false;
        JsonArray brokersJsonArray = jsonObject.getJsonArray("neighbors");
        for(int i=0; i<brokersJsonArray.size(); ++i){
            if( !brokersJsonArray.getJsonObject(i).containsKey("ip") ||
                    !brokersJsonArray.getJsonObject(i).containsKey("inport") ) return false;
            this.neighbors.add( new BrokerInfo(
                    (!brokersJsonArray.getJsonObject(i).containsKey("name")?
                            "UNKNOWN":brokersJsonArray.getJsonObject(i).getString("name")),
                    brokersJsonArray.getJsonObject(i).getString("ip"),
                    brokersJsonArray.getJsonObject(i).getInt("inport")));
        }
        return true;


    }

    public String getConfigFile() { return this.configFile; }
    public String getName() { return this.broker.getName(); }
    public String getIp() { return this.broker.getIp(); }
    public int getInport() { return this.broker.getInport(); }
    public ArrayList<BrokerInfo> getNeighbors() { return this.neighbors; }

    public BrokerInfo getBroker() { return this.broker; }
}
