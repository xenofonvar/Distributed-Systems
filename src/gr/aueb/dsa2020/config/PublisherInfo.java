package gr.aueb.dsa2020.config;

import gr.aueb.dsa2020.nodes.Publisher;

import java.io.Serializable;
import java.util.UUID;

public class PublisherInfo implements Serializable
{
    private String name;
    private int inport;
    private String ip;
    private Character minRange; private Character maxRange;
    //private UUID uuid;

    public PublisherInfo(){ this(null,null,0); }

    public PublisherInfo(String name, String ip, int inport){
        this.name = name; this.ip = ip; this.inport = inport; // this.uuid = UUID.randomUUID();
        this.minRange = '\000'; this.maxRange = '\000';
    }

    public String getName() { return name; }
    public PublisherInfo setName(String name) { this.name = name; return this; }
    public int getInport() { return inport; }
    public PublisherInfo setInport(int inport) { this.inport = inport; return this; }
    public String getIp() { return ip; }
    public PublisherInfo setIp(String ip) { this.ip = ip; return this; }
    public Character getMinRange() { return minRange; }
    public PublisherInfo setMinRange(Character minRange) { this.minRange = minRange; return this; }
    public Character getMaxRange() { return maxRange; }
    public PublisherInfo setMaxRange(Character maxRange) { this.maxRange = maxRange; return this;}

    //public int getHashCode() { return (this.toString() + " " + this.uuid.toString()).hashCode(); }

    /**
     * Rhe override is needed for the hashtable publishersDatasets to broker node class
     * @return returns the has of the current object
     */
    @Override
    public int hashCode(){
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object o){
        // If the object is compared with itself then return true
        if (o == this) return true;
        // Check if o is an instance of PublisherInfo or not
        if ( !(o instanceof PublisherInfo) ) return false;
        // cast the Object o to PublisherInfo object
        PublisherInfo pubi = (PublisherInfo) o;
        return this.name.equals(pubi.name) && this.inport == pubi.inport &&
                this.ip.equals(pubi.ip) && this.minRange.charValue() == pubi.minRange.charValue() &&
                this.maxRange.charValue() == pubi.maxRange.charValue();
    }

    @Override
    public String toString(){
        return "[" +this.name + "] - [" + this.ip + "]:[" + this.inport + "] ["+this.minRange+"-"+this.maxRange+"]";
    }

    private static final long serialVersionUID = -7112149652709478339L;
}
