package gr.aueb.dsa2020.config;

import java.io.Serializable;

public class BrokerInfo implements Serializable
{
    private String name;
    private int inport;
    private String ip;

    public BrokerInfo(){ this(null,null,0); }

    public BrokerInfo(String name, String ip, int inport){
        this.name = name; this.ip = ip; this.inport = inport;
    }

    public String getName() { return name; }
    public BrokerInfo setName(String name) { this.name = name; return this; }
    public int getInport() { return inport; }
    public BrokerInfo setInport(int inport) { this.inport = inport; return this; }
    public String getIp() { return ip; }
    public BrokerInfo setIp(String ip) { this.ip = ip; return this; }

    @Override
    public int hashCode(){
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object o){
        // If the object is compared with itself then return true
        if (o == this) return true;
        // Check if o is an instance of PublisherInfo or not
        if ( !(o instanceof BrokerInfo) ) return false;
        // cast the Object o to PublisherInfo object
        BrokerInfo bi = (BrokerInfo) o;
        System.out.println( this.inport == bi.inport && this.ip.equals(bi.ip) && this.name.equals(bi.name) );
        return this.inport == bi.inport && this.ip.equals(bi.ip) && this.name.equals(bi.name);
    }

    @Override
    public String toString(){ return "[" +this.name + "] - [" + this.ip + "]:[" + this.inport + "]"; }

    private static final long serialVersionUID = -4540456683589136514L;
}
