package gr.aueb.dsa2020.config;

import java.io.Serializable;

/**
 * This class represents the consumer information. We create consumer info class
 * for future storage of consumer information and meta data. For now the only information
 * that is kept is the consumer ip.
 */
public class ConsumerInfo implements Serializable {
    private String ip;

    public ConsumerInfo(String ip){ this.ip  = ip; }
    public ConsumerInfo(){ this(null); }

    public String getIp() { return ip; }
    public ConsumerInfo setIp(String ip) { this.ip = ip; return this; }

    @Override
    public String toString(){ return this.ip; }
}
