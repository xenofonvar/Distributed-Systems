package gr.aueb.dsa2020.data.exchange;

import java.io.Serializable;

public class ExchangeableMessage implements Serializable
{
    private short messageType;

    private Serializable exchangedObject;

    public ExchangeableMessage(){ this.exchangedObject = null; }

    public ExchangeableMessage setMessageType(short messageType) { this.messageType = messageType; return this; }

    public short getMessageType() { return messageType; }

    public Serializable getExchangedObject() { return exchangedObject; }

    public ExchangeableMessage setExchangedObject(Serializable exchangedObject)
        { this.exchangedObject = exchangedObject; return this; }

    // This class represents the types of exchangable messages.
    // The convention is XY_<friendly-description>
    // So the literal XY_FRIEND_DESCRIPTION means a message from X client to Y server and the
    // description is a friend description for programmers!!! :)
    // X is the sender and Y is the receiver
    // P: means publisher, B: means broker, C: means consumer
    // In case the code type begins with GP that means the type is generally proposed for every node
    // exchangeable message.
    public static class Types
    {
        // publisher to broker message types
        public static final short PB_PUBLISHER_REGISTRATION_CONNECT_INFO = 0;
        public static final short PB_PUBLISHER_REGISTRATION_ARTISTS_LIST = 1;
        public static final short PB_CHUNK_TRANSMISSION = 2;
        public static final short PB_REQUESTED_ARTIST_NOT_FOUND = 3;
        public static final short PB_REQUESTED_SONG_NOT_FOUND = 4;

        // broker to publisher message types -- 20
        public static final short BP_SONG_REQUEST = 20;

        // broker to broker message types -- 30
        public static final short BB_CONSUMER_REGISTRATION = 30;

        // broker to consumer message types -- 40
        public static final short BC_INCHARGE_BROKER = 40;
        public static final short BC_NO_RESULTS = 41;
        public static final short BC_CHUNK_TRANSMISSION = 42;
        public static final short BC_ARTISTS_TABLE_RESPONSE = 43;
        public static final short BC_BROKERS_LIST = 44;


        // consumer to broker message types -- 60
        public static final short CB_SONG_REQUEST = 60;
        public static final short CB_ARTISTS_TABLE_REQUEST = 61;

        // general purpose (GP) messages
        public static final short GP_EXCHANGE_ACHIEVED = 100;
        public static final short GP_EXCHANGE_FAILED = 101;

    }

    private static final long serialVersionUID = 4216329923910515068L;
}
