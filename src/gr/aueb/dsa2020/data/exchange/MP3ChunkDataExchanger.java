package gr.aueb.dsa2020.data.exchange;

import gr.aueb.dsa2020.data.mp3.MP3Chunk;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MP3ChunkDataExchanger {
    private MP3Chunk chunk;
    private short lastAction;
    private final Object lock = new Object(); // the object to lock

    public MP3ChunkDataExchanger(){ this.chunk = null; this.lastAction = MP3ChunkDataExchanger.LAST_ACTION_NONE; }

    public synchronized boolean write(MP3Chunk data){

//        System.out.println("\t[DataExchanger.write]: Writer is trying to acquire a lock to the lock object [" +
//                MP3ChunkDataExchanger.getDateTimeStr()+"]");
        synchronized (lock){
//            System.out.println("\t[DataExchanger.write]: Lock acquired successfully from Writer [" + MP3ChunkDataExchanger.getDateTimeStr()+"]");
            if(this.isClearToWrite())
            {
                this.chunk = data.clone(); // override for deep copy
                this.writerActed();
//                System.out.println("\t[DataExchanger.write]: Writer has update the data [" + MP3ChunkDataExchanger.getDateTimeStr()+"]");
                return true;
            }
            else return false;
        }
    }

    // if the read chunk is an empty chunk (chunk.isEmpty() == true) then writer is encountered with an error
    public synchronized MP3Chunk read()
    {
//        System.out.println("\t[DataExchanger.read]: Reader is trying to acquire a lock to the lock object [" +
//                MP3ChunkDataExchanger.getDateTimeStr()+"]");
        synchronized (lock){
//            System.out.println("\t[DataExchanger.read]: Lock acquired successfully from Reader [" + MP3ChunkDataExchanger.getDateTimeStr()+"]");
            // return a deep copy of this.data MP3Chunk object
            if(this.isClearToRead())
            {
//                System.out.println("\t[DataExchanger.read]: Reader has accessed the data [" + MP3ChunkDataExchanger.getDateTimeStr()+"]");
                this.readerActed();
                return this.chunk.clone();
            }
            else return null;
        }
    }

    private boolean isClearToWrite(){
        return this.lastAction== MP3ChunkDataExchanger.LAST_ACTION_NONE ||
                this.lastAction== MP3ChunkDataExchanger.LAST_ACTION_READER;
    }

    private boolean isClearToRead(){
        return this.lastAction== MP3ChunkDataExchanger.LAST_ACTION_WRITER;
    }

    private void writerActed(){
        this.lastAction = MP3ChunkDataExchanger.LAST_ACTION_WRITER;
    }

    private void readerActed(){
        this.lastAction = MP3ChunkDataExchanger.LAST_ACTION_READER;
    }


    public static String getDateTimeStr(){
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }

    public static final short LAST_ACTION_NONE = 0;
    public static final short LAST_ACTION_WRITER = 1;
    public static final short LAST_ACTION_READER = 2;
}
