package gr.aueb.dsa2020.data.mp3;

import java.io.Serializable;
import java.util.Arrays;

public class MP3Chunk implements Serializable, Cloneable
{
    private String filename; // **THE ABSOLUTE PATH TO MP3 FILE**
    private String title;
    private String artistName;
    private String albumInfo;
    private String genreInfo;
    private String year;

    private int totalPartitions;
    private int partitionNumber;
    private byte [] partition;

    public MP3Chunk(byte [] partition, int partitionNumber, int totalPartitions){
        this.partition = partition; this.partitionNumber = partitionNumber; this.totalPartitions = totalPartitions;
        this.filename = null; this.albumInfo = null; this.artistName = null; this.genreInfo = null;
        this.title = null; this.year = null;
    }

    public void setMetaDataInfoFrom(MP3MusicFile mp3file){
        this.filename = new String(mp3file.getFilename().toCharArray());
        this.title = new String(mp3file.getTitle().toCharArray());
        this.artistName = new String(mp3file.getArtistName().toCharArray());
        this.albumInfo = new String(mp3file.getAlbumInfo().toCharArray());
        this.genreInfo = new String(mp3file.getGenreInfo().toCharArray());
        this.year = new String(mp3file.getYear().toCharArray());
    }

    @Override
    public MP3Chunk clone(){
        MP3Chunk temp =
                new MP3Chunk(Arrays.copyOf(this.partition, this.partition.length), this.partitionNumber, this.totalPartitions );
        temp.setFilename(new String(this.getFilename().toCharArray()));
        temp.setTitle(new String(this.getTitle().toCharArray()));
        temp.setArtistName(new String(this.getArtistName().toCharArray()));
        temp.setAlbumInfo(new String(this.getAlbumInfo().toCharArray()));
        temp.setGenreInfo(new String(this.getGenreInfo().toCharArray()));
        temp.setYear(new String(this.getYear().toCharArray()));
        return temp;
    }

    public boolean isEmpty(){
        return this.partition == null && this.partitionNumber <=0 && this.totalPartitions <=0;
    }

    public int getTotalPartitions() { return totalPartitions; }
    public void setTotal(int totalPartitions) { this.totalPartitions = totalPartitions; }
    public int getPartitionNumber() { return partitionNumber; }
    public void setPartitionNumber(int partitionNumber) { this.partitionNumber = partitionNumber; }
    public byte[] getPartition() { return partition; }
    public void setPartition(byte[] partition) { this.partition = partition; }
    public String getFilename() { return filename; }
    public void setFilename(String filename) { this.filename = filename;  }
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    public String getArtistName() { return artistName; }
    public void setArtistName(String artistName) { this.artistName = artistName; }
    public String getAlbumInfo() { return albumInfo; }
    public void setAlbumInfo(String albumInfo) { this.albumInfo = albumInfo; }
    public String getGenreInfo() { return genreInfo; }
    public void setGenreInfo(String genreInfo) { this.genreInfo = genreInfo; }
    public String getYear() { return year; }
    public void setYear(String year) { this.year = year; }
    public int getActualSize() { return this.partition.length; }

    private static final long serialVersionUID = -843809931157935692L;
}
