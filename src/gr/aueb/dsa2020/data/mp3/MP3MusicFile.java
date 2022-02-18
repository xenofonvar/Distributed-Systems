package gr.aueb.dsa2020.data.mp3;

import com.mpatric.mp3agic.*;
import gr.aueb.dsa2020.config.PublisherInfo;
import gr.aueb.dsa2020.utils.FileUtilities;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

public class MP3MusicFile implements Serializable
{
    private String filename; // **THE ABSOLUTE PATH TO MP3 FILE**
    private String title;
    private String artistName;
    private String albumInfo;
    private String genreInfo;
    private String year;
    private int chunkSize; // chunk size in kilobyte
    //private UUID uuid;

    /*
     * First create a MP3MusicFile with the given filename and then call the loadMetaDataInfo to retrieve the metadata
     * information of the song. After if you need to create the chunks, call the createChunks method and access them by
     * calling the getChunksList method. After finishing the usage of the chunks don't forget to call the removeChunks
     * method to clear the memory.
     */
    public MP3MusicFile(String filename) {
        this.filename = filename;
        this.genreInfo = MP3MusicFile.EMPTY_FIELD;
        this.year = MP3MusicFile.EMPTY_FIELD;
        this.albumInfo = MP3MusicFile.EMPTY_FIELD;
        this.artistName = MP3MusicFile.EMPTY_FIELD;
        this.title = MP3MusicFile.EMPTY_FIELD;
        this.chunkSize = MP3MusicFile.DEFAULT_CHUNK_SIZE;
        //this.uuid = UUID.randomUUID();
    }

    // creates chunks for the specified song
    public synchronized ArrayList<MP3Chunk>  createChunks() {
        ArrayList<MP3Chunk> chunksList = new ArrayList<>();
        byte [] byteArray;
        if (this.filename == null) return null; // if is null
        // check if file exists and is actual a file
        File mp3fileHandler = new File(this.filename);
        if (!(mp3fileHandler.exists() & mp3fileHandler.isFile())) return null;
        // check the file's extension
        String extension = FileUtilities.getExtensionOf(this.filename);
        if (extension == null || !extension.equals("mp3")) return null;
        // try to read all the bytes of the file and assign them to byteArray
        try {
            byteArray = Files.readAllBytes(Paths.get(this.filename));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        // if the size of the file is equals or less than the chunk size we should create on chunk for the song
        if (byteArray.length <= this.chunkSize) {
            MP3Chunk tempChunk =
                    new MP3Chunk(Arrays.copyOf(byteArray, byteArray.length), 1, 1);
            tempChunk.setMetaDataInfoFrom(this);
            chunksList.add(tempChunk);
            return chunksList;
        }
        // in case the song size (in bytes) is greater than one chunk we create multiple chunks
        int numberOfChunks = byteArray.length / this.chunkSize; // calculate the chunks
        if ((byteArray.length % this.chunkSize) > 0)
            numberOfChunks += 1; // last file has less than size than chunkSize
        for (int i = 0; i < numberOfChunks; ++i) {
            MP3Chunk tempChunk;
            if ((i + 1) == numberOfChunks) // if is the last chunk
                tempChunk = new MP3Chunk(
                        Arrays.copyOfRange(byteArray, (i * this.chunkSize), byteArray.length ),
                        (i + 1), numberOfChunks);
            else // is fist or before last chunk
                tempChunk = new MP3Chunk(Arrays.copyOfRange(byteArray, (i * chunkSize), ((i + 1) * chunkSize)),
                        (i + 1), numberOfChunks);
            tempChunk.setMetaDataInfoFrom(this);
            chunksList.add(tempChunk);
        }
        return chunksList;
    }

    public boolean loadMetaDataInfo() {
        if(this.filename==null) return false; // if song is null
        //System.out.println("\t95");
        // check if file exists and is actual a file
        File mp3fileHandler = new File(this.filename);
        if( ! (mp3fileHandler.exists() & mp3fileHandler.isFile()) ) return false;
        //System.out.println("\t99");
        // check the file's extension
        String extension = FileUtilities.getExtensionOf(this.filename);
        if( extension==null || !extension.equals("mp3")  ) return false;
        //System.out.println("\t103");
        // try to retrieve the meta data information
        Mp3File mp3file;
        try { mp3file= new Mp3File(filename); }
        catch (UnsupportedTagException | IOException e) {
            /*e.printStackTrace();*/
            System.out.println("Error on loading metadata information of file "+this.filename+" cause: "+e.getMessage());
            return false;
        }
        catch (InvalidDataException e) {
//            e.printStackTrace();
            System.out.println("Error on loading metadata information of file "+this.filename+" cause: "+e.getMessage());
            return false;
        }
        //System.out.println("\t110");
        if (mp3file.hasId3v1Tag()) { // if we have version 1 id3 tags
            ID3v1 id3v1Tag = mp3file.getId3v1Tag();
            this.title = ( (id3v1Tag.getTitle()==null||id3v1Tag.getTitle().trim().length()==0)?mp3fileHandler.getName().replace(".mp3", "").toLowerCase().trim():id3v1Tag.getTitle().toLowerCase().trim() );
            this.artistName = ( (id3v1Tag.getArtist()==null||id3v1Tag.getArtist().trim().length()==0)?MP3MusicFile.UNKNOWN_FIELD:id3v1Tag.getArtist().toLowerCase().trim() );
            this.albumInfo = ( id3v1Tag.getAlbum()==null?MP3MusicFile.UNKNOWN_FIELD:id3v1Tag.getAlbum() );
            this.genreInfo = ( id3v1Tag.getGenreDescription()==null?MP3MusicFile.UNKNOWN_FIELD:id3v1Tag.getGenreDescription() );
            this.year = ( id3v1Tag.getYear()==null?MP3MusicFile.UNKNOWN_FIELD:id3v1Tag.getYear() );
        }
        else if (mp3file.hasId3v2Tag())  { // if we have version 2 id3 tags
            ID3v2 id3v2Tag = mp3file.getId3v2Tag();
            this.title = ( (id3v2Tag.getTitle()==null||id3v2Tag.getTitle().trim().length()==0)?mp3fileHandler.getName().replace(".mp3", "").toLowerCase().trim():id3v2Tag.getTitle().toLowerCase().trim() );
            this.artistName = ( (id3v2Tag.getArtist()==null||id3v2Tag.getArtist().trim().length()==0)?MP3MusicFile.UNKNOWN_FIELD:id3v2Tag.getArtist().toLowerCase().trim() );
            this.albumInfo = ( id3v2Tag.getAlbum()==null?MP3MusicFile.UNKNOWN_FIELD:id3v2Tag.getAlbum() );
            this.genreInfo = ( id3v2Tag.getGenreDescription()==null?MP3MusicFile.UNKNOWN_FIELD:id3v2Tag.getGenreDescription() );
            this.year = ( id3v2Tag.getYear()==null?MP3MusicFile.UNKNOWN_FIELD:id3v2Tag.getYear() );
        }
        else return false; // if there are no metadata to the mp3 file
        //System.out.println("\t128");
        return true; // finally if all executed normally, return true
    }

    public String getFilename() { return filename; }
    public MP3MusicFile setFilename(String filename) { this.filename = filename; return this; }
    public String getArtistName() {return artistName;}
    public MP3MusicFile setArtistName(String artistName) { this.artistName = artistName; return this; }
    public String getAlbumInfo() {return albumInfo;}
    public MP3MusicFile setAlbumInfo(String albumInfo) { this.albumInfo = albumInfo; return this; }
    public String getGenreInfo() {return genreInfo;}
    public MP3MusicFile setGenreInfo(String genreInfo) { this.genreInfo = genreInfo; return this; }
    public String getYear() { return year; }
    public MP3MusicFile setYear(String year) { this.year = year; return this; }
    public String getTitle() { return title; }
    public MP3MusicFile setTitle(String title) { this.title = title; return this; }
    public int getChunkSize() { return chunkSize; }

    public MP3MusicFile setChunkSize(int chunkSize) {
       this.chunkSize =
               (chunkSize>=MP3MusicFile.MIN_CHUNK_SIZE&& chunkSize <= MP3MusicFile.MAX_CHUNK_SIZE) ?
                       chunkSize : MP3MusicFile.DEFAULT_CHUNK_SIZE;
        return this;
    }

    public String toString(){
        return this.filename + ", " + this.artistName + ", " + this.title + ", " + this.albumInfo + ", " + this.genreInfo + ", " + this.year;
    }

    @Override
    public int hashCode(){
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object o){
        // If the object is compared with itself then return true
        if (o == this) return true;
        // Check if o is an instance of PublisherInfo or not
        if ( !(o instanceof MP3MusicFile) ) return false;
        // cast the Object o to PublisherInfo object
        MP3MusicFile mp3 = (MP3MusicFile) o;
        return this.filename.equals(mp3.filename) &&
                this.albumInfo.equals(mp3.albumInfo) &&
                this.artistName.equals(mp3.artistName) &&
                this.chunkSize == mp3.chunkSize &&
                this.genreInfo.equals(mp3.genreInfo) &&
                this.title.equals(mp3.title) &&
                this.year.equals(mp3.year);
    }



    private static final long serialVersionUID = -5452740108667479198L;
    public static final String EMPTY_FIELD= "<EMPTY-FIELD>";
    public static final String UNKNOWN_FIELD="unknown-field";
    public static final int MIN_CHUNK_SIZE = 102400; // minimum of 100kb
    public static final int MAX_CHUNK_SIZE = 5242880; // maximum  size in bytes (5Mb)
    public static final int DEFAULT_CHUNK_SIZE = 524288; // chunk size in bytes (512Kb)
}
