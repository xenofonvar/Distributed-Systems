package gr.aueb.dsa2020.data.mp3;

import java.io.Serializable;

public class MP3SearchPair implements Serializable {

    private String artistName;
    private String songTitle;

    public MP3SearchPair(String artistName, String songTitle){
        this.artistName = artistName; this.songTitle = songTitle;
    }

    public String getArtistName() { return artistName.toLowerCase(); }
    public MP3SearchPair setArtistName(String artistName) { this.artistName = artistName; return this; }
    public String getSongTitle() { return songTitle.toLowerCase(); }
    public MP3SearchPair setSongTitle(String songTitle) { this.songTitle = songTitle; return this; }

    @Override
    public int hashCode() { return this.toString().hashCode(); }
    public String toString(){
        return this.getArtistName()+" - "+this.getSongTitle();
    }
    private static final long serialVersionUID = 7434145675052505293L;
}
