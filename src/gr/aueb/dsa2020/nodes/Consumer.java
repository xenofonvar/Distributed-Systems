package gr.aueb.dsa2020.nodes;

import gr.aueb.dsa2020.config.BrokerInfo;
import gr.aueb.dsa2020.data.mp3.MP3Chunk;
import gr.aueb.dsa2020.data.mp3.MP3SearchPair;

import java.io.IOException;
import java.util.ArrayList;

public interface Consumer extends Node {
    ArrayList<MP3Chunk> performRequestFor(BrokerInfo broker, MP3SearchPair mp3)
            throws ClassNotFoundException, IOException;
}
