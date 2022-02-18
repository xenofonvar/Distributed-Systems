package gr.aueb.dsa2020;

import gr.aueb.dsa2020.config.BrokerInfo;
import gr.aueb.dsa2020.data.mp3.MP3Chunk;
import gr.aueb.dsa2020.data.mp3.MP3SearchPair;
import gr.aueb.dsa2020.nodes.ConsumerNode;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

public class ConsumerNodeDriver {
    // Obligatory cmd run options to perform a request:
    // consumer -b "192.168.1.40:2057" -a "jason shaw" -s "landra's dream" -o "C:\Users\tmk\Desktop\temp"
    // -b "192.168.1.5:2056" -a "brian boyko" -s "ambush in rattlesnake gulch" -o "C:\Users\tmk\Desktop\temp" -m online
    // you can use, optionally, the "-m offline" or "-m online" for online and offline mode
    private static String mode = "online";
    private static String outputDir = new File(".").getAbsolutePath();

    public static void main(String[] args){
        // create cli options
        Options options = ConsumerNodeDriver.createCLIOptions();
        // if no arguments are provided
        if(args.length==0){ ConsumerNodeDriver.printHelpMessage(options); System.exit(0); }
        // in case there are CLI arguments, try to parse them
        CommandLineParser parser = new DefaultParser();
        try { // parsing the given command line arguments
            System.out.printf("[i]> Try to parse given command line arguments...\n");
            CommandLine cmd = parser.parse(options, args); // create the parser
            // First usage case: prints the help message
            if(cmd.hasOption("h")) ConsumerNodeDriver.printHelpMessage(options);
            // Second usage case: Perform a request
            else if( cmd.hasOption("b") && cmd.hasOption("a") && cmd.hasOption("s") ) {
                System.out.printf("\tRequested artists name: [ %s ]\n", cmd.getOptionValue("a"));
                System.out.printf("\tRequested song title: [ %s ]\n", cmd.getOptionValue("s"));
                // try to parse the <IP>:<PORT> brokers networking identifiers
                BrokerInfo broker = ConsumerNodeDriver.parseBrokersIpPort(cmd.getOptionValue("b"));
                // take the arist name and the song title
                String artistName = cmd.getOptionValue("a");
                String songTitle = cmd.getOptionValue("s");
                MP3SearchPair mp3 = new MP3SearchPair(artistName, songTitle); // create the search pair
                // check if the mode type is specified
                if(cmd.hasOption("m") &&
                        (cmd.getOptionValue("m").equals("online") || cmd.getOptionValue("m").equals("offline")) )
                    mode = cmd.getOptionValue("m");
                // check if the output directory is specifies
                if(cmd.hasOption("o")){
                    File f = new File(cmd.getOptionValue("o"));
                    // check if the path represents an existing directory
                    if( f.exists() && f.isDirectory() ) outputDir = f.getAbsolutePath();
                    else throw new ParseException("Output directory parsing error");
                }
                ConsumerNode consumer; // create consumer node object
                // create the consumer node according to the mode type
                if(mode.equals("online")) consumer = new ConsumerNode(ConsumerNode.Mode.ONLINE);
                else consumer = new ConsumerNode(ConsumerNode.Mode.OFFLINE);
                consumer.setWorkingDir(outputDir);
                ArrayList<MP3Chunk> chunks;
                try { // try to perform the request to given broker
                    chunks = consumer.performRequestFor(broker, mp3);
                    if(chunks!=null) consumer.writeChunks(chunks);
                    else System.out.printf("[x] A null is returned from the broker. Failed to download requested song\n");
                } catch (IOException | ClassNotFoundException e) {
                    System.out.printf("[x]> An error occurred while performing the request to broker cause: %s\n",
                            e.getMessage());
                    e.printStackTrace();
                    System.exit(2);
                }
            // in case of artists' table request
            } else if( cmd.hasOption('t') && cmd.hasOption("b") && (!cmd.hasOption("a") && !cmd.hasOption("s")) ){
                boolean toStdOut = true;
                if(cmd.hasOption("o")){
                    File outDir = new File(cmd.getOptionValue("o"));
                    if( !(outDir.exists() && outDir.isDirectory()) )
                        throw new ParseException("The specified output directory doesn't exists.");
                    else{
                        toStdOut = false; // write contents of request to file
                        ConsumerNodeDriver.outputDir = outDir.getAbsolutePath();
                    }
                }
                // try to parse the <IP>:<PORT> brokers networking identifiers
                BrokerInfo broker = ConsumerNodeDriver.parseBrokersIpPort(cmd.getOptionValue("b"));
                ConsumerNode consumer = new ConsumerNode(ConsumerNode.Mode.ONLINE);
                try {
                    Hashtable<String, ArrayList<String>> artistsTable = consumer.requestArtistsTable(broker);
                    System.out.printf("[i]> Artists' Table received successfully from the broker [ %s ]\n", broker);
                    if(toStdOut) ConsumerNode.printArtistsTable(artistsTable, System.out);
                    else{
                        ConsumerNode.printArtistsTable(artistsTable,
                                new PrintStream(new File(outputDir+"/artists-table.txt")));
                        System.out.printf("[i]> Requested artists table has been written to file: [ %s ]\n",
                                outputDir+"/artists-table.txt");
                    }
                } catch (IOException | ClassNotFoundException e) {
                    System.out.printf("[x]> An error occurred while performing the request to broker cause: %s\n",
                            e.getMessage());
                    e.printStackTrace();
                    System.exit(3);
                }
            }
            // In any other case throw an error
            else throw new ParseException("Error on CLI option(s). Unknown option(s) or illegal combination detected");
        } catch (ParseException e) { // OOPS, something went wrong
            System.err.println( "[x]> CLI Parsing failed cause: " + e.getMessage()+", "+
                    Arrays.toString(e.getStackTrace()));
            System.exit(1);
        }
    }

    private static Options createCLIOptions(){
        Option broker = new Option("b", "broker", true,
                "[OBLIGATORY ARGUMENT] Specifies the broker server to make the initial request. The format "+
                        "of the value of that argument is: <IP>:<PORT>, where <IP> entity will be replace by the "+
                        "ip in which broker is listening and <PORT> entity is the corresponding port. Note that "+
                        "ip and port are separated by the colon (:) sign.");
        broker.setArgName("IP:PORT");

        // --- options for a song request
        Option artist = new Option("a", "artist", true,
                "[OBLIGATORY ARGUMENT] Specifies the artist name of the requested pair "+
                        "(Artist Name, Song Title).");
        artist.setArgName("ARTIST NAME");

        Option song = new Option("s", "song", true,
                "[OBLIGATORY ARGUMENT] Specifies the song's title of the requested pair "+
                        "(Artist Name, Song Title).");
        song.setArgName("SONG TITLE");

        Option outputDir= new Option("o", "output-directory", true,
                "[OPTIONAL ARGUMENT] Specifies absolute or relative path to store the received data. "+
                "If this argument is omitted then the current working directory is used as output directory.");
        outputDir.setArgName("PATH");

        Option mode = new Option("m", "mode", true,
                "[OPTIONAL ARGUMENT] Specifies the mode that the client will be executed. It has two "+
                "option, \'online\' or \'offline\'. In online mode the received data are saved to the output directory "+
                "as mp3 chunks. On the other hand in offline mode the received data are saved as a whole piece of "+
                "mp3 music file. If this options is omitted then the default option is online.");
        mode.setArgName("MODE");

        // -- options for an artists' table request
        Option requestArtistsTable = new Option("t", "artists-table", false,
                "[OBLIGATORY ARGUMENT] Function options to request from the broker the artists' table."+
                " This option cannot combined with any other request. Note that the broker options (-b) must "+
                "be supplied to perform the consumer client a request. If the -o options used with that option "+
                "then a file with name artists-table.txt will be created to the specified output directory <PATH>, " +
                "containing the requested data (artists table).");

        // --- help options
        Option help = new Option("h", "help", false,
                "[OPTIONAL ARGUMENT] Prints the help message.");

        // add options to Options instance
        Options options = new Options();
        options.addOption(broker).addOption(artist).
                addOption(song).addOption(outputDir).
                addOption(mode).addOption(help).addOption(requestArtistsTable);
        return options;
    }

    private static void printHelpMessage(Options options){
        System.out.println("Distributed Systems - Assignment: 1st Part\nCS AUEB - Summer Semester 2019-2020\n\n"+
                "[~]> Consumer Node Driver Program");
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(100);
        String headerMessage = "\n[~]> Command Line Arguments are:";
        String footerMessage = "\n[~]> Participants:\nKampouridis Theocharis (3140313)\n"+
                "Paparidis Evaggelos (3150231)\nSigalos Konstantinos (3140182)\nVarelis Xenofon (3170014)";
        formatter.printHelp("consumer -b <IP:PORT> -a <ARTIST> -s <SONG TITLE>",
                headerMessage, options, footerMessage );
    }

    private static BrokerInfo parseBrokersIpPort(String argumentOption) throws ParseException {
        // try to parse the <IP>:<PORT> brokers networking identifiers
        String [] splittedBrokerArgs = argumentOption.split(":");//cmd.getOptionValue("b").split(":");
        if(splittedBrokerArgs.length!=2) throw new ParseException("Broker's option argument parsing error");
        // below we cehck if the given ip address is actually a valid ip address
        try { InetAddress.getByName(splittedBrokerArgs[0]); }
        catch (UnknownHostException e) { throw new ParseException("Broker's given ip parsing error"); }
        String brokersIp = splittedBrokerArgs[0]; // final store the brokers ip
        Integer brokersInport = 0; // try to extract the port
        try{ // check for a valid port
            brokersInport = Integer.parseInt(splittedBrokerArgs[1]);
            // we should use a port that doesn't belongs to the range of well known ports
            if( !(brokersInport>1024 && brokersInport<=65536 ) ) throw new NumberFormatException();
        }
        catch(NumberFormatException e){ throw new ParseException("Broker's given port parsing error"); }
        BrokerInfo broker = new BrokerInfo(); // create the brokers info object
        broker.setIp(brokersIp).setInport(brokersInport);
        return broker;
    }
}
