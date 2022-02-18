package gr.aueb.dsa2020;

import gr.aueb.dsa2020.nodes.BrokerNode;
import org.apache.commons.cli.*;

import java.util.Arrays;

public class BrokerNodeDriver {
    public static void main(String[] args) {
        Options options = BrokerNodeDriver.createCLIOptions();
        // if no arguments are provided
        if(args.length==0){ BrokerNodeDriver.printHelpMessage(options); System.exit(0); }
        // in case there are CLI arguments
        CommandLineParser parser = new DefaultParser();
        try { // parsing the given command line arguments
            CommandLine cmd = parser.parse(options, args); // create the parser
            // First usage case: prints the help message
            if(cmd.hasOption("h")) BrokerNodeDriver.printHelpMessage(options);
            // Second usage case: Initialize a BrokerNode
            else if( cmd.hasOption("c") ) { new BrokerNode(cmd.getOptionValue("c")).init(); }
            // In any other case throw an error
            else { throw new ParseException("Unknown CLI option"); }
        } catch (ParseException e) { // OOPS, something went wrong
            System.err.println( "[x]> CLI Parsing failed cause: " + e.getMessage()+", "+ Arrays.toString(e.getStackTrace()));
            System.exit(1);
        }
    }

    private static Options createCLIOptions(){
        Option configFile = new Option("c", "config-file", true,
                "[OBLIGATORY ARGUMENT] Specifies the file which contains all the settings for the current broker."+
                        " The config file is JSON-based");
        configFile.setArgName("CONFIG-FILE");
        Option help = new Option("h", "help", false,
                "[OPTIONAL ARGUMENT] Prints the help message.");

        // add options to Options instance
        Options options = new Options();
        options.addOption(configFile).addOption(help);
        return options;
    }

    private static void printHelpMessage(Options options){
        System.out.println("Distributed Systems - Assignment: 1st Part\nCS AUEB - Summer Semester 2019-2020\n\n"+
                "[~]> Broker Node Driver Program");
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(100);
        String headerMessage = "\n[~]> Command Line Arguments are:";
        String footerMessage = "\n[~]> Participants:\nKampouridis Theocharis (3140313)\n"+
                "Paparidis Evaggelos (3150231)\nSigalos Konstantinos (3140182)\nVarelis Xenofon (3170014)";
        formatter.printHelp("publisher -c <CONFIG-FILE>", headerMessage, options, footerMessage );
    }
}
