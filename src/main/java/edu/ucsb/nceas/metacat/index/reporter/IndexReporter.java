package edu.ucsb.nceas.metacat.index.reporter;

import java.io.FileNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.service.types.v1.Identifier;
import org.json.JSONArray;
import org.json.JSONObject;
import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.Address;

public class IndexReporter {

	/** Define a logger to log to the console or a file*/
	public static Log log = LogFactory.getLog(IndexReporter.class);
	
	/* Command line options */
	private Options options = null;

	/* The pathe to the Hazelcast config file */
	private String configPath;

	private final String usage = "report-index -p <path-to-hazelcast-config> | [-h]";

	/* The JSON object to be returned by the reporter */
	private JSONObject json;

	/* The Hazelcast configuration object */
	private FileSystemXmlConfig hzConfig = null;
	
	/** Constructor */
	public IndexReporter() {
		
	}
	
	/** Run the class */
	public static void main(String[] args) {
		log.trace("IndexReporter.main() called.");
		
		IndexReporter reporter = new IndexReporter();
		reporter.handleCommandOptions(args);
	}

	/* Handle command line options passed to the IndexReporter */
	private void handleCommandOptions(String[] args) {
		log.trace("IndexReporter.handleCommandOptions() called.");
		
		// Set up command line options
		this.options = new Options();
		this.options.addOption("h", "help", false, "Show this usage help.");
		this.options.addOption("p", "path", true, "Path to the Hazelcast config file. (Required)");
		
		// Parse the given options
		CommandLineParser parser = new DefaultParser();
		try {
			CommandLine command = parser.parse(this.options, args);
			
			// Handle the -h option
			if (command.hasOption("h") ) {
				showUsage();
				
			// Handle the -p option (required)
			} else if ( command.hasOption("p") ) {
				this.configPath = command.getOptionValue("p");
				this.json = report();
				System.out.println(this.json.toString(4));
				
			} else {
				showUsage();
				
			}
			System.exit(0);
			
		} catch (ParseException e) {
			log.error("Couldn't parse the given command line options: " +
				e.getMessage());
			System.exit(1);
			
		}
	}

	/* Prints the JSON report output to the console */
	private JSONObject report() {
		log.trace("IndexReporter.report() called.");
		
		JSONObject json = new JSONObject();
		
		// Check that we have access to the config file path
		Path path = FileSystems.getDefault().getPath(this.configPath);
		if ( Files.notExists(path) ) {
			log.error("The hazelcast configuration file does not exist at the given path.");
			System.exit(1);
			
		} else {
			// Create the Hazelcast config if we have permissions
			if ( Files.isReadable(path) ) {
				
				// Configure Hazelcast
				try {
					log.debug("Using configuration from: " + this.configPath);
					
					// Read the on-disk configuration from Metacat
					this.hzConfig = new FileSystemXmlConfig(this.configPath);
					
				} catch (FileNotFoundException e) {
					log.error("The hazelcast configuration file does not exist at the given path. " +
						e.getMessage());
					System.exit(1);
				}
				
				if ( this.hzConfig != null ) {
					
					// Build a new client configuration to connect to the Hazelcast member
					ClientConfig clientConfig = new ClientConfig();
					try {
						clientConfig.setGroupConfig(this.hzConfig.getGroupConfig());
						String host = this.hzConfig.getNetworkConfig().getInterfaces().getInterfaces().iterator().next();
						String port = new Integer(this.hzConfig.getNetworkConfig().getPort()).toString();
						String ipAndPort = host + ":" + port;
						clientConfig.addAddress(ipAndPort);
						
					} catch (RuntimeException e) {
						log.error("Couldn't configure the Hazelcast cluster. Please check the configuration file. " +
							e.getMessage());
						System.exit(1);
						
					}
					
					// Connect to Hazelcast
					try {
						HazelcastClient hzClient = HazelcastClient.newHazelcastClient(clientConfig);
						
						IMap indexQueue = hzClient.getMap("hzIndexQueue");
						int size = indexQueue.size();
						
						// Append the queue size to the JSON output
						json.put("size", size);
						
						// Append the identifier list to the JSON output, but sort it in a tree map first
						Set<Identifier> identifiers = indexQueue.keySet();
						SortedMap<Integer, String> idsMap = new TreeMap<Integer, String>();
						JSONObject idsObject = new JSONObject();
						int count = 1;
						for (Identifier identifier : identifiers) {
							idsMap.put(new Integer(count), identifier.getValue());
							count++;
						}
						
						// Add it to the ids object
						for (Integer key : idsMap.keySet()) {
							idsObject.put(key.toString(), idsMap.get(key));
						}
						json.put("identifiers", idsObject);
						
					} catch (RuntimeException re) {
						log.error("Couldn't get the Hazelcast index queue map. Please try again. " +
							re.getMessage());
						System.exit(1);
					}
				}
				
			} else {
				log.error("The hazelcast configuration file is not readable with the given permissions. "  +
					"Please be sure that someone has given your account read access to the file.");
				System.exit(1);
			}
		}
		return json;
	}

	/* Prints the command usage to the console */
	private void showUsage() {
		log.trace("IndexReporter.showUsage() called.");
		
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(this.usage , this.options);
	}

}
