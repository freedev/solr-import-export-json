package it.damore.solr.backuprestore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import it.damore.solr.backuprestore.Config.ActionType;

/**
 * Hello world!
 */
public class App {

  private static Logger logger = LoggerFactory.getLogger(App.class);
  
  private static Config config = null;

  private static ObjectMapper objectMapper = new ObjectMapper();

  
  public static void main(String[] args) throws IOException, ParseException, URISyntaxException {

    DateFormat df = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:sss'Z'");
    objectMapper.setDateFormat(df);
    config = getConfigFromArgs(args);
    
    try (HttpSolrClient client = new HttpSolrClient(config.getSolrUrl())) {

      try {
        switch (config.getActionType()) {
          case BACKUP:
            
            readAllDocuments(client, getFile(config));
            break;

          case RESTORE:
            
            writeAllDocuments(client, getFile(config));
            break;

          default:
            throw new RuntimeException("unsupported sitemap type");
        }


        logger.info("Build complete.");

      } catch (SolrServerException | IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } 
    }

  }

  private static void writeAllDocuments(HttpSolrClient client, File outputFile) throws FileNotFoundException, IOException, SolrServerException {
    // TODO Auto-generated method stub
    int BATCH = 200;

    client.deleteByQuery("*:*");
    
    logger.info("Reading " + config.getFileName());
    
    try (BufferedReader pw = new BufferedReader(new FileReader(outputFile))) {
      pw.lines().collect(StreamUtils.batchCollector(BATCH, l -> {
        List<SolrInputDocument> collect = l.stream().map(j -> { 
          SolrInputDocument s = new SolrInputDocument();
          try {
            Map <String, Object> map = new HashMap<String, Object>();
            map = objectMapper.readValue(j, new TypeReference<Map<String, Object>>(){});
            for(Entry<String, Object> e : map.entrySet()) {
              if (!e.getKey().equals("_version_"))
                s.addField(e.getKey(), e.getValue());
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return s;
        }).collect(Collectors.toList());
        try {
          client.add(collect);
        } catch (SolrServerException | IOException e) {
          throw new RuntimeException(e);
        }
      }));
    }

    client.commit();
       
  }

  
  private static void readAllDocuments(HttpSolrClient client, File outputFile) throws SolrServerException,
                                                                                 IOException {

    SolrQuery solrQuery = new SolrQuery();
    solrQuery.setQuery("*:*");
    solrQuery.setRows(0);
    solrQuery.addSort("id", ORDER.asc); // Pay attention to this line
 
    String cursorMark = CursorMarkParams.CURSOR_MARK_START;

    objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    
    QueryResponse r = client.query(solrQuery);

    logger.info("Found " + r.getResults().getNumFound() + " documents");

    if (!config.getDryRun()) {
      logger.info("Creating " + config.getFileName());

      try (PrintWriter pw = new PrintWriter(outputFile)) {
        solrQuery.setRows(200);
        boolean done = false;
        while (!done) {
          solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
          QueryResponse rsp = client.query(solrQuery);
          String nextCursorMark = rsp.getNextCursorMark();
          for (SolrDocument d : rsp.getResults()) {
            pw.write(objectMapper.writeValueAsString(d));
            pw.write("\n");
          }
          if (cursorMark.equals(nextCursorMark)) {
            done = true;
          }

          cursorMark = nextCursorMark;
        }
        
      }
    }
    
  }
  
  
  private static File getFile(Config config) {
      return new File(config.getFileName());
  }


  private static Config getConfigFromArgs(String[] args) throws ParseException {
    CommandLine cmd = parseCommandLine(args);
    String solrUrl = cmd.getOptionValue("solrUrl");
    String file = cmd.getOptionValue("file");
    String dryRun = cmd.getOptionValue("dryRun");
    String actionType = cmd.getOptionValue("actionType");
    
    if (actionType == null) {
      throw new MissingArgumentException("actionType should be [" + String.join("|", ActionType.getNames())
  + "]");
    }

    if (solrUrl == null) {
      throw new MissingArgumentException("solrUrl missing");
    }

    Config c = new Config();
    c.setSolrUrl(solrUrl);
    c.setFileName(file);

    for (ActionType o : ActionType.values()) {
      if (actionType.equalsIgnoreCase(o.toString())) {
        c.setActionType(o);
        break;
      }
    }
    if (c.getActionType() == null) {
      throw new MissingArgumentException("actionType should be [" + String.join("|", ActionType.getNames())
  + "]");
    }

    c.setDryRun(dryRun != null);
    
    logger.info("Current configuration " + c);
    
    return c;
  }


  private static CommandLine parseCommandLine(String[] args) throws ParseException {
    Options cliOptions = new Options();
    cliOptions.addOption("s", "solrUrl", true, "solr url");
    cliOptions.addOption("a", "actionType", true, "action type [" + String.join("|", ActionType.getNames())
    + "]");
    cliOptions.addOption("f", "file", true, "file");
    cliOptions.addOption("d", "dryRun", false, "dryrun");
    cliOptions.addOption("h", "help", false, "help");
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(cliOptions, args);

    if (cmd.hasOption("help") || cmd.hasOption("h")) {
      String header = "solr-backup-restore-json\n\n";
      String footer = "\nPlease report issues at https://github.com/freedev/solr-backup-restore-json";

      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("myapp", header, cliOptions, footer, true);
      System.exit(0);
    }

    return cmd;
  }


}
