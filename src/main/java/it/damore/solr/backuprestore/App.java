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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.regex.Pattern;
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

/*
 * This file is part of solr-backup-restore-json. solr-backup-restore-json is free software: you can redistribute it
 * and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version. solr-backup-restore-json is distributed in
 * the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a
 * copy of the GNU General Public License along with solr-backup-restore-json. If not, see
 * <http://www.gnu.org/licenses/>.
 */


/**
 * @author freedev Backups and restore a Solr collection
 */
public class App {

  private static final String[] SOLR_URL     = new String[] {"s", "solrUrl"};
  private static final String[] ACTION_TYPE  = new String[] {"a", "actionType"};
  private static final String[] OUTPUT       = new String[] {"o", "output"};
  private static final String[] DELETE_ALL   = new String[] {"d", "deleteAll"};
  private static final String[] FILTER_QUERY = new String[] {"f", "filterQuery"};
  private static final String[] HELP         = new String[] {"h", "help"};
  private static final String[] DRY_RUN      = new String[] {"D", "dryRun"};
  private static final String[] UNIQUE_ID    = new String[] {"i", "uniqueId"};
  private static final String[] SKIP_FIELDS  = new String[] {"S", "skipFields"};

  private static Logger         logger       = LoggerFactory.getLogger(App.class);
  private static Config         config       = null;
  private static ObjectMapper   objectMapper = new ObjectMapper();
  private static long           counter;


  /**
   * @param counter the counter to set
   */
  public static long incrementCounter(long counter) {
    App.counter += counter;
    return App.counter;
  }

  public static void main(String[] args) throws IOException, ParseException, URISyntaxException {

    config = getConfigFromArgs(args);

    logger.info("Found config: " + config);

    try (HttpSolrClient client = new HttpSolrClient(config.getSolrUrl())) {

      try {
        switch (config.getActionType()) {
          case BACKUP:

            readAllDocuments(client, new File(config.getFileName()));
            break;

          case RESTORE:

            writeAllDocuments(client, new File(config.getFileName()));
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

  private static SolrInputDocument json2SolrInputDocument(String j) {
    SolrInputDocument s = new SolrInputDocument();
    try {
      Map<String, Object> map = objectMapper.readValue(j, new TypeReference<Map<String, Object>>() {});
      for (Entry<String, Object> e : map.entrySet()) {
        if (!e.getKey()
              .equals("_version_"))
          s.addField(e.getKey(), e.getValue());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return s;
  }

  private static void writeAllDocuments(HttpSolrClient client, File outputFile) throws FileNotFoundException,
                                                                                IOException, SolrServerException {
    int BATCH = 200;

    if (!config.getDryRun() && config.getDeleteAll()) {
      logger.info("delete all!");
      client.deleteByQuery("*:*");
    }
    logger.info("Reading " + config.getFileName());

    try (BufferedReader pw = new BufferedReader(new FileReader(outputFile))) {
      pw.lines()
        .collect(StreamUtils.batchCollector(BATCH, l -> {
          List<SolrInputDocument> collect = l.stream()
                                             .map(App::json2SolrInputDocument)
                                             .collect(Collectors.toList());
          try {
            if (!config.getDryRun()) {
              logger.info("adding " + collect.size() + " documents (" + incrementCounter(collect.size()) + ")");
              client.add(collect);
            }
          } catch (SolrServerException | IOException e) {
            throw new RuntimeException(e);
          }
        }));
    }

    if (!config.getDryRun()) {
      logger.info("Commit");
      client.commit();
    }

  }


  private static void readAllDocuments(HttpSolrClient client, File outputFile) throws SolrServerException, IOException {

    SolrQuery solrQuery = new SolrQuery();
    solrQuery.setQuery("*:*");
    if (config.getFilterQuery() != null) {
      solrQuery.addFilterQuery(config.getFilterQuery());
    }
    solrQuery.setRows(0);
    solrQuery.addSort("id", ORDER.asc); // Pay attention to this line

    String cursorMark = CursorMarkParams.CURSOR_MARK_START;

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    
//    objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
    DateFormat df = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:sss'Z'");
    objectMapper.setDateFormat(df);
    objectMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    QueryResponse r = client.query(solrQuery);

    long nDocuments = r.getResults()
                       .getNumFound();
    logger.info("Found " + nDocuments + " documents");

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
            config.getSkipFieldsSet().forEach(s -> d.removeFields(s));
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

  /**
   * read parameters from args
   * 
   * @param args
   * @return Config
   * @throws ParseException
   */
  private static Config getConfigFromArgs(String[] args) throws ParseException {
    CommandLine cmd = parseCommandLine(args);
    String solrUrl = cmd.getOptionValue(SOLR_URL[1]);
    String skipFields = cmd.getOptionValue(SKIP_FIELDS[1]);
    String file = cmd.getOptionValue(OUTPUT[1]);
    String filterQuery = cmd.getOptionValue(FILTER_QUERY[1]);
    String uniqueId = cmd.getOptionValue(UNIQUE_ID[1]);
    Boolean deleteAll = cmd.hasOption(DELETE_ALL[1]);
    Boolean dryRun = cmd.hasOption(DRY_RUN[1]);
    String actionType = cmd.getOptionValue(ACTION_TYPE[1]);

    if (actionType == null) {
      throw new MissingArgumentException("actionType should be [" + String.join("|", ActionType.getNames()) + "]");
    }

    if (solrUrl == null) {
      throw new MissingArgumentException("solrUrl missing");
    }
    
    Config c = new Config();
    c.setSolrUrl(solrUrl);
    c.setFileName(file);

    if (uniqueId != null) {
      c.setUniqueId(uniqueId);
    } else {
      c.setUniqueId("Id");
    }


    if (skipFields != null) {
      c.setSkipFieldsSet(Pattern.compile(",").splitAsStream(skipFields).collect(Collectors.toSet()));
    }

    for (ActionType o : ActionType.values()) {
      if (actionType.equalsIgnoreCase(o.toString())) {
        c.setActionType(o);
        break;
      }
    }
    if (c.getActionType() == null) {
      throw new MissingArgumentException("actionType should be [" + String.join("|", ActionType.getNames()) + "]");
    }

    c.setFilterQuery(filterQuery);

    c.setDeleteAll(deleteAll);

    c.setDryRun(dryRun);

    logger.info("Current configuration " + c);

    return c;
  }

  /**
   * Parse command line
   * 
   * @param args
   * @return
   * @throws ParseException
   */
  private static CommandLine parseCommandLine(String[] args) throws ParseException {
    Options cliOptions = new Options();
    cliOptions.addOption(SOLR_URL[0], SOLR_URL[1], true, "solr url");
    cliOptions.addOption(ACTION_TYPE[0], ACTION_TYPE[1], true,
                         "action type [" + String.join("|", ActionType.getNames()) + "]");
    cliOptions.addOption(OUTPUT[0], OUTPUT[1], true, "output file");
    cliOptions.addOption(DELETE_ALL[0], DELETE_ALL[1], false, "delete all documents before restore");
    cliOptions.addOption(FILTER_QUERY[0], FILTER_QUERY[1], true, "filter Query during backup");
    cliOptions.addOption(UNIQUE_ID[0], UNIQUE_ID[1], true, "specify unique Id for deep paging");
    cliOptions.addOption(DRY_RUN[0], DRY_RUN[1], false, "dry run test");
    cliOptions.addOption(SKIP_FIELDS[0], SKIP_FIELDS[1], true, "comma separated fields list to skip during backup/restore");
    cliOptions.addOption(HELP[0], HELP[1], false, "help");
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
