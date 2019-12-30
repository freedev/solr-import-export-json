package it.damore.solr.importexport.config;

import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.damore.solr.importexport.config.CommandLineConfig.ActionType;
import it.damore.solr.importexport.config.SolrField.MatchType;

public class ConfigFactory {

  private static Logger         logger             = LoggerFactory.getLogger(ConfigFactory.class);

  private static final String[] BLOCK_SIZE         = new String[] {"b", "blockSize"};
  private static final String[] SKIP_DOCS          = new String[] {"x", "skipCount"};
  private static final String[] COMMIT_DURING_WORK = new String[] {"c", "commitDuringImport"};
  private static final String[] DISABLE_CURSORS    = new String[] {"D", "disableCursors"};
  private static final String[] SOLR_URL           = new String[] {"s", "solrUrl"};
  private static final String[] ACTION_TYPE        = new String[] {"a", "actionType"};
  private static final String[] OUTPUT             = new String[] {"o", "output"};
  private static final String[] DELETE_ALL         = new String[] {"d", "deleteAll"};
  private static final String[] FILTER_QUERY       = new String[] {"f", "filterQuery"};
  private static final String[] HELP               = new String[] {"h", "help"};
  private static final String[] DRY_RUN            = new String[] {"D", "dryRun"};
  private static final String[] UNIQUE_KEY         = new String[] {"k", "uniqueKey"};
  private static final String[] SKIP_FIELDS        = new String[] {"S", "skipFields"};
  private static final String[] INCLUDE_FIELDS     = new String[] {"i", "includeFields"};
  private static final String[] DATETIME_FORMAT    = new String[] {"F", "dateTimeFormat"};
  private static final String[] USER               = new String[] {"u", "user"};
  private static final String[] PASSWORD           = new String[] {"p", "password"};


  /**
   * read parameters from args
   * 
   * @param args
   * @return Config
   * @throws ParseException
   */
  public static CommandLineConfig getConfigFromArgs(String[] args) throws ParseException
  {
    CommandLine cmd = parseCommandLine(args);
    String solrUrl = cmd.getOptionValue(SOLR_URL[1]);
    String skipFields = cmd.getOptionValue(SKIP_FIELDS[1]);
    String includeFields = cmd.getOptionValue(INCLUDE_FIELDS[1]);
    String file = cmd.getOptionValue(OUTPUT[1]);
    String filterQuery = cmd.getOptionValue(FILTER_QUERY[1]);
    String uniqueKey = cmd.getOptionValue(UNIQUE_KEY[1]);
    Boolean deleteAll = cmd.hasOption(DELETE_ALL[1]);
    Boolean disableCursors = cmd.hasOption(DISABLE_CURSORS[1]);
    Boolean dryRun = cmd.hasOption(DRY_RUN[1]);
    String actionType = cmd.getOptionValue(ACTION_TYPE[1]);
    String blockSize = cmd.getOptionValue(BLOCK_SIZE[1]);
    String skipCount = cmd.getOptionValue(SKIP_DOCS[1]);
    String commitAfter = cmd.getOptionValue(COMMIT_DURING_WORK[1]);
    String dateTimeFormat = cmd.getOptionValue(DATETIME_FORMAT[1]);
    String user = cmd.getOptionValue(USER[1]);
    String password = cmd.getOptionValue(PASSWORD[1]);

    if (actionType == null) {
      throw new MissingArgumentException("actionType should be [" + String.join("|", ActionType.getNames()) + "]");
    }

    if (solrUrl == null) {
      throw new MissingArgumentException("solrUrl missing");
    }

    CommandLineConfig c = new CommandLineConfig();
    c.setSolrUrl(solrUrl);
    c.setFileName(file);

    if (uniqueKey != null) {
      c.setUniqueKey(uniqueKey);
    }

    if (skipFields != null) {
      c.setSkipFieldSet(convertStringToSolrFieldSet(skipFields));
    }

    if (includeFields != null) {
      c.setIncludeFieldSet(convertStringToSolrFieldSet(includeFields));
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

    c.setDisableCursors(disableCursors);

    c.setDryRun(dryRun);
    if (skipCount != null) {
      c.setSkipCount(Long.valueOf(skipCount));
    }

    if (commitAfter != null) {
      c.setCommitAfter(Integer.valueOf(commitAfter));
    }

    if (blockSize != null) {
      c.setBlockSize(Integer.parseInt(blockSize));
    }

    if (dateTimeFormat != null) {
      c.setDateTimeFormat(dateTimeFormat);
    }

    if (user != null && password != null) {
      c.setUser(user);
      c.setPassword(password);
      c.setHasCredentials(true);
    }

    logger.info("Current configuration " + c);

    return c;
  }

  /**
   * @param includeFields
   * @return
   */
  private static Set<SolrField> convertStringToSolrFieldSet(String includeFields)
  {
    return Pattern.compile(",")
                  .splitAsStream(includeFields)
                  .map(String::trim)
                  .filter(s -> !s.equals("*"))
                  .map(s ->
                    {
                      if (s.startsWith("*")) {
                        return new SolrField(s.substring(1), MatchType.ENDS_WITH);
                      } else if (s.endsWith("*")) {
                        return new SolrField(s.substring(0, s.length() - 1), MatchType.STARTS_WITH);
                      } else
                        return new SolrField(s, MatchType.EQUAL);
                    })
                  .collect(Collectors.toSet());
  }

  /**
   * Parse command line
   * 
   * @param args
   * @return
   * @throws ParseException
   */
  private static CommandLine parseCommandLine(String[] args) throws ParseException
  {
    Options cliOptions = new Options();
    cliOptions.addOption(SOLR_URL[0], SOLR_URL[1], true, "solr url - http://localhost:8983/solr/collection_name");
    cliOptions.addOption(ACTION_TYPE[0], ACTION_TYPE[1], true, "action type [" + String.join("|", ActionType.getNames()) + "]");
    cliOptions.addOption(OUTPUT[0], OUTPUT[1], true, "output file");
    cliOptions.addOption(DELETE_ALL[0], DELETE_ALL[1], false, "delete all documents before import");
    cliOptions.addOption(DISABLE_CURSORS[0], DISABLE_CURSORS[1], false, "disable Solr cursors while reading");
    cliOptions.addOption(FILTER_QUERY[0], FILTER_QUERY[1], true, "filter Query during export");
    cliOptions.addOption(UNIQUE_KEY[0], UNIQUE_KEY[1], true, "specify unique key for deep paging");
    cliOptions.addOption(DRY_RUN[0], DRY_RUN[1], false, "dry run test");
    cliOptions.addOption(INCLUDE_FIELDS[0], INCLUDE_FIELDS[1], true, "simple comma separated fields list to be used during export. if not specified all the existing fields are used");
    cliOptions.addOption(SKIP_FIELDS[0], SKIP_FIELDS[1], true,
                         "comma separated fields list to skip during export/import, this field list accepts for each field prefix/suffix a wildcard *. So you can specify skip all fields starting with name_*");
    cliOptions.addOption(BLOCK_SIZE[0], BLOCK_SIZE[1], true, "block size (default " + CommandLineConfig.DEFAULT_BLOCK_SIZE + " documents)");
    cliOptions.addOption(SKIP_DOCS[0], SKIP_DOCS[1], true,
                         "Number of documents to be skipped when loading from file. Useful when an error occurs, " + "so loading can continue from last successful save." + " ");
    cliOptions.addOption(COMMIT_DURING_WORK[0], COMMIT_DURING_WORK[1], true, "Commit progress after specified number of docs. If not specified, " + "whole work will be committed.");
    cliOptions.addOption(DATETIME_FORMAT[0], DATETIME_FORMAT[1], true, "set custom DateTime format (default " + CommandLineConfig.DEFAULT_DATETIME_FORMAT + " )");
    cliOptions.addOption(HELP[0], HELP[1], false, "help");
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(cliOptions, args);

    if (cmd.hasOption("help") || cmd.hasOption("h")) {
      String header = "solr-import-export-json\n\n";
      String footer = "\nPlease report issues at https://github.com/freedev/solr-import-export-json";

      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("myapp", header, cliOptions, footer, true);
      System.exit(0);
    }

    return cmd;
  }

}
