package it.damore.solr.importexport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import it.damore.solr.importexport.config.CommandLineConfig;
import it.damore.solr.importexport.config.ConfigFactory;
import it.damore.solr.importexport.config.SkipField;
import it.damore.solr.importexport.config.SkipField.MatchType;

/*
 * This file is part of solr-import-export-json. solr-import-export-json is free software: you can redistribute it
 * and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version. solr-import-export-json is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a
 * copy of the GNU General Public License along with solr-import-export-json. If not, see
 * <http://www.gnu.org/licenses/>.
 */

/**
 * @author freedev Import and Export of a Solr collection
 */
public class App {

  private static Logger            logger       = LoggerFactory.getLogger(App.class);
  private static CommandLineConfig config       = null;
  private static ObjectMapper      objectMapper = new ObjectMapper();
  private static long              counter;

  private static Set<SkipField> skipFieldsEquals;
  private static Set<SkipField> skipFieldsStartWith;
  private static Set<SkipField> skipFieldsEndWith;

  /**
   * @param counter
   *          the counter to set
   */
  public static long incrementCounter(long counter) {
    App.counter += counter;
    return App.counter;
  }

  public static void main(String[] args) throws IOException, ParseException, URISyntaxException {

    config = ConfigFactory.getConfigFromArgs(args);

    skipFieldsEquals = config.getSkipFieldsSet()
                             .stream()
                             .filter(s -> s.getMatch() == MatchType.EQUAL)
                             .collect(Collectors.toSet());
    skipFieldsStartWith = config.getSkipFieldsSet()
                                .stream()
                                .filter(s -> s.getMatch() == MatchType.STARTS_WITH)
                                .collect(Collectors.toSet());
    skipFieldsEndWith = config.getSkipFieldsSet()
                              .stream()
                              .filter(s -> s.getMatch() == MatchType.ENDS_WITH)
                              .collect(Collectors.toSet());

    logger.info("Found config: " + config);

    if (config.getUniqueKey() == null) {
      readUniqueKeyFromSolrSchema();
    }

    try (HttpSolrClient client = new HttpSolrClient(config.getSolrUrl())) {

      try {
        switch (config.getActionType()) {
        case EXPORT:
        case BACKUP:

          readAllDocuments(client, new File(config.getFileName()));
          break;

        case RESTORE:
        case IMPORT:

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

  /**
   * @throws IOException
   * @throws JsonParseException
   * @throws JsonMappingException
   * @throws MalformedURLException
   */

  private static void readUniqueKeyFromSolrSchema() throws IOException, JsonParseException, JsonMappingException, MalformedURLException {
    String sUrl = config.getSolrUrl() + "/schema/uniquekey?wt=json";
    Map<String, Object> uniqueKey = objectMapper.readValue(readUrl(sUrl), new TypeReference<Map<String, Object>>() {
    });
    if (uniqueKey.containsKey("uniqueKey")) {
      config.setUniqueKey((String) uniqueKey.get("uniqueKey"));
    } else {
      logger.warn("unable to find valid uniqueKey defaulting to \"id\".");
    }
  }

  /**
   * @param sUrl
   * @return
   * @throws MalformedURLException
   * @throws IOException
   */
  private static String readUrl(String sUrl) throws MalformedURLException, IOException {
    StringBuilder sbJson = new StringBuilder();
    URL url = new URL(sUrl);
    String userInfo = url.getUserInfo();
    URLConnection openConnection = url.openConnection();
    if (userInfo != null && !userInfo.isEmpty()) {
      String authStr = Base64.getEncoder()
                             .encodeToString(userInfo.getBytes());
      openConnection.setRequestProperty("Authorization", "Basic " + authStr);
    }

    BufferedReader in = new BufferedReader(new InputStreamReader(openConnection.getInputStream()));
    String inputLine;
    while ((inputLine = in.readLine()) != null)
      sbJson.append(inputLine);
    in.close();
    return sbJson.toString();
  }

  /**
   * @param j
   * @return
   */
  private static SolrInputDocument json2SolrInputDocument(String j) {
    SolrInputDocument s = new SolrInputDocument();
    try {
      Map<String, Object> map = objectMapper.readValue(j, new TypeReference<Map<String, Object>>() {
      });
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

  /**
   * @param client
   * @param outputFile
   * @throws FileNotFoundException
   * @throws IOException
   * @throws SolrServerException
   */
  private static void writeAllDocuments(HttpSolrClient client, File outputFile) throws FileNotFoundException, IOException,
                                                                                SolrServerException {
    if (skipFieldsStartWith.size() > 0 || skipFieldsEndWith.size() > 0) {
      throw new RuntimeException("skipFieldsStartWith and skipFieldsEndWith are not supported at writing time");
    }
    if (!config.getDryRun() && config.getDeleteAll()) {
      logger.info("delete all!");
      client.deleteByQuery("*:*");
    }
    logger.info("Reading " + config.getFileName());

    try (BufferedReader pw = new BufferedReader(new FileReader(outputFile))) {
      pw.lines()
        .collect(StreamUtils.batchCollector(config.getBlockSize(), l -> {
          List<SolrInputDocument> collect = l.stream()
                                             .map(App::json2SolrInputDocument)
                                             .map(d -> {
                                               skipFieldsEquals.forEach(f -> d.removeField(f.getText()));
                                               return d;
                                             })
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

  /**
   * @param client
   * @param outputFile
   * @throws SolrServerException
   * @throws IOException
   */
  private static void readAllDocuments(HttpSolrClient client, File outputFile) throws SolrServerException, IOException {

    SolrQuery solrQuery = new SolrQuery();
    solrQuery.setQuery("*:*");
    if (config.getFilterQuery() != null) {
      solrQuery.addFilterQuery(config.getFilterQuery());
    }
    solrQuery.setRows(0);

    solrQuery.addSort(config.getUniqueKey(), ORDER.asc); // Pay attention to this line

    String cursorMark = CursorMarkParams.CURSOR_MARK_START;

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

    // objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
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
            skipFieldsEquals.forEach(f -> d.removeFields(f.getText()));
            if (skipFieldsStartWith.size() > 0 || skipFieldsEndWith.size() > 0) {
              Map<String, Object> collect = d.entrySet()
                                             .stream()
                                             .filter(e -> !skipFieldsStartWith.stream()
                                                                              .filter(f -> e.getKey()
                                                                                            .startsWith(f.getText()))
                                                                              .findFirst()
                                                                              .isPresent())
                                             .filter(e -> !skipFieldsEndWith.stream()
                                                                            .filter(f -> e.getKey()
                                                                                          .endsWith(f.getText()))
                                                                            .findFirst()
                                                                            .isPresent())
                                             .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
              pw.write(objectMapper.writeValueAsString(collect));
            } else {
              pw.write(objectMapper.writeValueAsString(d));
            }
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

}
