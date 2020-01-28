/**
 * 
 */
package it.damore.solr.importexport.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/*
This file is part of solr-import-export-json.

solr-import-export-json is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

solr-import-export-json is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with solr-import-export-json.  If not, see <http://www.gnu.org/licenses/>.
*/


/**
 * @author freedev
 *
 */
public class CommandLineConfig {

  public enum ActionType {
    IMPORT("import"),
    EXPORT("export"),
    BACKUP("backup"),
    RESTORE("restore");
    
    private String name;

    private ActionType(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
    
    public static List<String> getNames() {
        return Arrays.asList(ActionType.values()).stream().map(ActionType::getName).collect(Collectors.toList());
    }

  }
  
  public static final int DEFAULT_BLOCK_SIZE = 5000;
  public static final String DEFAULT_DATETIME_FORMAT = "YYYY-MM-dd'T'HH:mm:sss'Z'";
  
  private ActionType actionType;
  private String solrUrl;
  private String user;
  private String password;
  private Boolean hasCredentials = false;
  private String fileName;
  private Boolean deleteAll;
  private Boolean disableCursors;
  private Set<SolrField> skipFieldSet = Collections.emptySet();
  private Set<SolrField> includeFieldSet = Collections.emptySet();
  private String filterQuery;
  private String uniqueKey;
  private Boolean dryRun;
  private long skipCount;
  private Integer commitAfter;
  private int blockSize = DEFAULT_BLOCK_SIZE;
  private String dateTimeFormat = DEFAULT_DATETIME_FORMAT;
  /**
   * @return the solrUrl
   */
  public String getSolrUrl() {
    return solrUrl;
  }

  /**
   * @param solrUrl the solrUrl to set
   */
  public void setSolrUrl(String solrUrl) {
    this.solrUrl = solrUrl;
  }

  /**
   * @return the dryRun
   */
  public Boolean getDryRun() {
    return dryRun;
  }

  /**
   * @param dryRun the dryRun to set
   */
  public void setDryRun(Boolean dryRun) {
    this.dryRun = dryRun;
  }

  /**
   * @return the ActionType
   */
  public ActionType getActionType() {
    return actionType;
  }

  /**
   * @param actionType the ActionType to set
   */
  public void setActionType(ActionType actionType) {
    this.actionType = actionType;
  }

  /**
   * 
   * @return fileName
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * 
   * @param fileName
   */
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  
  /**
   * @return the filterQuery
   */
  public String getFilterQuery() {
    return filterQuery;
  }

  /**
   * @param filterQuery the filterQuery to set
   */
  public void setFilterQuery(String filterQuery) {
    this.filterQuery = filterQuery;
  }

  
  /**
   * @return the deleteAll
   */
  public Boolean getDeleteAll() {
    return deleteAll;
  }

  /**
   * @param deleteAll the deleteAll to set
   */
  public void setDeleteAll(Boolean deleteAll) {
    this.deleteAll = deleteAll;
  }

  /**
   * @return the skipFieldsSet
   */
  public Set<SolrField> getSkipFieldSet() {
    return skipFieldSet;
  }

  /**
   * @param skipFieldSet the list of fields to skip
   */
  public void setSkipFieldSet(Set<SolrField> skipFieldSet) {
    this.skipFieldSet = skipFieldSet;
  }

  /**
   * @return the uniqueId
   */
  public String getUniqueKey() {
    return uniqueKey;
  }

  /**
   * @param uniqueId the uniqueId to set
   */
  public void setUniqueKey(String uniqueId) {
    this.uniqueKey = uniqueId;
  }

  
  /**
   * @return the blockSize
   */
  public int getBlockSize() {
    return blockSize;
  }

  /**
   * @param blockSize the blockSize to set
   */
  public void setBlockSize(int blockSize) {
    this.blockSize = blockSize;
  }

  
  /**
   * @return the dateTimeFormat
   */
  public String getDateTimeFormat()
  {
    return dateTimeFormat;
  }

  /**
   * @param dateTimeFormat the dateTimeFormat to set
   */
  public void setDateTimeFormat(String dateTimeFormat)
  {
    this.dateTimeFormat = dateTimeFormat;
  }

  public Set<SolrField> getIncludeFieldSet()
  {
    return includeFieldSet;
  }

  public void setIncludeFieldSet(Set<SolrField> includeFieldSet)
  {
    this.includeFieldSet = includeFieldSet;
  }
  
  public long getSkipCount() {
    return skipCount;
  }

  public void setSkipCount(long skipCount) {
    this.skipCount = skipCount;
  }


  public Integer getCommitAfter() {
    return commitAfter;
  }

  public void setCommitAfter(Integer commitAfter) {
    this.commitAfter = commitAfter;
  }

  public Boolean getDisableCursors() {
    return disableCursors;
  }

  public void setDisableCursors(Boolean disableCursors) {
    this.disableCursors = disableCursors;
  }

  public Boolean hasCredentials() {
    return hasCredentials;
  }

  public void setHasCredentials(Boolean hasCredentials) {
    this.hasCredentials = hasCredentials;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  @Override
  public String toString() {
    return "CommandLineConfig{" +
            "actionType=" + actionType +
            ", solrUrl='" + solrUrl + '\'' +
            ", fileName='" + fileName + '\'' +
            ", deleteAll=" + deleteAll +
            ", disableCursors=" + disableCursors +
            ", skipFieldSet=" + skipFieldSet +
            ", includeFieldSet=" + includeFieldSet +
            ", filterQuery='" + filterQuery + '\'' +
            ", uniqueKey='" + uniqueKey + '\'' +
            ", dryRun=" + dryRun +
            ", skipCount=" + skipCount +
            ", commitAfter=" + commitAfter +
            ", blockSize=" + blockSize +
            ", dateTimeFormat='" + dateTimeFormat + '\'' +
            ", user='" + user + '\'' +
            ", password='**********'" +
            '}';
  }
}
