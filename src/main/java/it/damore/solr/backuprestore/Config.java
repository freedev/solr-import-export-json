/**
 * 
 */
package it.damore.solr.backuprestore;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/*
This file is part of solr-backup-restore-json.

solr-backup-restore-json is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

solr-backup-restore-json is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with solr-backup-restore-json.  If not, see <http://www.gnu.org/licenses/>.
*/


/**
 * @author freedev
 *
 */
public class Config {

  public enum ActionType {
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
  
  private ActionType actionType;
  private String solrUrl;
  private String fileName;
  private Boolean deleteAll;
  private Set<String> skipFieldsSet = Collections.emptySet();
  private String filterQuery;
  private String uniqueKey;
  private Boolean dryRun;
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
   * @param ActionType the ActionType to set
   */
  public void setActionType(ActionType actionType) {
    this.actionType = actionType;
  }

  /**
   * 
   * @return
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
  public Set<String> getSkipFieldsSet() {
    return skipFieldsSet;
  }

  /**
   * @param skipFieldsSet the skipFieldsSet to set
   */
  public void setSkipFieldsSet(Set<String> skipFieldsSet) {
    this.skipFieldsSet = skipFieldsSet;
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

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "Config [actionType=" + actionType + ", solrUrl=" + solrUrl + ", fileName=" + fileName + ", deleteAll="
           + deleteAll + ", skipFieldsSet=" + skipFieldsSet + ", filterQuery=" + filterQuery + ", uniqueKey="
           + uniqueKey + ", dryRun=" + dryRun + "]";
  }

}
