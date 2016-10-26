/**
 * 
 */
package it.damore.solr.backuprestore;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
  
  private String solrUrl;
  private Boolean dryRun;
  private String fileName;
  private ActionType actionType;
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
   * @return the OutputType
   */
  public ActionType getActionType() {
    return actionType;
  }

  /**
   * @param OutputType the OutputType to set
   */
  public void setActionType(ActionType actionType) {
    this.actionType = actionType;
  }

  /**
   * @return the collectionName
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * @param collectionName the collectionName to set
   */
  public void setFileName(String inputFileName) {
    this.fileName = inputFileName;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "Config [solrUrl=" + solrUrl + ", dryRun=" + dryRun + ", fileName=" + fileName + ", OutputType=" + actionType + "]";
  }
  
}
