package it.damore.solr.importexport.config;

public class SolrField {
  
  private String text;
  
  private MatchType match = MatchType.EQUAL;
  
  public enum MatchType {
    EQUAL,
    STARTS_WITH,
    ENDS_WITH
  }
  
  public SolrField(String text, MatchType match) {
    this.text = text;
    this.match = match;
  }

  /**
   * @return the text
   */
  public String getText() {
    return text;
  }

  /**
   * @param text the text to set
   */
  public void setText(String text) {
    this.text = text;
  }

  /**
   * @return the match
   */
  public MatchType getMatch() {
    return match;
  }

  /**
   * @param match the match to set
   */
  public void setMatch(MatchType match) {
    this.match = match;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "SkipField [text=" + text + ", match=" + match + "]";
  }

}
