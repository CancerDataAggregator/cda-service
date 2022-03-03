package bio.terra.cda.app.models;

import java.util.List;

public class LegacyFile {
  private String id;
  private List<Identifier> identifier;
  private String label;
  private String data_category;
  private String data_type;
  private String file_format;
  private String associated_project;
  private String drs_uri;
  private String byte_size;
  private String checksum;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public List<Identifier> getIdentifier() {
    return identifier;
  }

  public void setIdentifier(List<Identifier> identifier) {
    this.identifier = identifier;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public String getData_category() {
    return data_category;
  }

  public void setData_category(String data_category) {
    this.data_category = data_category;
  }

  public String getData_type() {
    return data_type;
  }

  public void setData_type(String data_type) {
    this.data_type = data_type;
  }

  public String getFile_format() {
    return file_format;
  }

  public void setFile_format(String file_format) {
    this.file_format = file_format;
  }

  public String getAssociated_project() {
    return associated_project;
  }

  public void setAssociated_project(String associated_project) {
    this.associated_project = associated_project;
  }

  public String getDrs_uri() {
    return drs_uri;
  }

  public void setDrs_uri(String drs_uri) {
    this.drs_uri = drs_uri;
  }

  public String getByte_size() {
    return byte_size;
  }

  public void setByte_size(String byte_size) {
    this.byte_size = byte_size;
  }

  public String getChecksum() {
    return checksum;
  }

  public void setChecksum(String checksum) {
    this.checksum = checksum;
  }

  @Override
  public String toString() {
    return "LegacyFile{"
        + "id='"
        + id
        + '\''
        + ", identifier="
        + identifier
        + ", label='"
        + label
        + '\''
        + ", data_category='"
        + data_category
        + '\''
        + ", data_type='"
        + data_type
        + '\''
        + ", file_format='"
        + file_format
        + '\''
        + ", associated_project='"
        + associated_project
        + '\''
        + ", drs_uri='"
        + drs_uri
        + '\''
        + ", byte_size='"
        + byte_size
        + '\''
        + ", checksum='"
        + checksum
        + '\''
        + '}';
  }
}
