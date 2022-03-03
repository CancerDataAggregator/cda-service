package bio.terra.cda.app.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class File {
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
  private String data_modality;
  private String imaging_modality;
  private String dbgap_accession_number;
  private List<Subject> Subject;
  private String vital_status;
  private String age_at_death;
  private String cause_of_death;
  private List<ResearchSubject> ResearchSubject;
  private List<Diagnosis> Diagnosis;
  private List<Treatment> treatment;
  private List<Specimen> specimen;

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

  public String getData_modality() {
    return data_modality;
  }

  public void setData_modality(String data_modality) {
    this.data_modality = data_modality;
  }

  public String getImaging_modality() {
    return imaging_modality;
  }

  public void setImaging_modality(String imaging_modality) {
    this.imaging_modality = imaging_modality;
  }

  public String getDbgap_accession_number() {
    return dbgap_accession_number;
  }

  public void setDbgap_accession_number(String dbgap_accession_number) {
    this.dbgap_accession_number = dbgap_accession_number;
  }

  public List<bio.terra.cda.app.models.Subject> getSubject() {
    return Subject;
  }

  public void setSubject(List<bio.terra.cda.app.models.Subject> subject) {
    Subject = subject;
  }

  public String getVital_status() {
    return vital_status;
  }

  public void setVital_status(String vital_status) {
    this.vital_status = vital_status;
  }

  public String getAge_at_death() {
    return age_at_death;
  }

  public void setAge_at_death(String age_at_death) {
    this.age_at_death = age_at_death;
  }

  public String getCause_of_death() {
    return cause_of_death;
  }

  public void setCause_of_death(String cause_of_death) {
    this.cause_of_death = cause_of_death;
  }

  public List<bio.terra.cda.app.models.ResearchSubject> getResearchSubject() {
    return ResearchSubject;
  }

  public void setResearchSubject(List<bio.terra.cda.app.models.ResearchSubject> researchSubject) {
    ResearchSubject = researchSubject;
  }

  public List<bio.terra.cda.app.models.Diagnosis> getDiagnosis() {
    return Diagnosis;
  }

  public void setDiagnosis(List<bio.terra.cda.app.models.Diagnosis> diagnosis) {
    Diagnosis = diagnosis;
  }

  public List<Treatment> getTreatment() {
    return treatment;
  }

  public void setTreatment(List<Treatment> treatment) {
    this.treatment = treatment;
  }

  public List<Specimen> getSpecimen() {
    return specimen;
  }

  public void setSpecimen(List<Specimen> specimen) {
    this.specimen = specimen;
  }

  @Override
  public String toString() {
    return "File{"
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
        + ", data_modality='"
        + data_modality
        + '\''
        + ", imaging_modality='"
        + imaging_modality
        + '\''
        + ", dbgap_accession_number='"
        + dbgap_accession_number
        + '\''
        + ", Subject="
        + Subject
        + ", vital_status='"
        + vital_status
        + '\''
        + ", age_at_death='"
        + age_at_death
        + '\''
        + ", cause_of_death='"
        + cause_of_death
        + '\''
        + ", ResearchSubject="
        + ResearchSubject
        + ", Diagnosis="
        + Diagnosis
        + ", treatment="
        + treatment
        + ", specimen="
        + specimen
        + '}';
  }
}
