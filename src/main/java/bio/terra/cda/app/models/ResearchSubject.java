package bio.terra.cda.app.models;

import java.util.List;

public class ResearchSubject {
  private String id;
  private Identifier identifier;
  private String member_of_research_project;
  private String primary_diagnosis_condition;
  private String primary_diagnosis_site;
  private List<Diagnosis> Diagnosis;
  private List<Specimen> Specimen;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Identifier getIdentifier() {
    return identifier;
  }

  public void setIdentifier(Identifier identifier) {
    this.identifier = identifier;
  }

  public String getMember_of_research_project() {
    return member_of_research_project;
  }

  public void setMember_of_research_project(String member_of_research_project) {
    this.member_of_research_project = member_of_research_project;
  }

  public String getPrimary_diagnosis_condition() {
    return primary_diagnosis_condition;
  }

  public void setPrimary_diagnosis_condition(String primary_diagnosis_condition) {
    this.primary_diagnosis_condition = primary_diagnosis_condition;
  }

  public String getPrimary_diagnosis_site() {
    return primary_diagnosis_site;
  }

  public void setPrimary_diagnosis_site(String primary_diagnosis_site) {
    this.primary_diagnosis_site = primary_diagnosis_site;
  }

  public List<bio.terra.cda.app.models.Diagnosis> getDiagnosis() {
    return Diagnosis;
  }

  public void setDiagnosis(List<bio.terra.cda.app.models.Diagnosis> diagnosis) {
    Diagnosis = diagnosis;
  }

  public List<bio.terra.cda.app.models.Specimen> getSpecimen() {
    return Specimen;
  }

  public void setSpecimen(List<bio.terra.cda.app.models.Specimen> specimen) {
    Specimen = specimen;
  }

  @Override
  public String toString() {
    return "ResearchSubject{"
        + "id='"
        + id
        + '\''
        + ", identifier="
        + identifier
        + ", member_of_research_project='"
        + member_of_research_project
        + '\''
        + ", primary_diagnosis_condition='"
        + primary_diagnosis_condition
        + '\''
        + ", primary_diagnosis_site='"
        + primary_diagnosis_site
        + '\''
        + ", Diagnosis="
        + Diagnosis
        + ", Specimen="
        + Specimen
        + '}';
  }
}
