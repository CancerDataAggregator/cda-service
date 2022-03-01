package bio.terra.cda.app.models;

import java.util.List;

public class Diagnosis {
  private String id;
  private Identifier identifier;
  private String primary_diagnosis;
  private String age_at_diagnosis;
  private String morphology;
  private String stage;
  private String grade;
  private String method_of_diagnosis;
  private List<Treatment> Treatment;

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

  public String getPrimary_diagnosis() {
    return primary_diagnosis;
  }

  public void setPrimary_diagnosis(String primary_diagnosis) {
    this.primary_diagnosis = primary_diagnosis;
  }

  public String getAge_at_diagnosis() {
    return age_at_diagnosis;
  }

  public void setAge_at_diagnosis(String age_at_diagnosis) {
    this.age_at_diagnosis = age_at_diagnosis;
  }

  public String getMorphology() {
    return morphology;
  }

  public void setMorphology(String morphology) {
    this.morphology = morphology;
  }

  public String getStage() {
    return stage;
  }

  public void setStage(String stage) {
    this.stage = stage;
  }

  public String getGrade() {
    return grade;
  }

  public void setGrade(String grade) {
    this.grade = grade;
  }

  public String getMethod_of_diagnosis() {
    return method_of_diagnosis;
  }

  public void setMethod_of_diagnosis(String method_of_diagnosis) {
    this.method_of_diagnosis = method_of_diagnosis;
  }

  public List<bio.terra.cda.app.models.Treatment> getTreatment() {
    return Treatment;
  }

  public void setTreatment(List<bio.terra.cda.app.models.Treatment> treatment) {
    Treatment = treatment;
  }

  @Override
  public String toString() {
    return "Diagnosis{"
        + "id='"
        + id
        + '\''
        + ", identifier="
        + identifier
        + ", primary_diagnosis='"
        + primary_diagnosis
        + '\''
        + ", age_at_diagnosis='"
        + age_at_diagnosis
        + '\''
        + ", morphology='"
        + morphology
        + '\''
        + ", stage='"
        + stage
        + '\''
        + ", grade='"
        + grade
        + '\''
        + ", method_of_diagnosis='"
        + method_of_diagnosis
        + '\''
        + ", Treatment="
        + Treatment
        + '}';
  }
}
