package bio.terra.cda.app.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Subject {
  private String id;
  private List<Identifier> identifier;
  private String species;
  private String sex;
  private String race;
  private String ethnicity;
  private String days_to_birth;
  private List<String> subject_associated_project;

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

  public String getSpecies() {
    return species;
  }

  public void setSpecies(String species) {
    this.species = species;
  }

  public String getSex() {
    return sex;
  }

  public void setSex(String sex) {
    this.sex = sex;
  }

  public String getRace() {
    return race;
  }

  public void setRace(String race) {
    this.race = race;
  }

  public String getEthnicity() {
    return ethnicity;
  }

  public void setEthnicity(String ethnicity) {
    this.ethnicity = ethnicity;
  }

  public String getDays_to_birth() {
    return days_to_birth;
  }

  public void setDays_to_birth(String days_to_birth) {
    this.days_to_birth = days_to_birth;
  }

  public List<String> getSubject_associated_project() {
    return subject_associated_project;
  }

  public void setSubject_associated_project(List<String> subject_associated_project) {
    this.subject_associated_project = subject_associated_project;
  }

  @Override
  public String toString() {
    return "Subject{"
        + "id='"
        + id
        + '\''
        + ", identifier="
        + identifier
        + ", species='"
        + species
        + '\''
        + ", sex='"
        + sex
        + '\''
        + ", race='"
        + race
        + '\''
        + ", ethnicity='"
        + ethnicity
        + '\''
        + ", days_to_birth='"
        + days_to_birth
        + '\''
        + ", subject_associated_project="
        + subject_associated_project
        + '}';
  }
}
