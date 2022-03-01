package bio.terra.cda.app.models;

import java.util.List;

public class Specimen {
  private String id;
  private List<Identifier> identifier;
  private String associated_project;
  private String age_at_collection;
  private String primary_disease_type;
  private String anatomical_site;
  private String source_material_type;
  private String specimen_type;
  private String derived_from_specimen;
  private String derived_from_subject;

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

  public String getAssociated_project() {
    return associated_project;
  }

  public void setAssociated_project(String associated_project) {
    this.associated_project = associated_project;
  }

  public String getAge_at_collection() {
    return age_at_collection;
  }

  public void setAge_at_collection(String age_at_collection) {
    this.age_at_collection = age_at_collection;
  }

  public String getPrimary_disease_type() {
    return primary_disease_type;
  }

  public void setPrimary_disease_type(String primary_disease_type) {
    this.primary_disease_type = primary_disease_type;
  }

  public String getAnatomical_site() {
    return anatomical_site;
  }

  public void setAnatomical_site(String anatomical_site) {
    this.anatomical_site = anatomical_site;
  }

  public String getSource_material_type() {
    return source_material_type;
  }

  public void setSource_material_type(String source_material_type) {
    this.source_material_type = source_material_type;
  }

  public String getSpecimen_type() {
    return specimen_type;
  }

  public void setSpecimen_type(String specimen_type) {
    this.specimen_type = specimen_type;
  }

  public String getDerived_from_specimen() {
    return derived_from_specimen;
  }

  public void setDerived_from_specimen(String derived_from_specimen) {
    this.derived_from_specimen = derived_from_specimen;
  }

  public String getDerived_from_subject() {
    return derived_from_subject;
  }

  public void setDerived_from_subject(String derived_from_subject) {
    this.derived_from_subject = derived_from_subject;
  }

  @Override
  public String toString() {
    return "Specimen{"
        + "id='"
        + id
        + '\''
        + ", identifier="
        + identifier
        + ", associated_project='"
        + associated_project
        + '\''
        + ", age_at_collection='"
        + age_at_collection
        + '\''
        + ", primary_disease_type='"
        + primary_disease_type
        + '\''
        + ", anatomical_site='"
        + anatomical_site
        + '\''
        + ", source_material_type='"
        + source_material_type
        + '\''
        + ", specimen_type='"
        + specimen_type
        + '\''
        + ", derived_from_specimen='"
        + derived_from_specimen
        + '\''
        + ", derived_from_subject='"
        + derived_from_subject
        + '\''
        + '}';
  }
}
