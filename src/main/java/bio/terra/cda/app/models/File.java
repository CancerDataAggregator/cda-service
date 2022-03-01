package bio.terra.cda.app.models;

import java.util.List;

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
}
