package bio.terra.cda.app.service;

import bio.terra.cda.app.models.ForeignKey;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class TablePrecedenceComparator implements Comparator<ForeignKey> {

  // TODO get from config file
  private List<String> tablePrecedenceList = Arrays.asList(
      "subject",
      "subject_researchsubject",
      "subject_identifier",
      "subject_associated_project",
      "researchsubject",
      "researchsubject_diagnosis",
      "researchsubject_identifier",
      "researchsubject_specimen",
      "researchsubject_treatment",
      "specimen",
      "specimen_identifier",
      "diagnosis",
      "diagnosis_identifier",
      "diagnosis_treatment",
      "treatment",
      "treatment_identifier",
      "file",
      "file_subject",
      "file_specimen",
      "file_identifier",
      "file_associated_project");

  @Override
  public int compare(ForeignKey o1, ForeignKey o2) {
    int comp = Integer.compare(tablePrecedenceList.indexOf(o1.getDestinationTableName()), (tablePrecedenceList.indexOf(o2.getDestinationTableName())));
    // if this returns 0 it thinks the element has already been added, so now actually check for FK equality
    if (comp == 0) {
      comp = o1.equals(o2) ? 0 : 1;
    }
    return comp;
  }
}
