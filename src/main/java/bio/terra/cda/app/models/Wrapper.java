package bio.terra.cda.app.models;

import java.util.List;

public class Wrapper {
  private List<Subject> results;

  public List<Subject> getResults() {
    return results;
  }

  public void setResults(List<Subject> results) {
    this.results = results;
  }

  @Override
  public String toString() {
    return "ResultDataOutput{" + "results=" + results + '}';
  }
}
