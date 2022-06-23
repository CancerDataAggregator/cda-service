package bio.terra.cda.app.models;

public class Partition {
  private final String path;
  private final String stringRepresentation;

  public Partition(String path, String stringRepresentation) {
    this.path = path;
    this.stringRepresentation = stringRepresentation;
  }

  public String getPath() {
    return this.path;
  }

  @Override
  public String toString() {
    return stringRepresentation;
  }
}
