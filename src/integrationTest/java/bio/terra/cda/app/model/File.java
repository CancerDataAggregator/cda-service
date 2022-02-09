package bio.terra.cda.app.model;

public class File {
    private String id;
    private Identifier identifier;
    private String label;
    private String data_category;
    private String data_type;
    private String associated_project;
    private String drs_uri;
    private long byte_size;
    private String data_modality;
    private String imaging_modality;
    private String dbgap_accession_number;

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

    public long getByte_size() {
        return byte_size;
    }

    public void setByte_size(long byte_size) {
        this.byte_size = byte_size;
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

    @Override
    public String toString() {
        return "File{" +
                "id='" + id + '\'' +
                ", identifier=" + identifier +
                ", label='" + label + '\'' +
                ", data_category='" + data_category + '\'' +
                ", data_type='" + data_type + '\'' +
                ", associated_project='" + associated_project + '\'' +
                ", drs_uri='" + drs_uri + '\'' +
                ", byte_size=" + byte_size +
                ", data_modality='" + data_modality + '\'' +
                ", imaging_modality='" + imaging_modality + '\'' +
                ", dbgap_accession_number='" + dbgap_accession_number + '\'' +
                '}';
    }
}
