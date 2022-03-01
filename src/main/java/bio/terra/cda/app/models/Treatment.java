package bio.terra.cda.app.models;

import java.util.Objects;

public class Treatment {
  private String id;
  private Identifier identifier;
  private String treatment_type;
  private String treatment_outcome;
  private String days_to_treatment_start;
  private String days_to_treatment_end;
  private String therapeutic_agent;
  private String treatment_anatomic_site;
  private String treatment_effect;
  private String treatment_end_reason;
  private String number_of_cycles;

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

  public String getTreatment_type() {
    return treatment_type;
  }

  public void setTreatment_type(String treatment_type) {
    this.treatment_type = treatment_type;
  }

  public String getTreatment_outcome() {
    return treatment_outcome;
  }

  public void setTreatment_outcome(String treatment_outcome) {
    this.treatment_outcome = treatment_outcome;
  }

  public String getDays_to_treatment_start() {
    return days_to_treatment_start;
  }

  public void setDays_to_treatment_start(String days_to_treatment_start) {
    this.days_to_treatment_start = days_to_treatment_start;
  }

  public String getDays_to_treatment_end() {
    return days_to_treatment_end;
  }

  public void setDays_to_treatment_end(String days_to_treatment_end) {
    this.days_to_treatment_end = days_to_treatment_end;
  }

  public String getTherapeutic_agent() {
    return therapeutic_agent;
  }

  public void setTherapeutic_agent(String therapeutic_agent) {
    this.therapeutic_agent = therapeutic_agent;
  }

  public String getTreatment_anatomic_site() {
    return treatment_anatomic_site;
  }

  public void setTreatment_anatomic_site(String treatment_anatomic_site) {
    this.treatment_anatomic_site = treatment_anatomic_site;
  }

  public String getTreatment_effect() {
    return treatment_effect;
  }

  public void setTreatment_effect(String treatment_effect) {
    this.treatment_effect = treatment_effect;
  }

  public String getTreatment_end_reason() {
    return treatment_end_reason;
  }

  public void setTreatment_end_reason(String treatment_end_reason) {
    this.treatment_end_reason = treatment_end_reason;
  }

  public String getNumber_of_cycles() {
    return number_of_cycles;
  }

  public void setNumber_of_cycles(String number_of_cycles) {
    this.number_of_cycles = number_of_cycles;
  }

  @Override
  public String toString() {
    return "Treatment{"
        + "id='"
        + id
        + '\''
        + ", identifier="
        + identifier
        + ", treatment_type='"
        + treatment_type
        + '\''
        + ", treatment_outcome='"
        + treatment_outcome
        + '\''
        + ", days_to_treatment_start='"
        + days_to_treatment_start
        + '\''
        + ", days_to_treatment_end='"
        + days_to_treatment_end
        + '\''
        + ", therapeutic_agent='"
        + therapeutic_agent
        + '\''
        + ", treatment_anatomic_site='"
        + treatment_anatomic_site
        + '\''
        + ", treatment_effect='"
        + treatment_effect
        + '\''
        + ", treatment_end_reason='"
        + treatment_end_reason
        + '\''
        + ", number_of_cycles='"
        + number_of_cycles
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Treatment)) return false;
    Treatment treatment = (Treatment) o;
    return Objects.equals(id, treatment.id)
        && Objects.equals(identifier, treatment.identifier)
        && Objects.equals(treatment_type, treatment.treatment_type)
        && Objects.equals(treatment_outcome, treatment.treatment_outcome)
        && Objects.equals(days_to_treatment_start, treatment.days_to_treatment_start)
        && Objects.equals(days_to_treatment_end, treatment.days_to_treatment_end)
        && Objects.equals(therapeutic_agent, treatment.therapeutic_agent)
        && Objects.equals(treatment_anatomic_site, treatment.treatment_anatomic_site)
        && Objects.equals(treatment_effect, treatment.treatment_effect)
        && Objects.equals(treatment_end_reason, treatment.treatment_end_reason)
        && Objects.equals(number_of_cycles, treatment.number_of_cycles);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        identifier,
        treatment_type,
        treatment_outcome,
        days_to_treatment_start,
        days_to_treatment_end,
        therapeutic_agent,
        treatment_anatomic_site,
        treatment_effect,
        treatment_end_reason,
        number_of_cycles);
  }
}
