package bio.terra.cda.app.service;

import bio.terra.cda.app.generators.EntitySqlGenerator;

public class ChildFilter extends Filter{
    /***
     * Class to construct optimized count preselect SQL statement from the filters
     * in the original count(*) wrapped query
     *
     * @throws RuntimeException If there is problem create the filters
     * @param baseFilterString Originally passed in as generated sql but later
     * @param generator
     * @param id
     */
    public ChildFilter(String baseFilterString, EntitySqlGenerator generator, String id) {
        super(baseFilterString, generator, id);
    }
}
