package bio.terra.cda.app.helpers;

import bio.terra.cda.app.util.TableSchema;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;

public class TableSchemaHelper {
    private TableSchemaHelper() {}

    public static TableSchema getNewTableSchema(String version) {
        return new TableSchema(StorageServiceHelper.newStorageService(version), new ConcurrentMapCacheManager());
    }
}
