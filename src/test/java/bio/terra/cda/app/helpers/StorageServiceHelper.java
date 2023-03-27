package bio.terra.cda.app.helpers;

import bio.terra.cda.app.service.StorageService;
import com.google.cloud.storage.StorageOptions;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class StorageServiceHelper {
    private StorageServiceHelper() {}

    public static StorageService newStorageService(String versionMock) {
        StorageService storageService = Mockito.mock(StorageService.class);

        when(storageService.getSchemaMap(versionMock))
                .thenAnswer(a -> getTableSchemaMap(versionMock));
        return storageService;
    }

    private static File[] getResourceFolderFiles (String folder) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL url = loader.getResource(folder);
        assert url != null;
        String path = url.getPath();
        return new File(path).listFiles();
    }

    public static Map<String, String> getTableSchemaMap(String version) {
        Map<String, String> tableSchemaMap = new HashMap<>();

        for (File f : getResourceFolderFiles(String.format("%s/%s", "schema", version))) {
            try (InputStream fileInputStream = new FileInputStream(f)) {
                tableSchemaMap.put(
                        f.getName().replace(".json", ""),
                        new String(fileInputStream.readAllBytes()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return tableSchemaMap;
    }
}
