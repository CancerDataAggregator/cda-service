package bio.terra.cda.app.service;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import org.springframework.cache.annotation.Cacheable;

public class StorageService {
    private final Storage storage;
    private final BucketOptions bucketOptions;

    private StorageService(
            Storage storage,
            BucketOptions bucketOptions) {
        this.storage = storage;
        this.bucketOptions = bucketOptions;
    }

    public Blob getBlob(String blobName) {
        return this.storage.get(bucketOptions.getBucketName(), blobName);
    }

    public String getAsString(String blobName) {
        Blob blob = this.getBlob(blobName);
        return new String(blob.getContent());
    }

    public String getSchemaContent(String schemaVersion) {
        return this.getAsString(
                String.format("%s/%s.json",
                        bucketOptions.getSchemaDirectory(),
                        schemaVersion));
    }

    public static StorageServiceBuilder newBuilder() {
        return new StorageServiceBuilder();
    }

    public static class StorageServiceBuilder {
        private Storage storage;
        private BucketOptions bucketOptions;

        private StorageServiceBuilder() {}

        public StorageServiceBuilder setStorage(Storage storage) {
            this.storage = storage;
            return this;
        }

        public StorageServiceBuilder setBucketOptions(BucketOptions bucketOptions) {
            this.bucketOptions = bucketOptions;
            return this;
        }

        public StorageService build() {
            return new StorageService(this.storage, this.bucketOptions);
        }
    }

    public static class BucketOptions {
        private final String bucketName;
        private final String schemaDirectory;

        private BucketOptions(
                String bucketName,
                String schemaDirectory
        ) {
            this.bucketName = bucketName;
            this.schemaDirectory = schemaDirectory;
        }

        public String getBucketName() {
            return this.bucketName;
        }

        public String getSchemaDirectory() {
            return this.schemaDirectory;
        }

        public static BucketOptionsBuilder newBuilder() {
            return new BucketOptionsBuilder();
        }

        public static class BucketOptionsBuilder {
            private String bucketName;
            private String schemaDirectory;

            public BucketOptionsBuilder() {}

            public BucketOptionsBuilder setBucketName(String bucketName) {
                this.bucketName = bucketName;
                return this;
            }

            public BucketOptionsBuilder setSchemaDirectory(String schemaDirectory) {
                this.schemaDirectory = schemaDirectory;
                return this;
            }

            public BucketOptions build() {
                return new BucketOptions(bucketName, schemaDirectory);
            }
        }
    }
}
