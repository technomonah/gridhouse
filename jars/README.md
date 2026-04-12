# Spark JARs

Pre-downloaded JARs for the `grindhouse-spark` container (Spark 3.4.x, Scala 2.12).
JARs are not tracked in git — download them before running `make up`.

## Download

```bash
make download-jars
```

Or manually:

```bash
cd jars/

# Iceberg runtime + Spark integration
curl -LO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.5.2/iceberg-spark-runtime-3.4_2.12-1.5.2.jar

# Iceberg AWS bundle — includes AWS SDK v2 required by Iceberg S3FileIO
# Note: use iceberg-aws-bundle, NOT aws-java-sdk-bundle (SDK v1 incompatible)
curl -LO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar

# Nessie Spark extensions — last version with Spark 3.4 support
# Note: 0.107.x only supports Spark 3.5
curl -LO https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.4_2.12/0.106.0/nessie-spark-extensions-3.4_2.12-0.106.0.jar

# Hadoop S3A connector
curl -LO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
```

## Versions

| JAR | Version | Notes |
|-----|---------|-------|
| iceberg-spark-runtime-3.4_2.12 | 1.5.2 | Iceberg + Spark 3.4 integration |
| iceberg-aws-bundle | 1.5.2 | AWS SDK v2 — required for Iceberg S3FileIO |
| nessie-spark-extensions-3.4_2.12 | 0.106.0 | Last version with Spark 3.4 support |
| hadoop-aws | 3.3.4 | S3A connector for Hadoop filesystem API |
