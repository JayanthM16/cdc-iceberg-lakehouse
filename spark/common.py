from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """SparkSession wired to an Iceberg Hadoop catalog named `lake` at /warehouse.

    To move the lakehouse to S3/MinIO, change `warehouse` to s3a://bucket/path and add the
    iceberg-aws-bundle + hadoop-aws packages. Nothing else in the pipeline changes.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", "/warehouse")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
