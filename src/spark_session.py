from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Crea y devuelve la sesión Spark local del taller."""
    return (
        SparkSession.builder.master("local[*]")
        .appName("taller-data-engineer")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
