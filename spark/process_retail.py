import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

RAW_PATH = "/opt/spark-data/raw/retail_sales.csv"
OUTPUT_PATH = "/opt/spark-data/processed/retail_sales"
SUMMARY_PATH = "/opt/spark-data/processed/sales_by_category"

DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
DB_NAME = os.environ.get("POSTGRES_DB", "airflow")
DB_USER = os.environ.get("POSTGRES_USER", "airflow")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

if not DB_PASSWORD:
    raise ValueError(
        "POSTGRES_PASSWORD is not set. "
        "Configure it in the project .env file and pass it to Spark."
    )

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def require(condition, message):
    if not condition:
        raise ValueError(f"DATA QUALITY CHECK FAILED: {message}")

    print(f"PASS: {message}")


def load_to_postgres(df, spark):
    staging_table = "public.retail_sales_staging"

    # Write validated transactions to the PostgreSQL staging table.
    (
        df.select(
            "transaction_id",
            "transaction_date",
            "product",
            "category",
            "quantity",
            "unit_price",
            "store",
            "total_sales",
        )
        .write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", staging_table)
        .options(**DB_PROPERTIES)
        .mode("overwrite")
        .save()
    )

    print("Validated transactions written to PostgreSQL staging.")

    # Upsert staging rows into the permanent table.
    connection = spark._jvm.java.sql.DriverManager.getConnection(
        JDBC_URL,
        DB_PROPERTIES["user"],
        DB_PROPERTIES["password"],
    )

    connection.setAutoCommit(False)

    try:
        statement = connection.createStatement()

        try:
            statement.executeUpdate(
                """
                INSERT INTO public.retail_sales (
                    transaction_id,
                    transaction_date,
                    product,
                    category,
                    quantity,
                    unit_price,
                    store,
                    total_sales
                )
                SELECT
                    transaction_id,
                    transaction_date,
                    product,
                    category,
                    quantity,
                    unit_price,
                    store,
                    total_sales
                FROM public.retail_sales_staging
                ON CONFLICT (transaction_id)
                DO UPDATE SET
                    transaction_date = EXCLUDED.transaction_date,
                    product = EXCLUDED.product,
                    category = EXCLUDED.category,
                    quantity = EXCLUDED.quantity,
                    unit_price = EXCLUDED.unit_price,
                    store = EXCLUDED.store,
                    total_sales = EXCLUDED.total_sales,
                    loaded_at = NOW()
                """
            )

            connection.commit()
            print("PostgreSQL retail_sales table updated successfully.")

        finally:
            statement.close()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def main():
    spark = (
        SparkSession.builder
        .appName("RetailSalesPipeline")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Read raw retail data.
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(RAW_PATH)
        )

        initial_count = df.count()

        require(initial_count > 0, "Raw input is not empty")
        print(f"Initial row count: {initial_count}")

        # Remove duplicate transaction IDs.
        df = df.dropDuplicates(["transaction_id"])

        # Convert source fields to the correct types.
        df = (
            df.withColumn(
                "transaction_date",
                F.to_date("transaction_date", "yyyy-MM-dd"),
            )
            .withColumn(
                "quantity",
                F.col("quantity").cast(IntegerType()),
            )
            .withColumn(
                "unit_price",
                F.col("unit_price").cast(DoubleType()),
            )
        )

        # Remove rows missing required information.
        df = df.dropna(
            subset=[
                "transaction_id",
                "transaction_date",
                "product",
                "category",
                "quantity",
                "unit_price",
            ]
        )

        # Remove invalid quantities and prices.
        df = df.filter(
            (F.col("quantity") > 0)
            & (F.col("unit_price") >= 0)
        )

        # Replace missing store names with Unknown.
        df = df.withColumn(
            "store",
            F.when(
                F.col("store").isNull()
                | (F.trim(F.col("store")) == ""),
                F.lit("Unknown"),
            ).otherwise(F.trim(F.col("store"))),
        )

        # Calculate sales for each transaction.
        df = df.withColumn(
            "total_sales",
            F.round(
                F.col("quantity") * F.col("unit_price"),
                2,
            ),
        )

        # Validate the cleaned transactions.
        cleaned_count = df.count()

        require(
            cleaned_count > 0,
            "Cleaned dataset is not empty",
        )

        duplicate_count = (
            df.groupBy("transaction_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        require(
            duplicate_count == 0,
            "Transaction IDs are unique",
        )

        missing_required = df.filter(
            F.col("transaction_id").isNull()
            | (F.trim(F.col("transaction_id")) == "")
            | F.col("transaction_date").isNull()
            | F.col("product").isNull()
            | (F.trim(F.col("product")) == "")
            | F.col("category").isNull()
            | (F.trim(F.col("category")) == "")
            | F.col("quantity").isNull()
            | F.col("unit_price").isNull()
        ).count()

        require(
            missing_required == 0,
            "Required fields are populated",
        )

        invalid_values = df.filter(
            (F.col("quantity") <= 0)
            | (F.col("unit_price") < 0)
        ).count()

        require(
            invalid_values == 0,
            "Quantities and prices are valid",
        )

        incorrect_sales = df.filter(
            F.abs(
                F.col("total_sales")
                - F.round(
                    F.col("quantity") * F.col("unit_price"),
                    2,
                )
            ) > 0.001
        ).count()

        require(
            incorrect_sales == 0,
            "Sales calculations are correct",
        )

        # Summarize sales by category.
        summary = (
            df.groupBy("category")
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("quantity").alias("units_sold"),
                F.round(
                    F.sum("total_sales"),
                    2,
                ).alias("total_revenue"),
            )
            .orderBy(F.desc("total_revenue"))
        )

        # Reconcile category revenue with transaction revenue.
        transaction_revenue = df.agg(
            F.round(
                F.sum("total_sales"),
                2,
            ).alias("revenue")
        ).first()["revenue"]

        category_revenue = summary.agg(
            F.round(
                F.sum("total_revenue"),
                2,
            ).alias("revenue")
        ).first()["revenue"]

        require(
            abs(
                float(transaction_revenue or 0)
                - float(category_revenue or 0)
            ) < 0.01,
            "Category revenue reconciles with transaction revenue",
        )

        require(
            summary.count() > 0,
            "Category summary is not empty",
        )

        # Load validated transactions into PostgreSQL.
        load_to_postgres(df, spark)

        # Write processed datasets for the dashboard.
        (
            df.orderBy("transaction_date", "transaction_id")
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(OUTPUT_PATH)
        )

        (
            summary.write
            .mode("overwrite")
            .option("header", True)
            .csv(SUMMARY_PATH)
        )

        print(f"Cleaned row count: {cleaned_count}")
        print(f"Total revenue: {transaction_revenue}")

        print("Cleaned transactions:")
        df.show(truncate=False)

        print("Sales by category:")
        summary.show(truncate=False)

        print("All Spark data-quality checks passed.")
        print(f"Processed data saved to: {OUTPUT_PATH}")
        print(f"Category summary saved to: {SUMMARY_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()