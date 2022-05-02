import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--project_id", required=True, type=str)
parser.add_argument("--bq_table_input", required=True, type=str)
parser.add_argument("--bucket", required=True, type=str)
args = parser.parse_args()

BQ_TABLE = f"{args.project_id}.crime_data.{args.bq_table_input}"
# BUCKET_URI = f"gs://{args.bucket}"

def main():
    spark = (SparkSession
        .builder
        .appName('transform_crime_data')
        .getOrCreate()
    )
    
    table = f"{args.project_id}.crime_data.{args.bq_table_input}"
    df = spark.read \
    .format("bigquery") \
    .option("table", table) \
    .load()
    
    summary_df = df.groupby([F.col("cmplnt_fr_dt"), F.col("addr_pct_cd"), F.col("ofns_desc")]).count()
    summary_df = summary_df.select((F.col("cmplnt_fr_dt")).alias("crime_date"), 
                                F.col("addr_pct_cd").alias("precinct"), 
                                F.col("ofns_desc").alias("category"),
                                F.col("count").alias("crime_count")
    )

    summary_df.write.format('bigquery') \
        .option('table', f"{args.project_id}.crime_data.nyc_crimes_aggr") \
        .option('temporaryGcsBucket', args.bucket) \
        .mode('overwrite') \
        .save()

    
    
if __name__ == "__main__":
    main()
  
  
