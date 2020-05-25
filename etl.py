import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, split, explode, count, size, \
    reverse, input_file_name, struct, lit, array, monotonically_increasing_id
from pyspark.sql.types import DoubleType, IntegerType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

"""Inits spark session."""
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def melt_df(df, fixed_columns, col_name):
    cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in fixed_columns))
    kvs = explode(array([struct(lit(c).alias(col_name), col(c).alias("value")) for c in cols])).alias("kvs")
    df = df.select(fixed_columns + [kvs]).select(fixed_columns + ["kvs.{}".format(col_name), "kvs.value"])
    return df


def process_country_data(spark, input_data, output_data):
    """ filepath to country data file """
    country_data = input_data + 'all.json'

    """ read country data """
    df = spark.read.json(country_data)
    df.persist()

    """ extract columns to create country table """
    country_table = df.select(col('alpha-3').alias('code'), col('name'), col('region'), \
        col('sub-region').alias('sub_region')).dropDuplicates()

    """ write country table to parquet files """
    country_table.write.parquet(output_data + 'dim_country.parquet', mode = 'overwrite')


def process_indicator_data(spark, input_data, output_data):
    """ filepath to wdi data file """
    wdi_data = input_data + 'WDIData.csv'
    """ read wdi data file """
    df_wdi = spark.read.csv(wdi_data, header='true')
    df_wdi.persist()
    """ clean data - remove null columns """
    count_not_null = df_wdi.agg(*[count(c).alias(c) for c in df_wdi.columns])
    not_null_cols = [c for c in count_not_null.columns if count_not_null[[c]].first()[c] > 0]
    df_wdi = df_wdi.select(*not_null_cols)
    """ melt data """
    fixed_columns = ['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code']
    df_wdi = melt_df(df_wdi, fixed_columns, 'year')

    """ filepath to world happiness data file """
    happiness_data = input_data + 'world-happiness/'
    """ read happiness data file """
    df_happ = spark.read.csv(happiness_data, header='true').withColumn("file", input_file_name())
    df_happ.persist()
    """ clean and transform data """
    df_happ = df_happ.withColumn("year", reverse(split(reverse(df_happ.file), '/')[0])[0:4])
    df_happ = df_happ.drop('file')
    df_happ = df_happ.toDF(*(c.replace(' ', '') for c in df_happ.columns))
    """ melt data """
    fixed_columns = ['Countryorregion', 'year']
    df_happ = melt_df(df_happ, fixed_columns, 'indicator')

    """ filepath to unece data file """
    unece_data = input_data + 'unece.json'
    """ read unece data file """
    df_unece = spark.read.json(unece_data)
    df_unece.persist()
    """ clean data """
    df_unece = df_unece.toDF(*(c.replace(',', '') for c in df_unece.columns))
    df_unece = df_unece.toDF(*(c.replace('.', '') for c in df_unece.columns))
    """ melt data """
    fixed_columns = ['Country', 'Year']
    df_unece = melt_df(df_unece, fixed_columns, 'indicator')

    #### -------------- DIM TIME ----------------- ####
    """ transform and extract columns to create time table """
    df_time = df_wdi.select('year').dropDuplicates()
    df_time = df_time.withColumn("year", df_time["year"].cast(IntegerType()))

    get_century = udf(lambda x : (x - 1) // 100 + 1)
    df_time = df_time.withColumn("century", get_century(df_time.year))

    get_decade = udf(lambda x : int(str(x)[2]) * 10)
    df_time = df_time.withColumn("decade", get_decade(df_time.year))

    time_table = df_time["year", "decade", "century"]

    """ write time table to parquet files """
    time_table.write.parquet(output_data + 'dim_time.parquet', mode = 'overwrite')
    #### -------------- DIM TIME ----------------- ####

    #### ------------ DIM INDICATOR -------------- ####
    """ extract wdi indicators for indicator table """
    indicator_table_wdi = df_wdi.select(col('Indicator Code').alias('code'), \
        col('Indicator Name').alias('name')).dropDuplicates()

    """ transform and extract column group from column code """
    indicator_table_wdi = indicator_table_wdi.withColumn('group', split(indicator_table_wdi.code, '\.').getItem(0))

    """ extract happiness indicators for indicator table """
    indicator_table_happ = df_happ.select(col('indicator').alias('code'), \
        col('indicator').alias('name')).dropDuplicates()
    indicator_table_happ = indicator_table_happ.withColumn('group', lit('people_happines'))

    """ extract unece indicators for indicator table """
    indicator_table_unece = df_unece.select(col('indicator').alias('code'), \
        col('indicator').alias('name')).dropDuplicates()
    indicator_table_unece = indicator_table_unece.withColumn('group', lit('unece_indicator'))

    """ write indicators table to parquet files """
    indicator_table = indicator_table_wdi.union(indicator_table_happ)
    indicator_table.write.parquet(output_data + 'dim_indicator.parquet', mode = 'overwrite')
    #### ------------ DIM INDICATOR -------------- ####

    #### -------------- FACT SCORE --------------- ####

    """ create score table from indicator files """
    df_score = df_wdi.select(col('year').alias('dim_time_year'), col('Country Code'), \
       col('Country Name'), col('Indicator Code').alias('dim_indicator_code'), col("value").cast(DoubleType()))

    """ read country parquet files to get country code """
    country_df = spark.read.parquet(output_data + 'dim_country.parquet')
    df_score = df_score.join(country_df.select("code", "name"), country_df['code'] == df_score['Country Code'])

    """ unece score """
    df_unece = df_unece.join(df_score.select("code", "name"), df_unece.Country == df_score.name)
    df_unece_score = df_unece.select(col('Year').alias('dim_time_year'), col('code').alias('dim_country_code'), \
       col('indicator').alias('dim_indicator_code'), col("value").cast(DoubleType()))

    """ happines score """
    df_happ = df_happ.join(df_score.select("code", "name"), df_happ.Countryorregion == df_score.name)
    df_happ_score = df_happ.select(col('year').alias('dim_time_year'), col('code').alias('dim_country_code'), \
       col('indicator').alias('dim_indicator_code'), col("value").cast(DoubleType()))

    """ union of all indicators score """
    df_score = df_score.select(col('dim_time_year'), col('code').alias('dim_country_code'), \
       col('dim_indicator_code'), col("value").cast(DoubleType()))
    score_table = df_score.union(df_unece_score).union(df_happ_score)

    score_table = score_table.withColumn("score_id", monotonically_increasing_id())
    """ write score table to parquet files partitioned by year """
    score_table.write.partitionBy('dim_time_year').parquet(output_data + 'fact_score.parquet', mode = 'overwrite')
    #### -------------- FACT SCORE --------------- ####


def dw_files_head(spark, output_data):
    spark.read.parquet(output_data + 'dim_indicator.parquet').show()
    spark.read.parquet(output_data + 'dim_country.parquet').show()
    spark.read.parquet(output_data + 'dim_time.parquet').show()
    spark.read.parquet(output_data + 'fact_score.parquet').show()


def main():
    spark = create_spark_session()

    """ S3 dir for input and output data """
    input_data = "s3a://lcf-udacity-de-bucket/"
    output_data = "s3a://lcf-udacity-de-bucket/countryindicatorsdw/"

    process_country_data(spark, input_data, output_data)
    process_indicator_data(spark, input_data, output_data)

    dw_files_head(spark, output_data)


if __name__ == "__main__":
    main()
