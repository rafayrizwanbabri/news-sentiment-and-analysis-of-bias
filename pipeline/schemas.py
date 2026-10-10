"""Explicit schemas and canonical column lists for the Lakehouse layers."""

from pyspark.sql.types import StringType, StructField, StructType


# Raw CSV values intentionally enter Bronze as strings.  Parsing them at this
# boundary would hide source-system type changes; Silver performs typed casts.
RAW_NEWS_SCHEMA = StructType([
    StructField("Source", StringType(), True),
    StructField("Link", StringType(), True),
    StructField("Headline", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Topic", StringType(), True),
    StructField("Author", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Article_Content", StringType(), True),
    StructField("Sentiment_Bias", StringType(), True),
    StructField("Keywords", StringType(), True),
])

RAW_COLUMNS = [field.name for field in RAW_NEWS_SCHEMA.fields]
