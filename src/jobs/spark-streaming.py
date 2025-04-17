import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import struct, from_json, to_json, col, udf, when
import time
from transformers import pipeline

# Load the sentiment analysis pipeline only once
sa_pipeline = pipeline("sentiment-analysis")

def sentiment_analysis(text):
    if text:
        result = sa_pipeline(text)[0]
        return result['label']
    return None

def start_streaming(spark):
    topic = 'movie_review'
    while True:
        try:
            stream_df = ( spark.readStream.format("socket")
                                .option("host", "localhost")
                                .option("port", 9999)
                                .load()
                        )

            schema = StructType([
                StructField("review", StringType()),
                StructField("sentiment", StringType())
            ])

            stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select(("data.*"))

            sentiment_analysis_udf = udf(sentiment_analysis, StringType())

            stream_df = stream_df.withColumn(
                'feedback',
                when(col('review').isNotNull(), sentiment_analysis_udf(col('review')))
                .otherwise(None)
            )

            # 👇 Convert to single JSON string column called 'value' (needed for Kafka sink)
            kafka_df = stream_df.select(to_json(struct("review", "sentiment", "feedback")).alias("value"))

            query = (kafka_df \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker-2:9092") \
            .option("topic", topic) \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .outputMode("append") \
            .start()
            .awaitTermination()
            )

        except Exception as e:
            print(f'Exception encountered: {e}. Retrying in 10s')
            time.sleep(10)

if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()
    start_streaming(spark_conn)

            # query = stream_df.writeStream \
            #     .format("console") \
            #     .outputMode("append") \
            #     .options(truncate=False) \
            #     .start()
            # query.awaitTermination()    