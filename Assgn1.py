from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .appName("IMDB Movie Reviews HW 1.2") \
    .getOrCreate()


input_path = "/Users/pramathkp/Downloads/IMDB Dataset.csv"   

df = spark.read.csv(
    input_path,
    header=True,
    inferSchema=True,
    multiLine=True,   
    quote='"',
    escape='"'
)


df.printSchema()
df.show(5, truncate=False)


df_clean = df.filter(
    (F.col("review").isNotNull()) &
    (F.trim(F.col("review")) != "")
)


df_clean = df_clean.filter(F.col("sentiment").isin("positive", "negative"))

print("After cleaning, row count:", df_clean.count())


total_reviews = df_clean.count()
print("Total number of reviews:", total_reviews)

print("Counts by sentiment:")
df_clean.groupBy("sentiment").count().show()


df_transformed = df_clean.withColumn(
    "review_length",
    F.length(F.col("review"))
)

df_transformed.select("review", "review_length", "sentiment").show(5, truncate=False)

long_reviews = df_transformed.filter(F.col("review_length") > 500)
print("Reviews with length > 500 characters:")
long_reviews.select("review_length", "sentiment").show(10, truncate=False)


output_path = "imdb_reviews_cleaned_transformed"
df_transformed.write.mode("overwrite").csv(output_path, header=True)

print("Saved cleaned & transformed data to:", output_path)
