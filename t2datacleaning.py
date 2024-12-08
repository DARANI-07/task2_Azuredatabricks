data = data.fillna({"Location": 0})

from pyspark.sql.functions import col, when
data = data.withColumn("Location", when(col("Location") > 100, 100).otherwise(col("Location")))
