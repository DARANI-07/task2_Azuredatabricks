from pyspark.sql.window import Window
from pyspark.sql.functions import avg, sum

window_spec = Window.partitionBy("group_column").orderBy("date_column").rowsBetween(-2, 0)
data = data.withColumn("rolling_avg", avg("value_column").over(window_spec))

from pyspark.ml.feature import StringIndexer, OneHotEncoder
indexer = StringIndexer(inputCol="category", outputCol="category_index")
data = indexer.fit(data).transform(data)

encoder = OneHotEncoder(inputCol="category_index", outputCol="category_onehot")
data = encoder.fit(data).transform(data)
