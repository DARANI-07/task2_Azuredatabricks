from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
data = assembler.transform(data)

train_data, test_data = data.randomSplit([0.8, 0.2])

lr = LinearRegression(featuresCol="features", labelCol="Age")
model = lr.fit(train_data)
