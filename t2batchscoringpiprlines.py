from pyspark.ml.regression import LinearRegressionModel

# Load the model
loaded_model = LinearRegressionModel.load("wasbs://mycontainer@mystorageaccount.blob.core.windows.net/models/linear_model")

# Predict on new data
new_data = spark.read.csv("wasbs://mycontainer@mystorageaccount.blob.core.windows.net/new_data.csv", header=True, inferSchema=True)
new_data = assembler.transform(new_data)
predictions = loaded_model.transform(new_data)

# Save predictions to Blob Storage
predictions.write.csv("wasbs://mycontainer@mystorageaccount.blob.core.windows.net/predictions.csv")
