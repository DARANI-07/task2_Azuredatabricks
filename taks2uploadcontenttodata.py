spark.conf.set(
    "fs.azure.account.key.<your-storage-account-name>.blob.core.windows.net",
    "Z0htG59KlVyejt5e95k+1bGRc9jC+5Mejds+2LKzWrr+PfxZErY1w=="
)

data = spark.read.csv("wasbs://mycontainer@mystorageaccount.blob.core.windows.net/data.csv", header=True, inferSchema=True)
data.show()
