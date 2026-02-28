# ------------------------------------------------------------------------------
# Import necessary modules
# ------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# ------------------------------------------------------------------------------
# Step 1: Initialize a SparkSession - Entry point to work with DataFrames
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("UDF").getOrCreate()

# ------------------------------------------------------------------------------
# Step 2: Create a UDF using a lambda function
# The lambda function takes a name and returns a greeting string
# The return type is explicitly mentioned using StringType
# ------------------------------------------------------------------------------
concat_udf = udf(lambda x: f"Hello {x}", StringType())

# ------------------------------------------------------------------------------
# Step 3: Create a sample DataFrame with a "name" column
# ------------------------------------------------------------------------------
df = spark.createDataFrame([("John",), ("Jane",)], ["name"])

# ------------------------------------------------------------------------------
# Step 4: Apply the UDF to add a new column "greeting"
# ------------------------------------------------------------------------------
df.withColumn('greeting', concat_udf('name')).show()

# Output:
# +----+--------+
# |name|greeting|
# +----+--------+
# |John|Hello John|
# |Jane|Hello Jane|
# +----+-----------+

# ------------------------------------------------------------------------------
# Alternative Method: Using @udf decorator to define UDF
# ------------------------------------------------------------------------------
@udf(StringType())  # Registers the function as a UDF with StringType return type
def hello_name(name):
    return f"Hello {name}"

# ------------------------------------------------------------------------------
# Step 5: Apply the decorated UDF directly (no need to register separately)
# ------------------------------------------------------------------------------
df.withColumn('greeting', hello_name('name')).show()

# ------------------------------------------------------------------------------
# Concepts Recap:
# - UDF (User Defined Function): Used when custom logic can't be expressed using built-in functions
# - StringType(): Return type for string-based UDFs
# - udf(): Used to register Python functions as Spark UDFs
# - @udf decorator: Cleaner alternative to explicitly registering UDFs
# ------------------------------------------------------------------------------
