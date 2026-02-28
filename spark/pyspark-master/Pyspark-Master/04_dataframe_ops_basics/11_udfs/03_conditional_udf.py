# ------------------------------------------------------------------------------
# Import required modules from PySpark
# ------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# ------------------------------------------------------------------------------
# Step 1: Initialize SparkSession (Entry point for DataFrame operations)
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("UDF").getOrCreate()

# ------------------------------------------------------------------------------
# Step 2: Define a regular Python function to classify a number as even or odd
# ------------------------------------------------------------------------------
def odd_even(x):
    return 'even' if x % 2 == 0 else 'odd'

# ------------------------------------------------------------------------------
# Step 3: Convert the Python function to a PySpark UDF
# We must specify the return type (StringType here)
# ------------------------------------------------------------------------------
odd_even_udf = udf(odd_even, StringType())

# ------------------------------------------------------------------------------
# Step 4: Create a sample DataFrame with a single column "number"
# ------------------------------------------------------------------------------
df = spark.createDataFrame([(1,), (2,), (3,)], ['number'])

# ------------------------------------------------------------------------------
# Step 5: Apply the UDF to create a new column "odd_or_even"
# ------------------------------------------------------------------------------
df.withColumn('odd_or_even', odd_even_udf('number')).show()

# Output:
# +------+-----------+
# |number|odd_or_even|
# +------+-----------+
# |     1|        odd|
# |     2|       even|
# |     3|        odd|
# +------+-----------+

# ------------------------------------------------------------------------------
# ALTERNATIVE APPROACH: Use Python decorator to register the function as a UDF
# Cleaner syntax, especially when defining and registering at once
# ------------------------------------------------------------------------------
@udf(StringType())  # UDF decorator with StringType return
def odd_even(x):
    return 'even' if x % 2 == 0 else 'odd'

# ------------------------------------------------------------------------------
# Reuse the same DataFrame and apply decorated UDF
# ------------------------------------------------------------------------------
df.withColumn('odd_or_even', odd_even('number')).show()

# ------------------------------------------------------------------------------
# Concepts Highlighted:
# - UDF: Enables custom logic not available in Spark's built-in functions
# - StringType(): Defines the return type of the UDF
# - withColumn(): Adds or replaces a column in a DataFrame
# - @udf: Python decorator to simplify UDF creation
# ------------------------------------------------------------------------------
