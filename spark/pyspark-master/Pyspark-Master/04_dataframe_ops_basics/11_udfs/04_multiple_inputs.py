# ------------------------------------------------------------------------------
# Import required modules
# ------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# ------------------------------------------------------------------------------
# Step 1: Create a SparkSession - required to work with DataFrames
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("UDF").getOrCreate()

# ------------------------------------------------------------------------------
# Step 2: Define a UDF using a lambda function to multiply two numbers
# Also register the UDF using udf() and specify the return type (IntegerType)
# ------------------------------------------------------------------------------
multiply_udf = udf(lambda x, y: x * y, IntegerType())

# ------------------------------------------------------------------------------
# Step 3: Create a DataFrame with two numeric columns 'a' and 'b'
# ------------------------------------------------------------------------------
df = spark.createDataFrame([(1, 2), (3, 4)], ['a', 'b'])

# ------------------------------------------------------------------------------
# Step 4: Apply the multiply UDF to compute product of 'a' and 'b'
# This creates a new column called 'product'
# ------------------------------------------------------------------------------
df.withColumn('product', multiply_udf('a', 'b')).show()

# Output:
# +---+---+-------+
# |  a|  b|product|
# +---+---+-------+
# |  1|  2|      2|
# |  3|  4|     12|
# +---+---+-------+

# ------------------------------------------------------------------------------
# ALTERNATIVE METHOD: Using @udf decorator to define a multiply UDF
# This method is more readable and commonly used in modern PySpark code
# ------------------------------------------------------------------------------
@udf(IntegerType())  # UDF returns an integer (the product)
def multiply(x, y):
    return x * y

# ------------------------------------------------------------------------------
# Step 5: Apply the decorated UDF to create the 'product' column
# ------------------------------------------------------------------------------
df.withColumn('product', multiply('a', 'b')).show()

# ------------------------------------------------------------------------------
# Concepts Covered:
# - UDF with multiple arguments (x and y)
# - Using lambda vs @udf decorator
# - IntegerType() for numeric return type
# - withColumn to add a derived column to the DataFrame
# ------------------------------------------------------------------------------
