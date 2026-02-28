# Import necessary modules from PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# ------------------------------------------------------------------------------
# Step 1: Create SparkSession - Entry point to PySpark functionality
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName('UDF Example').getOrCreate()

# ------------------------------------------------------------------------------
# Step 2: Define a Python function to double a number
# This function will be converted to a UDF (User Defined Function)
# ------------------------------------------------------------------------------
def double_num(x):
    return x * 2  # Returns double the input number

# ------------------------------------------------------------------------------
# Step 3: Register the function as a UDF
# We need to specify the return type using PySpark's DataType (IntegerType here)
# ------------------------------------------------------------------------------
double_udf = udf(double_num, IntegerType())

# ------------------------------------------------------------------------------
# Step 4: Create a sample DataFrame with a single column "number"
# ------------------------------------------------------------------------------
df = spark.createDataFrame([(1,), (2,), (3,)], ['number'])

# ------------------------------------------------------------------------------
# Step 5: Apply the UDF to the "number" column using withColumn
# This creates a new column "doubled" with the result of double_udf
# ------------------------------------------------------------------------------
df.withColumn('doubled', double_udf('number')).show()

# Output:
# +------+-------+
# |number|doubled|
# +------+-------+
# |     1|      2|
# |     2|      4|
# |     3|      6|
# +------+-------+

# ------------------------------------------------------------------------------
# Alternative Approach: Annotations | Use decorator syntax to define UDF
# ------------------------------------------------------------------------------
@udf(IntegerType())  # This registers the function as a UDF with IntegerType output
def double_num(x):
    return x * 2

# ------------------------------------------------------------------------------
# Step 6: Re-create the same DataFrame (optional if reusing existing df)
# ------------------------------------------------------------------------------
df = spark.createDataFrame([(1,), (2,), (3,)], ['number'])

# ------------------------------------------------------------------------------
# Step 7: Apply the decorated UDF directly without needing to register separately
# ------------------------------------------------------------------------------
df.withColumn('doubled', double_num('number')).show()

# --------------Register UDF for temp views ----------------------

df.createOrReplaceTempView("nums")
spark.udf.register("double_view_num", double_num)
spark.sql("select double_view_num(number) from nums").show()
