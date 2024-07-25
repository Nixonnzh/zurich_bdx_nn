import time
from datetime import date
import snowflake.snowpark.functions as F
from zurich_bdx_accelerator.utils.snowpark_connector import snowpark_session_create

start_time = time.time()

demo_session = snowpark_session_create()

# inspect_internal_stage()
# df_name_check()
# df_to_raw()
# df_validations(scheme, df)


# we run the query101
df = demo_session.sql("select * from  SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER")

# note the the group_by has a different spelling
df2 = df.group_by("C_MKTSEGMENT").agg(F.sum("C_ACCTBAL").alias("SUBTOTAL"))

# get total of accountbalance
acct_bal_total = df2.agg(F.sum("SUBTOTAL").alias("SUBTOTAL"))

# converting to a pandas df, so as to extract a variable
pdf = acct_bal_total.toPandas()
total_sum = pdf.iloc[0, 0]

df2 = df2.with_column("percentage", (df2["SUBTOTAL"] * 100) / total_sum)
df2 = df2.sort(F.col("percentage").desc())
df2.show()

print("-----------")

print("--- %s seconds ---" % (time.time() - start_time))
