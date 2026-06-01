#!/usr/bin/env python
# coding: utf-8

# ## retail_transformation_notebook
# 
# null

# ###  Reading Bronze files using pandas

# In[2]:


# pandas to read Excel files because it is the most reliable method

import pandas as pd
import warnings
warnings.filterwarnings('ignore')

BRONZE = "/lakehouse/default/Files/bronze/"

print("Reading source files...")

pdf1        = pd.read_excel(BRONZE + "retail_data1.xlsx")
pdf2        = pd.read_excel(BRONZE + "retail_data2.xlsx")
pdf_product = pd.read_excel(BRONZE + "product_details.xlsx")

print(f"retail_data1    : {len(pdf1):,} rows, {len(pdf1.columns)} cols")
print(f"retail_data2    : {len(pdf2):,} rows, {len(pdf2.columns)} cols")
print(f"product_details : {len(pdf_product):,} rows")

# Preview
display(pdf1.head(3))


# ### Convert to Spark DataFrames

# In[3]:


# Convert pandas → Spark 

df_raw1     = spark.createDataFrame(pdf1)
df_raw2     = spark.createDataFrame(pdf2)
product_dim = spark.createDataFrame(pdf_product)

print("Converted to Spark DataFrames successfully.")
print(f"df_raw1 schema:")
df_raw1.printSchema()


# ### Standardize column names

# In[4]:


# Clean columns names: lowercase, strip spaces, replace spaces with underscores

def clean_col_names(df):
    cleaned = [
        c.strip().lower()
         .replace(' ', '_')
         .replace('-', '_')
         .replace('(', '')
         .replace(')', '')
        for c in df.columns
    ]
    return df.toDF(*cleaned)

df_raw1     = clean_col_names(df_raw1)
df_raw2     = clean_col_names(df_raw2)
product_dim = clean_col_names(product_dim)

print("Column names after cleaning:")
print(df_raw1.columns)


# ### Union both transaction datasets

# In[5]:


# Stack both datasets on top of each other.

rows_1 = df_raw1.count()
rows_2 = df_raw2.count()

df_combined = df_raw1.unionByName(df_raw2)

print(f"df_raw1 rows    : {rows_1:,}")
print(f"df_raw2 rows    : {rows_2:,}")
print(f"Combined total  : {df_combined.count():,}")
print(f"Expected        : {rows_1 + rows_2:,}")


# ### Remove duplicate rows

# In[6]:


# dropDuplicates() removes rows where every column is identical.

before = df_combined.count()
df_dedup = df_combined.dropDuplicates()
after = df_dedup.count()

print(f"Rows before dedup : {before:,}")
print(f"Rows after dedup  : {after:,}")
print(f"Duplicates removed: {before - after:,}")


# ### Handle missing values

# In[7]:


from pyspark.sql.functions import col, when, lit, trim, initcap, lower

# Strategy:
# Categorical columns (city, category etc.) → fill with 'Unknown'
# Customer info columns                      → fill with 'Not Provided'
# Quantity <= 0                              → remove (invalid transaction)

# Fill categorical nulls
cat_cols = ['city', 'category', 'payment_method', 'sales_channel']
for c in cat_cols:
    if c in df_dedup.columns:
        df_dedup = df_dedup.fillna({c: 'Unknown'})
        print(f"  Filled nulls in '{c}' with 'Unknown'")

# Fill customer info nulls
pii_cols = ['customer_name', 'customer_email', 'customer_phone']
for c in pii_cols:
    if c in df_dedup.columns:
        df_dedup = df_dedup.fillna({c: 'Not Provided'})
        print(f"  Filled nulls in '{c}' with 'Not Provided'")

# Remove invalid quantities
QUANTITY_COL = 'quantity'   
if QUANTITY_COL in df_dedup.columns:
    invalid = df_dedup.filter(col(QUANTITY_COL) <= 0).count()
    df_dedup = df_dedup.filter(col(QUANTITY_COL) > 0)
    print(f"\nRemoved {invalid} rows with quantity <= 0")

print(f"\nRows remaining: {df_dedup.count():,}")


# ### Standardize text and parse dates

# In[8]:


from pyspark.sql.functions import *
from pyspark.sql.types import StringType

# STANDARDIZE TEXT COLUMNS

df_clean = df_dedup

str_cols = [
    field.name
    for field in df_clean.schema.fields
    if isinstance(field.dataType, StringType)]

for c in str_cols:
    if c not in ["email", "transaction_date"]:
        df_clean = df_clean.withColumn(
            c,
            initcap(trim(col(c))))


#  PARSE NORMAL DATE FORMATS

df_clean = df_clean.withColumn(
    "normal_date",
    coalesce(
        to_date(col("transaction_date"), "MM-dd-yyyy"),
        to_date(col("transaction_date"), "dd-MM-yyyy"),
        to_date(col("transaction_date"), "yyyy-MM-dd"),
        to_date(col("transaction_date"), "MM/dd/yyyy"),
        to_date(col("transaction_date"), "dd/MM/yyyy")))

# EXTRACT GREGORIANCALENDAR DATES

df_clean = (df_clean
    .withColumn(
        "gc_year",
        regexp_extract(
            col("transaction_date"),
            "YEAR=([0-9]+)",
            1
        ).cast("int"))
    .withColumn(
        "gc_month",
        regexp_extract(
            col("transaction_date"),
            "MONTH=([0-9]+)",
            1
        ).cast("int"))
    .withColumn(
        "gc_day",
        regexp_extract(
            col("transaction_date"),
            "DAY_OF_MONTH=([0-9]+)",
            1
        ).cast("int")))

# GregorianCalendar months start from 0
# Jan=0, Feb=1, ..., Dec=11

df_clean = df_clean.withColumn(
    "gc_date",
    when(
        col("gc_year").isNotNull(),
        make_date(
            col("gc_year"),
            col("gc_month") + 1,
            col("gc_day")
        )
    )
)

# FINAL DATE COLUMN

df_clean = df_clean.withColumn(
    "transaction_date_final",
    coalesce(
        col("normal_date"),
        col("gc_date")
    ))

# Replace original column

df_clean = (
    df_clean
    .drop("transaction_date")
    .withColumnRenamed(
        "transaction_date_final",
        "transaction_date"
    )
)

# DATE DIMENSIONS

df_clean = (
    df_clean
    .withColumn("year", year("transaction_date"))
    .withColumn("month_num", month("transaction_date"))
    .withColumn("month_name", date_format("transaction_date", "MMMM"))
    .withColumn("quarter", quarter("transaction_date"))
    .withColumn("day_of_week", date_format("transaction_date", "EEEE"))
)

# DROP HELPER COLUMNS

df_clean = df_clean.drop(
    "normal_date",
    "gc_year",
    "gc_month",
    "gc_day",
    "gc_date"
)

#  VALIDATION
print("=" * 50)
print("TOTAL ROWS :", df_clean.count())

print(
    "NULL DATES :",
    df_clean.filter(
        col("transaction_date").isNull()
    ).count()
)

print("=" * 50)

df_clean.select(
    "transaction_date",
    "year",
    "month_name",
    "quarter",
    "day_of_week"
).show(20, False)



# In[9]:


#  CATEGORY VALUE STANDARDIZATION

# Some category values in the raw data use abbreviations or shorthand (e.g. "Elec", "Furn"). 

from pyspark.sql.functions import col, when

category_mapping = {
    "Elec"        : "Electronics",
    "Furn"        : "Furniture",
    "Home"       : "Home Appliances",         
}
category_expr = col("category")

for abbrev, full_name in category_mapping.items():
    category_expr = when(
        col("category") == abbrev,
        full_name
    ).otherwise(category_expr)

# Applying the expression — only the matched values are changed,
df_clean = df_clean.withColumn("category", category_expr)

# Validation 
print("=" * 50)
print("CATEGORY VALUES AFTER STANDARDIZATION")
print("=" * 50)
df_clean.groupBy("category") \
        .count() \
        .orderBy("category") \
        .show(truncate=False)


# In[10]:


display(df_clean.head(3))


# ### PII masking with SHA-256

# In[11]:


from pyspark.sql.functions import sha2, lower, trim, when, lit
# SHA-256 is a one-way cryptographic hash.
# "john@email.com" always becomes the same 64-character hex string.
# It CANNOT be reversed — this satisfies data protection requirements.
# sha2(column, 256) is PySpark's built-in function — no extra library needed.

def mask_pii_col(df, original_col, masked_col):
    """Hash a PII column with SHA-256 and drop the original."""
    if original_col not in df.columns:
        print(f"  Skipping '{original_col}' — column not found.")
        return df
    df = df.withColumn(
        masked_col,
        when(
            col(original_col).isNull() |
            (trim(col(original_col)) == '') |
            (lower(trim(col(original_col))) == 'not provided'),
            lit('MASKED_NONE')
        ).otherwise(
            sha2(lower(trim(col(original_col))), 256)
        )
    ).drop(original_col)
    print(f"  '{original_col}' masked → '{masked_col}'")
    return df

print("Applying PII masking...")

# Use the EXACT column names from YOUR dataset
df_clean = mask_pii_col(df_clean, 'email', 'email_masked')
df_clean = mask_pii_col(df_clean, 'phone', 'phone_masked')

print("\nColumns after masking:")
print(df_clean.columns)

print("\nSample masked email (64-char SHA-256 hash):")
df_clean.select('email_masked').show(1, truncate=False)


# ###  Enrich with product dimension table

# In[12]:


from pyspark.sql.functions import isnan, col

df_clean.filter(isnan(col("price"))).count()


# In[13]:


from pyspark.sql.functions import col, when, isnan

# Product dimension
product_dim_clean = product_dim.select(
    "product_id",
    col("price").alias("standard_product_price")
)

# Join
df_enriched = (
    df_clean
    .join(
        product_dim_clean,
        on="product_id",
        how="left"
    )
)

# Handling both Null and NaN price values
df_enriched = df_enriched.withColumn(
    "price",
    when(
        col("price").isNull() | isnan(col("price")),
        col("standard_product_price")
    ).otherwise(col("price"))
)

# Drop helper column
df_enriched = df_enriched.drop("standard_product_price")


# In[14]:


display(df_enriched.head(3))


# ### Calculate revenue

# In[15]:


from pyspark.sql.functions import round as spark_round

# Revenue formula:
# If discount exists: (price - discount*price) * quantity
# If no discount col : price * quantity

DISCOUNT_COL = 'discount'  

if DISCOUNT_COL in df_enriched.columns:
    df_enriched = df_enriched.withColumn(
        'revenue',
        spark_round(
            (col('price') - col(DISCOUNT_COL)*col('price')) * col('quantity'),
            2
        )
    )
    print("Revenue calculated with discount applied.")
else:
    df_enriched = df_enriched.withColumn(
        'revenue',
        spark_round(col('price') * col('quantity'), 2)
    )
    print("Revenue calculated (no discount column found).")

# Remove any negative revenue rows (returns / data errors)
neg_rev = df_enriched.filter(col('revenue') < 0).count()
df_enriched = df_enriched.filter(col('revenue') >= 0)
print(f"Removed {neg_rev} negative revenue rows.")

total_rev = df_enriched.agg({'revenue': 'sum'}).collect()[0][0]
print(f"\nTotal Revenue : {total_rev:,.2f}")
print(f"Final row count: {df_enriched.count():,}")


# In[16]:


display(df_enriched.head(3))


# ### Write the Silver Delta table

# In[17]:


# Write the fully cleaned and enriched dataset as a managed Delta table

df_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail_silver")

print("Silver table written: retail_silver")
print(f"Rows: {df_enriched.count():,}")
print(f"Columns: {len(df_enriched.columns)}")


# ### Writing all Gold KPI tables

# In[18]:


from pyspark.sql.functions import (
    sum as spark_sum,
    count, avg, countDistinct,
    round as spark_round
)

# Gold 1: Revenue by Category 
gold_category = df_enriched.groupBy('category').agg(
    spark_round(spark_sum('revenue'), 2).alias('total_revenue'),
    count('*').alias('transaction_count'),
    spark_round(avg('revenue'), 2).alias('avg_order_value')
).orderBy('total_revenue', ascending=False)

gold_category.write \
    .format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_revenue_by_category")
print("  gold_revenue_by_category written")
gold_category.show()

# Gold 2: Revenue by City 
gold_city = df_enriched.groupBy('city').agg(
    spark_round(spark_sum('revenue'), 2).alias('total_revenue'),
    count('*').alias('transaction_count'),
    countDistinct('email_masked').alias('unique_customers')
).orderBy('total_revenue', ascending=False)

gold_city.write \
    .format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_revenue_by_city")
print("  gold_revenue_by_city written")

# Gold 3: Top products 
PRODUCT_COL = 'product_name' 

gold_products = df_enriched.groupBy(PRODUCT_COL).agg(
    spark_sum('quantity').alias('units_sold'),
    spark_round(spark_sum('revenue'), 2).alias('total_revenue')
).orderBy('total_revenue', ascending=False)

gold_products.write \
    .format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_top_products")
print("  gold_top_products written")

# Gold 4: Monthly revenue trend
gold_monthly = df_enriched.groupBy('year', 'month_num', 'month_name').agg(
    spark_round(spark_sum('revenue'), 2).alias('monthly_revenue'),
    count('*').alias('transaction_count')
).orderBy('year', 'month_num')

gold_monthly.write \
    .format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_monthly_trend")
print("  gold_monthly_trend written")

# Gold 5: Revenue by channel 
if 'sales_channel' in df_enriched.columns:
    gold_channel = df_enriched.groupBy('sales_channel').agg(
        spark_round(spark_sum('revenue'), 2).alias('total_revenue'),
        count('*').alias('transaction_count')
    ).orderBy('total_revenue', ascending=False)

    gold_channel.write \
        .format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("gold_revenue_by_channel")
    print("  gold_revenue_by_channel written")

print("\nAll Gold tables written successfully.")


# ### Final summary printout 

# In[19]:


from pyspark.sql.functions import spark_partition_id

total_revenue   = df_enriched.agg(spark_sum('revenue')).collect()[0][0]
total_txns      = df_enriched.count()
avg_order_val   = total_revenue / total_txns

print("=" * 50)
print("     PIPELINE COMPLETE — FINAL SUMMARY")
print("=" * 50)
print(f"  Total Revenue         : ₹{total_revenue:>14,.2f}")
print(f"  Total Transactions    :  {total_txns:>14,}")
print(f"  Average Order Value   : ₹{avg_order_val:>14,.2f}")
print(f"  Silver table rows     :  {df_enriched.count():>14,}")
print(f"  Gold tables written   :  5")
print("=" * 50)
print("\nDelta Tables in Lakehouse:")
spark.sql("SHOW TABLES").show(truncate=False)


# In[20]:


df_enriched.write.format("csv").option("header", "true").mode("overwrite").save("Files/Clean_Retail_Data")


# ### Write all Gold KPI tables
