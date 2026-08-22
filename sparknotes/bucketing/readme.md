# What is Bucketing ? 

**Apache Spark bucketing** is a performance optimization technique used to divide large datasets into smaller, more manageable chunks based on the **hash value** of a specific column. By pre-organizing data on disk, you can significantly accelerate common operations like **filtering, joins, and aggregations** by reducing the need for costly data shuffling (0:26 - 0:38).

### Define- 
`Bucketing is a physical data-layout technique where Spark distributes rows into a fixed number of buckets using a hash of one or more columns.`

### How Bucketing Works:
* **Hashing and Modulo:** When you bucket by a column (e.g., *product_id*), Spark computes the hash of the key and applies a modulo operation with the number of buckets to determine which file a row belongs to.
* **Avoiding Shuffles:** In a standard join or aggregation, Spark typically performs a "shuffle" to move identical keys to the same executor. With bucketed data, this redistribution is already handled on disk, allowing Spark to skip the expensive shuffle step entirely.

* **Bucket Pruning:** During filtering, Spark can use the bucketing metadata to scan only the specific bucket containing the requested data, effectively ignoring unnecessary files and speeding up retrieval.


## How to decide the number of Buckets?

### Key Considerations:
* **Selecting Bucket Count:** A common heuristic is to aim for an optimal bucket size between **128 MB and 200 MB** per bucket.

* **Comparison with Partitioning:** Unlike partitioning—which can lead to the "small file problem" if the column has high cardinality—bucketing distributes data more predictably into a fixed number of files .

* **Best for Repeated Operations:** Bucketing is most beneficial when the same datasets are joined or aggregated frequently, as the overhead of writing the bucketed files is offset by the savings in subsequent query performance.

Deciding the optimal number of buckets involves balancing the size of your dataset with a target bucket size. Here is the approach discussed in the video:

* **Optimal Bucket Size:** The recommended target size for each bucket is between **128 MB and 200 MB**.

* **The Formula:** You can determine the number of buckets by dividing the total size of your dataset ($X$) by your chosen optimal bucket size:

$$\text{Number of Buckets} = \frac{\text{Total Dataset Size}}{\text{Optimal Bucket Size (e.g., 200 MB)}}$$

- For example, if your dataset is **1 GB (1,000 MB)**, dividing by **200 MB** would result in **5 buckets**. 

- To estimate the total size of your dataset, you can consider the number of records, the number of columns, and the approximate byte width of the variables in your data.


## Which are the scenarios when bucketing might shuffle the data ? 

While bucketing is designed to minimize data movement, it can still trigger a shuffle in certain scenarios where the bucketing strategy does not align with the query requirements. Here are the primary cases where this happens:

* **Mismatched Bucket Counts:** If you are performing a join between two bucketed datasets, but they were created with a different number of buckets ($X$ vs $Y$), Spark must shuffle one of the datasets to redistribute it into the target number of buckets so the join can proceed.

* **Incorrect Join Keys:** Even if both datasets are bucketed with the same number of buckets, if you join them using a column **other than** the one used for bucketing, the bucketing metadata is rendered useless for that operation. This forces Spark to perform a full shuffle to reorganize the data by the new join key.

- In essence, bucketing only avoids a shuffle when the data is already organized exactly as the operation expects: using the same join key and the same number of buckets across both involved datasets.


## Example

```
df.write \
    .format("parquet") \
    .bucketBy(8, "user_id") \
    .sortBy("user_id") \
    .mode("overwrite") \
    .saveAsTable("bucketed_user_table")
```
---
#### How to read it?

```
bucketed_df = spark.table("bucketed_user_table")

```
---

### Rules

- Pair with Sorting: Always use `.sortBy()` alongside `.bucketBy()` to prepare data for fast sort-merge joins

## What is Spark Catalog ? 

- The Spark Catalog is where Spark keeps metadata about tables, databases, views, functions, and other SQL objects.

- The Bucketing Metadata is also stores in spark catalog.

- Think of it like an index/registry of tables. Below information is metadata

```
Table name:
customers

Location:
s3://company-data/warehouse/customers/

Columns:
customer_id
name
...

Bucket count:
8

Bucket columns:
customer_id
```
---

## Where does the Catalog itself live?

- This depends on what catalog you are using.

- In a simple Spark installation, Spark can use an in-memory/session catalog for temporary objects and can use a Hive-compatible metastore for persistent table metadata.

## Architecture

```
                    Spark
                      |
                 Spark Catalog
                      |
             +--------+--------+
             |                 |
       Table Metadata      Table Location
             |                 |
       bucketBy(8, id)       S3 / HDFS /
       schema                NFS / Local
                                 |
                                 ↓
                         Actual bucketed files

```
---

| Feature                                 | `partitionBy()`                       | `bucketBy()`                                   |
| --------------------------------------- | ------------------------------------- | ---------------------------------------------- |
| Main purpose                            | Reduce data scanned                   | Reduce shuffle for suitable joins/aggregations |
| Organization                            | Directory/folder by value             | Fixed number of hash buckets                   |
| Number                                  | Depends on distinct values            | Explicit number of buckets                     |
| Good column                             | Low/medium cardinality filter columns | High-cardinality join/grouping columns         |
| Typical example                         | `year`, `month`, `country`            | `customer_id`, `account_id`                    |
| Query benefit                           | Partition pruning                     | Potential shuffle elimination/reduction        |
| High-cardinality column                 | Usually bad                           | Often suitable                                 |
| Creates folder per value                | Yes                                   | No, not the same model                         |
| Requires catalog metadata               | Not necessarily                       | Yes, for Spark to exploit bucketing metadata   |
| Automatically guarantees faster queries | No                                    | No                                             |
