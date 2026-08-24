### What is Salting ?
- Salting improves performance by preventing data skew in Apache Spark operations like joins and aggregations.  When a specific key appears too frequently in a dataset, it overloads a single shuffle partition, creating a bottleneck.

- By adding a random salt value to the join or group-by key, the data is distributed across multiple partitions rather than being concentrated on one node.
- This ensures that the computational load is spread more evenly, allowing parallel processing to happen efficiently

## Salting in Joins (Steps to apply the salting)

- To mitigate data skew in Apache Spark, salting adds randomness to your join or group-by keys to ensure data is evenly distributed across shuffle partitions. 
- Here are the steps for both scenarios:

### Implementing Salting in Joins :

1. Choose a Salt Number: Select a value that represents how much you want to distribute the skewed data.
2. Salt the First Data Frame: Add a new column with random integers between 0 and your salt number (eg. column_name- Salt_number).

3. Prepare the Second Data Frame: Create an array column of all possible salt values (0 to salt number - 1) and use the explode function to replicate rows for every possible salt value.

4. Join: Perform the join using both the original key and the new salt column.

### Implementing the salting in groupBy()>

1. Choose a Salt Number: Select a number based on your shuffle partition distribution.

2. Add Salt Column: Assign a random integer (0 to salt number - 1) to each row in your data frame 

3. Initial Aggregation: Perform a groupBy using both the original value column and the new salt column to perform a partial count/aggregation (21:14).

4. Final Aggregation: Perform a second groupBy on the original value column only, summing the partial counts obtained from the previous step to arrive at the final result 

### How to Choose a correct salt number?

1. Purpose: The salt number determines the degree to which you distribute your skewed data across your shuffle partitions.

2. Avoid Too Small: If you choose a number that is too small, your data will remain "jam-packed" into a single partition, failing to resolve the skew.

3. Avoid Too Large: If you choose a number that is too large, you risk over-fragmenting your data into very tiny, inefficient pieces, which can also degrade performance.
* Practical Tip: Start by looking at your current shuffle partition count. The goal is to choose a number that ensures your data is spread more evenly across those available partitions.

----
Refer the Salting Practical example in ([Pyspark_Examples](../../pyspark_scenarios.ipynb))