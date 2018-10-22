# SPJUA-Query-Evaluator
Implemented a SQL Query Engine with efficient JOIN and probabilistic data structures to support TPC-H queries and data set.

Given a list of queries like below the program will parse and evaluate the query.

1. CREATE TABLE R (A int, B date, C string, ... )
2. SELECT A, B, ... FROM R
3. SELECT A, B, ... FROM R WHERE ...
4. SELECT A+B AS C, ... FROM R
5. SELECT A+B AS C, ... FROM R WHERE ...
6. SELECT * FROM R
7. SELECT * FROM R WHERE ...
8. SELECT R.A, ... FROM R WHERE ...
9. SELECT Q.C, ... FROM (SELECT A, C, ... FROM R) Q WHERE ...

The method which was used was volcano style iteration of the queries.

Volcano-Style Computation (Iterators)
=====================================
This is an example of volcano style computation.
```javascript
with open('data.dat', 'r') as f:
  for line in f:
    fields = split(",", line)
    if(fields[2] != "Ensign" and int(fields[3]) > 25):
      print(fields[1])
```
```SQL
SELECT fields[1] FROM 'data.dat' 
WHERE fields[2] != "Ensign" AND CAST(fields[3] AS int) > 25
```
## Query rewriting
The first steps to execute an SQL command is to parse it into a realtional algebra tree. Then for query optimization we push down the selection operators and replace every selection operator on top of a cross product with a hash join. 
Another optimization I did is projection pushdown operation. Essentially, you only read the attributes that you will need in the query from the each database file, and discard all the attributes that you will not use. 
Implemented all these joins to use in different occasions like when the join fits in memory or when the join goes out of our memory size.
- Sort-Merge Join: An implementation of sort-merge join for use on out-of-memory joins.
- 1-Pass Hash Join: An implementation of the in-memory hash join algorithm.
- Index Nested-Loop Join: An implementation of index nested loop join algorithm.
- Join Specialization: Rewrite Selection + CrossProduct operators into Hash Join operators.
- Index Scanner: Search values with index lookup.
