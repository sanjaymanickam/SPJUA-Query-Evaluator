# SPJUA-Query-Evaluator
Implemented a SQL Query Engine with efficient JOIN and probabilistic data structures to support TPC-H queries and data set.

Given a list of queries like below the program will parse and evaluate the query.
1|CREATE TABLE R (A int, B date, C string, ... )
2|SELECT A, B, ... FROM R
3|SELECT A, B, ... FROM R WHERE ...
4|SELECT A+B AS C, ... FROM R
5|SELECT A+B AS C, ... FROM R WHERE ...
6|SELECT * FROM R
7|SELECT * FROM R WHERE ...
8|SELECT R.A, ... FROM R WHERE ...
9|SELECT Q.C, ... FROM (SELECT A, C, ... FROM R) Q WHERE ...

The method which was used was volcano style iteration of the queries.

Volcano-Style Computation (Iterators)
=====================================
This is an example of volcano style computation.
'code()'
with open('data.dat', 'r') as f:
  for line in f:
    fields = split(",", line)
    if(fields[2] != "Ensign" and int(fields[3]) > 25):
      print(fields[1])
'
'''SQL
'''
SELECT fields[1] FROM 'data.dat' 
WHERE fields[2] != "Ensign" AND CAST(fields[3] AS int) > 25
