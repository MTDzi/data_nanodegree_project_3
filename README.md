# Introduction

In this project we copied a dataset storred as JSONs on S3 to staging tables on Redshift to then turn them into a star schema using PostgreSQL commands on Redshift itself.

# Commands

To move the JSONs from S3 to Redshift run the following command:
```python
python create_tables.py
```

To turn the resulting tables into the correct schema run:
```python
python etl.py
```

## Exploration

I also included the notebook in which I developed the SQL and COPY commands + there's code for setting up the required IAM role and Redshift cluster.


