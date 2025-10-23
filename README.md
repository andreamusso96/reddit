# reddit

This project aims to understand how people heal from plantar fasciitis by analyzing reddit submission/comments


To run this on the cluster I will need to:
- Insert the addresses of the comments and submission on the cluster
- Decide the output dirs where the comments and submissions will be put in parquet
- Put that output dirs in _sources.yml from dbt
- Fix the profiles.yml to allow spark to use more memory and cores
- Change the spark_warhouse and metastore addresses
- I think those are the main addresses that need to change (maybe make them environment variables???)
