# reddit

This project aims to understand how people heal from plantar fasciitis by analyzing reddit submission/comments


How to run the job on spark (test run):
- Login to the euler
- Load the appropriate modules:
`module load stack/2024-06 gcc/12.2.0 python/3.11.6 openjdk/11.0.20.1_1`
- Activate the venv: 
`source reddit_env/bin/activate`
- Pull the new code
`cd reddit`
then 
`git pull`
- Run an interactive job
`srun --pty bash`
- Enter the warehouse folder
`cd src/warehouse`

- Activate the environment variables
```
set -a
source .env
set + a
```
- Debug dbt
`dbt debug`

After this all should work and create a connection. 
Next, I already copied the data to the scratch and set the right envionment variables so the paths point to the correct place on the scrach
(for the .env file see reddit/src/warehouse/.env). So, you just need to:
- Add external dbt dependencies with 
`dbt run-operation stage_external_sources --args "select: raw.submissions"`
- Run the dbt script
`dbt run --select comments_and_submissions_with_keywords`

Once this runs with the dummy data (just 2005 and 2006 for both comments and submission), you can try with the whole dataset. 
The only issue is that the scratch has max 2.5TB memory, so I need to figure out what to do with that. 


# Installing the enviornment to run this on the cluster
First load the software:
`module load stack/2024-06 gcc/12.2.0 python/3.11.6 openjdk/11.0.20.1_1`
Next create a venv
`python -m venv reddit_env`
Activate venv
`source reddit_env/bin/activate`
Install requirements
`pip install --upgrade pip`
`pip install dbt-core dbt-spark[session]`
