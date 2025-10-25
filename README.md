# reddit

This project aims to understand how people heal from plantar fasciitis by analyzing reddit submission/comments


How to run the job on spark (test run):
- Login to the euler
- Load the appropriate modules:
`module stack/2025-06 gcc/12.2.0 python/3.13.0 openjdk/17.0.11_9`
- Activate the venv: 
`source reddit_env/bin/activate`
- Pull the new code
`cd reddit`
then 
`git pull`
- Run an interactive job
`srun --pty bash`
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
`dbt run-operation stage_external_sources --args "select: raw.submissions`
- Run the dbt script
`dbt run --select commnets_and_submission_with_keywords`

Once this runs with the dummy data (just 2005 and 2006 for both comments and submission), you can try with the whole dataset. 
The only issue is that the scratch has max 2.5TB memory, so I need to figure out what to do with that. 


