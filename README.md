# reddit

This project aims to implement a keyword search for reddit data:

## Setting up on the cluster

- First load the software
`module load stack/2024-06 gcc/12.2.0 python/3.11.6 openjdk/11.0.20.1_1`
- Next create a venv
`python -m venv reddit_env`
- Activate venv
`source reddit_env/bin/activate`
- Install requirements
`pip install --upgrade pip`
`pip install dbt-core dbt-spark[session]`


## Running on the cluster

The job is run from a run_job.sh script. 
To run this script you need to:

1) Login to euler
2) Go to the project folder
`cd reddit/src/warehouse`
3) Set you environmental parameters by modifying the .env file there
`nano .env`
4) Run the run_job.sh via sbatch
`sbatch < run_jobs.sh`

## Speed tests

### submissions_with_keywords

A submissions_with_keywords query looks through each submission and checks if it contains some keywords. 
It then takes all submissions that contain the keywords and writes them to an output table. 

I ran the submissions_with_keywords query on the full 2005-2025 dataset with keywords 
'plantar fasciitis', 'plantar fascitis', 'heel pain', 'plantar fasciosis'

I used these specs for the job:

```
#SBATCH --time=03:00:00
#SBATCH --cpus-per-task=8         
#SBATCH --mem-per-cpu=4GB      
```   

The raw data files were stored in the directories:
RAW_SUBMISSIONS_PARQUET_DIR="/cluster/work/gess/coss/users/anmusso/reddit_parquet/submissions"
RAW_COMMENTS_PARQUET_DIR="/cluster/work/gess/coss/users/anmusso/reddit_parquet/comments"

I wrote the output to "/cluster/scratch/anmusso/"

The job took **55 minutes**.
I then bumped up the number of cores to 16 and the job took **28.6** minutes. 

### submissions_with_keywords + comments_and_submissions_with_keywords

The combination of these two queries does the following:
1) We look through each submission and checks if it contains some keywords. Filter out the submissions with a given keyword and write them in a table. 
2) We take the submissions outputted from 1) and find all comments relative to those submissions (with a join). We write comments + submissions to a table. 

I ran this query with the keywords: 'plantar fasciitis', 'plantar fascitis', 'heel pain', 'plantar fasciosis'

I used these specs for the job:
```
#SBATCH --time=03:30:00
#SBATCH --cpus-per-task=32      <--- NOTE HERE WE USED MORE CORES    
#SBATCH --mem-per-cpu=4GB         
```

The raw data files were stored in the directories:
RAW_SUBMISSIONS_PARQUET_DIR="/cluster/work/gess/coss/users/anmusso/reddit_parquet/submissions"
RAW_COMMENTS_PARQUET_DIR="/cluster/work/gess/coss/users/anmusso/reddit_parquet/comments"

I wrote the output to "/cluster/scratch/anmusso/"

The job took **X minutes** as a whole. Here is the breakdown:
- 897s (15 minutes) for submissions_with_keywords
- 2819s (47 minutes) for comments_and_submissions_with_keywords

I found 37111 submissions with 348353 total comments. 
