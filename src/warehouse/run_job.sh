#!/bin/bash
#SBATCH --job-name=dbt_spark_local
#SBATCH --time=03:00:00
#SBATCH --cpus-per-task=8          
#SBATCH --mem-per-cpu=4GB         
#SBATCH --output=slurm-%j.out


module load stack/2024-06 gcc/12.2.0 python/3.11.6 openjdk/11.0.20.1_1
source reddit_env/bin/activate

set -a
source .env
set + a

echo "RAW_SUBMISSIONS_PARQUET_DIR: $RAW_SUBMISSIONS_PARQUET_DIR"
echo "RAW_COMMENTS_PARQUET_DIR: $RAW_COMMENTS_PARQUET_DIR"

dbt debug
dbt run-operation stage_external_sources --vars "ext_full_refresh: true"
dbt run --select submissions comments submissions_with_keywords comments_and_submissions_with_keywords
dbt run-operation print_rowcount --args '{"model":"submissions_with_keywords"}'
dbt run-operation print_start_end_dates --args '{"model":"submissions_with_keywords"}'