#!/bin/bash
#SBATCH --job-name=dbt_spark_local
#SBATCH --time=03:30:00
#SBATCH --cpus-per-task=32          
#SBATCH --mem-per-cpu=4GB         
#SBATCH --output=slurm-%j.out


module load stack/2024-06 gcc/12.2.0 python/3.11.6 openjdk/11.0.20.1_1
source /cluster/home/anmusso/reddit_env/bin/activate

cd /cluster/home/anmusso/reddit/src/warehouse

set -a
source .env
set + a

echo "RAW_SUBMISSIONS_PARQUET_DIR: $RAW_SUBMISSIONS_PARQUET_DIR"
echo "RAW_COMMENTS_PARQUET_DIR: $RAW_COMMENTS_PARQUET_DIR"

dbt debug
dbt run-operation stage_external_sources --vars "ext_full_refresh: true"
dbt run --select comments comments_with_keywords