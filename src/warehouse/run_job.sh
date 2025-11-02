#!/bin/bash
#SBATCH --job-name=dbt_spark_local
#SBATCH --time=03:00:00
#SBATCH --cpus-per-task=8          
#SBATCH --mem-per-cpu=4GB         
#SBATCH --output=%j.out


module load stack/2024-06 gcc/12.2.0 python/3.11.6 openjdk/11.0.20.1_1
source reddit_env/bin/activate

cd src/warehouse
set -a
source .env
set + a

dbt debug
dbt run
