#!/bin/bash

#SBATCH --nodes=1
#SBATCH --mem-per-cpu=10GB
#SBATCH --output=/cluster/home/anmusso/reddit/src/transform/slurm_logs/%j.out

module load stack/2024-06 python/3.11.6
source /cluster/home/anmusso/quantization/myenv/bin/activate

python /cluster/home/anmusso/reddit/src/transform/job.py "$@"