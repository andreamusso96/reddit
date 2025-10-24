#!/bin/bash

#SBATCH --nodes=1
#SBATCH --mem-per-cpu=10GB

module load stack/2024-06 python/3.11.6
source /cluster/home/anmusso/quantization/myenv/bin/activate

python /cluster/home/anmusso/reddit/src/transform/job.py "$@"