#!/bin/bash
#$ -V
#$ -cwd
#$ -q all.q
#$ -l vf=20G
#$ -pe smp 1

source /nas02/software/conda/Miniconda3/miniconda3/bin/activate /nas02/project/huyifan/software/nextflow/v24.04.4
nextflow -log nextflow.log \
        run  /nas02/pipeline/large_pla/current_version/main.nf  \
        -profile sge -qs 20 -resume \
        --input ./input.tsv & pid=$!
#--------------------------------------------------------------------------------------------------------------------
wait $pid
if [ $? -eq 0 ]; then \
	echo "Command succeeded at $(date)" > done
else \
	echo "Command failed at $(date)" > error
fi

