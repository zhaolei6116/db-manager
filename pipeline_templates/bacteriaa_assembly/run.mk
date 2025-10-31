#!/bin/bash
#!/bin/bash
#$ -V
#$ -cwd
#$ -q all.q
#$ -l vf=20G
#$ -pe smp 1


source /nas02/software/conda/Miniconda3/miniconda3/bin/activate /nas02/project/huyifan/software/nextflow/v24.04.4
nextflow -log nextflow.log \
	run  /nas02/pipeline/bac_genome_assembly/latest_version/main.nf \
	-profile sge -qs 20 -resume \
	--csv ./input.tsv 
