source /nas02/software/conda/Miniconda3/miniconda3/bin/activate /nas02/project/huyifan/software/nextflow/v24.04.4
nextflow -log nextflow.log \
	run -qs 10 -resume /nas02/pipeline/bac_genome_assembly/latest_version/main.nf \
	-profile sge \
	--csv ./input.tsv 
	--outdir ./ 
