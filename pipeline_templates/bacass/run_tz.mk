#!/usr/bin/env bash
#SBATCH --job-name=kwbio_bacass
#SBATCH --partition=compute2
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=20G
#SBATCH --time=12:00:00
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err


export JAVA_HOME=/gpfs/kw/project/zhaolei/software
export NXF_JAVA_HOME="$JAVA_HOME"
export PATH="/gpfs/kw/software/git/v2.34.1/bin:$JAVA_HOME/bin:/gpfs/kw/project/zhaolei/software/nextflow:/opt/ohpc/pub/libs/singularity/singularity-ce-4.2.1/bin:$PATH"
export NXF_HOME=/gpfs/kw/project/zhaolei/software/nextflow_cache
export NXF_VER=25.10.7
export NXF_OFFLINE=true
export NXF_SINGULARITY_CACHEDIR=/gpfs/kw/project/zhaolei/singularity/kwbio-bacass
export KWBIOMEDICINE_SINGULARITY_CACHEDIR="$NXF_SINGULARITY_CACHEDIR"
export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8


nextflow -version
java -version
singularity --version

# PIPELINE=/gpfs/kw/project/zhaolei/pipeline/kwbio-bacass/main.nf

nextflow -log "./nextflow.log" \
    run /gpfs/kw/project/zhaolei/pipeline/kwbio-bacass/main.nf \
    -profile slurm,singularity,kw_tz \
    -resume \
    --input input.tsv \
    -qs 20 &
pid=$!

#--------------------------------------------------------------------------------------------------------------------
wait $pid
if [ $? -eq 0 ]; then \
        echo "Command succeeded at $(date)" > "$out_dir/done"
else \
        echo "Command failed at $(date)" > "$out_dir/error"
fi