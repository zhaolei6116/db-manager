run:
	
	
	#--------------------------------------------------------------------------------------------------------------------
	cleanup() {
		echo "终止Nextflow主进程 (PID: $$pid)"; \
		kill -TERM "$$pid" 2>/dev/null; \
		wait "$$pid"  # 确保Nextflow完成清理; \
		exit 143
	}
	trap cleanup TERM INT  # 捕获Slurm发送的信号
	#--------------------------------------------------------------------------------------------------------------------
	sleep 100
	
	# main script
	export PATH=/nas04/Software/singularity-ce-4.2.1/bin/:$$PATH
	export NXF_OFFLINE=true
	
	cd $(run_path)
	rm -f error done

	#--------------------------------------------------------------------------------------------------------------------
	source /nas02/software/conda/Miniconda3/miniconda3/bin/activate /nas02/project/huyifan/software/nextflow/v24.04.4
	nextflow -log nextflow.log \
		run -qs 10 -resume /nas02/pipeline/bac_genome_assembly/latest_version/main.nf \
		-profile sge \
		-with-weblog http://$(host):$(port)/nextflow/$(uuid) \
		--batch $(batch) \
		--csv ./input.tsv \
		--outdir ./ & pid=$$!
	#--------------------------------------------------------------------------------------------------------------------
	
	wait $$pid
	
	if [ $$? -eq 0 ]; then \
		echo "Command succeeded at $$(date)" > done
	else \
		echo "Command failed at $$(date)" > error
	fi
