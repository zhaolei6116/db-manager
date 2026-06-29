cd /nas02/project/zhaolei/pipeline/data_management
source venv/bin/activate
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

python src/main.py

# python src/services/ingestion_service.py  
# python src/services/validation_service.py   
# python src/services/analysis_service.py
