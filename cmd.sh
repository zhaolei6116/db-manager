cd /nas02/project/zhaolei/pipeline/data_management
source venv/bin/activate
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

python src/main.py
