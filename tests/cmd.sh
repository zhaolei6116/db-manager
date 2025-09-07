cd /nas02/project/zhaolei/pipeline/data_management
source venv/bin/activate
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
# python -m unittest tests/test_data_processor.py -v
python  src/processing/json_data_processor.py