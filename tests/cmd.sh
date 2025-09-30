cd /nas02/project/zhaolei/pipeline/data_management
source venv/bin/activate
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
# python -m unittest tests/test_data_processor.py -v
python  src/processing/json_data_processor.py

# {
#   'project': {'project_id': 'SD250829174355', 'custom_name': '江苏省农业科学院-李彬', 'user_name': '程振孔', 'mobile': '15296597895', 'remarks': ''}, 

#   'sample': {'sample_id': 'T22508295523', 'project_id': 'SD250829174355', 'sample_name': 'pYL85-AH-2', 'sample_type': 'plasmid', 

#   'sample_type_raw': '质粒', 'resistance_type': '', 'project_type': '插入片段测通', 'species_name': '-', 'genome_size': '-', 'data_volume': '-', 'ref': 'tcatgctatggattatgg', 'plasmid_length': 8084, 'length': 2500}, 

#   'batch': {'batch_id': '25083005', 'sequencer_id': '02', 'laboratory': 'T'}, 

#   'sequence': {'sample_id': 'T22508295523', 'batch_id': '25083005', 'board': 'T250829043', 'board_id': 'G3', 'machine_ver': 'V3', 'barcode_type': 'CW-Bar16bp-2208-1', 'barcode_prefix': 'barcode', 'barcode_number': '0119', 'reanalysis_times': 1, 'experiment_times': 1, 'allanalysis_times': 1, 'experiment_no': '', 'sample_con': ' 15.91 ', 'sample_status': '风险检测', 'unqualifytime': '1970-01-01 08:00:00', 'report_path': '/kfservice/t/primecx/25083005/T22508295523_R1.zip', 'report_raw_path': '-'}, 

#   'sequence_run': {'sample_id': 'T22508295523', 'batch_id': '25083005', 'lab_sequencer_id': 'TSequenator02', 'barcode': 'barcode0119', 'batch_id_path': '/bioinformation/Project/Sequencing/TSequenator02/25083005', 'raw_data_path': '/bioinformation/Project/Sequencing/TSequenator02/25083005/no_sample_id/', 'data_status': 'pending', 'process_status': 'no'}

# }