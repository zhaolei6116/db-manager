
# tests/test_data_processor.py
import json
import unittest
from unittest.mock import patch
from pathlib import Path
from src.processing.json_data_processor import DataProcessor

class TestDataProcessor(unittest.TestCase):
    def setUp(self):
        self.json_data = {
            "Client": "SD250828132038",
            "Detect_no": "S22508281622",
            "Sample_name": "CTP-3",
            "Sample_type": "plasmid",
            "Sample_type_raw": "菌液(需摇菌)",
            "Resistance_type": "卡那",
            "user_name": "蔡万钏",
            "Mobile": "17857021604",
            "Custom_name": "华东理工大学-王玮（WF)",
            "Remarks": "",
            "Reanalysis_times": 0,
            "Experiment_times": 2,
            "Allanalysis_times": 1,
            "Project": "插入片段测通",
            "Ref": "TGCCTAGTGAATGCTCCGTA,AGTATCACAACCTAGCTATC",
            "Species_name": "-",
            "Genome_size": "-",
            "Data_volume": "-",
            "Report_path": "/kfservice/s/primecx/25083011/S22508281622_2.zip",
            "Report_raw_path": "-",
            "PLASMID_LENGTH": 15000,
            "Length": 2500,
            "Sample_con": "1.00 ",
            "Sample_status": "风险检测",
            "Laboratory": "S",
            "Experiment_no": "2508280232",
            "Batch_id": "25083011",
            "Board": "S250830016",
            "Board_id": "B1",
            "Sequencer_id": "01",
            "Machine_ver": "V3",
            "Barcode_type": "CW-Bar16bp-2208-1",
            "Barcode_prefix": "barcode",
            "Barcode_number": "0674",
            "Unqualifytime": "1970-01-01 08:00:00",
            "Unknown_field": "test_value"
        }
        self.json_path = Path('test.json')
        with open(self.json_path, 'w', encoding='utf-8') as f:
            json.dump(self.json_data, f)
        self.processor = DataProcessor()

    def tearDown(self):
        if self.json_path.exists():
            self.json_path.unlink()

    def test_parse_json_file(self):
        """测试解析JSON文件"""
        result = self.processor.parse_json_file(self.json_path)
        self.assertIsNotNone(result)
        self.assertEqual(set(result.keys()), {'project', 'sample', 'batch', 'sequence', 'sequence_run'})

        # 检查project表
        project_dict = result['project']
        self.assertEqual(project_dict['project_id'], "SD250828132038")
        self.assertEqual(project_dict['custom_name'], "华东理工大学-王玮（WF)")
        self.assertEqual(project_dict['user_name'], "蔡万钏")

        # 检查sample表
        sample_dict = result['sample']
        self.assertEqual(sample_dict['sample_id'], "S22508281622")
        self.assertEqual(sample_dict['project_type'], "插入片段测通")
        self.assertEqual(sample_dict['resistance_type'], "卡那")

        # 检查batch表
        batch_dict = result['batch']
        self.assertEqual(batch_dict['batch_id'], "25083011")
        self.assertEqual(batch_dict['laboratory'], "S")
        self.assertEqual(batch_dict['sequencer_id'], "01")

        # 检查sequence表
        sequence_dict = result['sequence']
        self.assertEqual(sequence_dict['sample_id'], "S22508281622")
        self.assertEqual(sequence_dict['batch_id'], "25083011")
        self.assertEqual(sequence_dict['barcode_prefix'], "barcode")
        self.assertEqual(sequence_dict['barcode_number'], "0674")

        # 检查sequence_run表
        sequence_run_dict = result['sequence_run']
        self.assertEqual(sequence_run_dict['sample_id'], "S22508281622")
        self.assertEqual(sequence_run_dict['batch_id'], "25083011")
        self.assertEqual(sequence_run_dict['lab_sequencer_id'], "SSequenator01")
        self.assertEqual(sequence_run_dict['barcode'], "barcode0674")
        self.assertEqual(sequence_run_dict['data_status'], "pending")
        self.assertTrue(sequence_run_dict['batch_id_path'].startswith("/bioinformation/Project/Sequencing/SSequenator01/25083011"))

    @patch('src.utils.data_processor.logger')  # 修正：使用正确的模块路径
    def test_unknown_field_logging(self, mock_logger):
        """测试未知字段记录日志"""
        self.processor.parse_json_file(self.json_path)
        mock_logger.warning.assert_called()

    def test_invalid_json_file(self):
        """测试无效JSON文件"""
        invalid_path = Path('invalid.json')
        with open(invalid_path, 'w', encoding='utf-8') as f:
            f.write("invalid json")
        result = self.processor.parse_json_file(invalid_path)
        self.assertIsNone(result)
        invalid_path.unlink()

    def test_nonexistent_file(self):
        """测试不存在的文件"""
        result = self.processor.parse_json_file(Path('nonexistent.json'))
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()