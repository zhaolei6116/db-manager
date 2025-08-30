# tests/test_models.py
"""Unit tests for database models."""
import unittest
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from src.core.database import get_session, Base
from src.core.models import Project, Sample, Batch, Sequencing, AnalysisInput, AnalysisTask, SequenceRun, ProcessData

class TestModels(unittest.TestCase):
    def setUp(self):
        self.session = get_session(config_file='config/mysql_config.yaml')
        # 清空相关表，保留表结构
        self.session.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))  # 临时禁用外键约束
        for table in ['analysis_tasks', 'analysis_inputs', 'processed_data_dependency', 'process_data', 'sequence_run', 'sequence', 'sample', 'batch', 'project']:
            self.session.execute(text(f"TRUNCATE TABLE {table};"))
        self.session.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))  # 恢复外键约束
        Base.metadata.create_all(self.session.bind)
        self.session.commit()

    def tearDown(self):
        self.session.close()

    def test_create_project(self):
        project = Project(project_id="P001", custom_name="Test Project")
        self.session.add(project)
        self.session.commit()
        result = self.session.query(Project).filter_by(project_id="P001").first()
        self.assertIsNotNone(result)
        self.assertEqual(result.custom_name, "Test Project")

    def test_analysis_input_task_relationship(self):
        # 插入依赖表记录，满足外键约束
        project = Project(project_id="P001")
        sample = Sample(sample_id="S001", project_id="P001")
        batch = Batch(batch_id="B001")
        sequence = Sequencing(sequence_id="SR001", sample_id="S001", batch_id="B001")
        sequence_run = SequenceRun(sequence_id="SR001", sample_id="S001", batch_id_path="B001")
        process = ProcessData(process_id=1, sequence_id="SR001", process_status="no")
        input = AnalysisInput(
            input_id="I001", process_id=1, sample_id="S001", project_id="P001",
            batch_id="B001", analysis_status="no", retry_count=0
        )
        task1 = AnalysisTask(
            task_id="T001", input_id="I001", sample_id="S001", project_id="P001",
            analysis_status="pending"
        )
        task2 = AnalysisTask(
            task_id="T002", input_id="I001", sample_id="S001", project_id="P001",
            analysis_status="failed"
        )
        self.session.add_all([project, sample, batch, sequence, sequence_run, process, input, task1, task2])
        self.session.commit()
        result = self.session.query(AnalysisInput).filter_by(input_id="I001").first()
        self.assertEqual(len(result.analysis_task), 2)  # 1:N 关系

    def test_json_import(self):
        json_data = {
            "Client": "SD250726162017",
            "Detect_no": "T22507265020",
            "Sample_name": "GH-2-16S",
            "Sample_type": "dna",
            "Sample_type_raw": "菌体",
            "Resistance_type": "",
            "user_name": "有康",
            "Mobile": "13385717187",
            "Custom_name": "有康生物",
            "Remarks": "",
            "Reanalysis_times": 0,
            "Experiment_times": 1,
            "Allanalysis_times": 0,
            "Project": "细菌鉴定(16S)",
            "Ref": "N",
            "Species_name": "-",
            "Genome_size": "-",
            "Data_volume": "-",
            "Report_path": "/kfservice/t/primecx/25072909/T22507265020.zip",
            "Report_raw_path": "/kfservice/t/primecx/25072909/T22507265020_rawdata.zip",
            "PLASMID_LENGTH": 0,
            "Length": 0,
            "Sample_con": "378",
            "Sample_status": "合格",
            "Laboratory": "T",
            "Experiment_no": "",
            "Batch_id": "25072909",
            "Board": "T250729004",
            "Board_id": "H2",
            "Sequencer_id": "06",
            "Machine_ver": "V3",
            "Barcode_type": "CW-Bar16bp-2208-1",
            "Barcode_prefix": "barcode",
            "Barcode_number": "0688",
            "Unqualifytime": "1970-01-01 08:00:00"
        }
        project = Project(
            project_id=json_data["Client"],
            custom_name=json_data["Custom_name"],
            user_name=json_data["user_name"],
            mobile=json_data["Mobile"],
            remarks=json_data["Remarks"]
        )
        sample = Sample(
            sample_id=json_data["Detect_no"],
            project_id=json_data["Client"],
            sample_name=json_data["Sample_name"],
            sample_type=json_data["Sample_type"],
            sample_type_raw=json_data["Sample_type_raw"],
            resistance_type=json_data["Resistance_type"],
            project_type=json_data["Project"],
            species_name=json_data["Species_name"],
            genome_size=json_data["Genome_size"],
            data_volume=json_data["Data_volume"],
            ref=json_data["Ref"],
            plasmid_length=json_data["PLASMID_LENGTH"],
            length=json_data["Length"]
        )
        batch = Batch(
            batch_id=json_data["Batch_id"],
            sequencer_id=json_data["Sequencer_id"],
            laboratory=json_data["Laboratory"]
        )
        sequencing = Sequencing(
            sequence_id=f"RUN_{json_data['Detect_no']}",
            sample_id=json_data["Detect_no"],
            batch_id=json_data["Batch_id"],
            board=json_data["Board"],
            board_id=json_data["Board_id"],
            machine_ver=json_data["Machine_ver"],
            barcode_type=json_data["Barcode_type"],
            barcode_prefix=json_data["Barcode_prefix"],
            barcode_number=json_data["Barcode_number"],
            reanalysis_times=json_data["Reanalysis_times"],
            experiment_times=json_data["Experiment_times"],
            allanalysis_times=json_data["Allanalysis_times"],
            experiment_no=json_data["Experiment_no"],
            sample_con=float(json_data["Sample_con"]),
            sample_status=json_data["Sample_status"],
            unqualifytime=json_data["Unqualifytime"],
            report_path=json_data["Report_path"],
            report_raw_path=json_data["Report_raw_path"],
            run_type="initial"
        )
        self.session.add_all([project, sample, batch, sequencing])
        self.session.commit()

        project_result = self.session.query(Project).filter_by(project_id="SD250726162017").first()
        sample_result = self.session.query(Sample).filter_by(sample_id="T22507265020").first()
        batch_result = self.session.query(Batch).filter_by(batch_id="25072909").first()
        seq_result = self.session.query(Sequencing).filter_by(sequence_id=f"RUN_T22507265020").first()
        self.assertIsNotNone(project_result)
        self.assertEqual(project_result.custom_name, "有康生物")
        self.assertIsNotNone(sample_result)
        self.assertEqual(sample_result.sample_name, "GH-2-16S")
        self.assertIsNotNone(batch_result)
        self.assertEqual(batch_result.laboratory, "T")
        self.assertIsNotNone(seq_result)
        self.assertEqual(seq_result.sample_con, 378.0)

if __name__ == "__main__":
    unittest.main()