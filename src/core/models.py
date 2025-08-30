"""
Database models for the db-manager project.
"""

from typing import List
from sqlalchemy import (
    Column, String, Integer, Float, DateTime, Enum, Text, JSON, 
    ForeignKey, text, Index, UniqueConstraint
)
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class Project(Base):
    __tablename__ = 'project'
    project_id = Column(String(50), primary_key=True)
    custom_name = Column(String(100))
    user_name = Column(String(50))
    mobile = Column(String(20))
    remarks = Column(String(255))
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False)
    samples = relationship("Sample", back_populates="project")
    analysis_inputs = relationship("AnalysisInput", back_populates="project")
    __table_args__ = {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}

class Sample(Base):
    __tablename__ = 'sample'
    sample_id = Column(String(50), primary_key=True)
    project_id = Column(String(50), ForeignKey('project.project_id'))
    sample_name = Column(String(100))
    sample_type = Column(String(50))
    sample_type_raw = Column(String(50))
    resistance_type = Column(String(50))
    project_type = Column(String(100))
    species_name = Column(String(100))
    genome_size = Column(String(50))
    data_volume = Column(String(50))
    ref = Column(Text)
    plasmid_length = Column(Integer)
    length = Column(Integer)
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False)
    project = relationship("Project", back_populates="samples")
    sequences = relationship("Sequencing", back_populates="sample")
    analysis_inputs = relationship("AnalysisInput", back_populates="sample")
    analysis_path_records = relationship("SampleAnalysisPathRecord", back_populates="sample", cascade="all, delete-orphan")
    __table_args__ = {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}

class Batch(Base):
    __tablename__ = 'batch'
    batch_id = Column(String(50), primary_key=True)
    sequencer_id = Column(String(50))
    laboratory = Column(String(10))
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False)
    sequences = relationship("Sequencing", back_populates="batch")
    analysis_inputs = relationship("AnalysisInput", back_populates="batch")
    process_records = relationship("BatchProcessRecord", back_populates="batch", cascade="all, delete-orphan")
    __table_args__ = {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}

class Sequencing(Base):
    __tablename__ = 'sequence'
    sequence_id = Column(String(50), primary_key=True)
    sample_id = Column(String(50), ForeignKey('sample.sample_id'))
    batch_id = Column(String(50), ForeignKey('batch.batch_id'))
    board = Column(String(50))
    board_id = Column(String(50))
    machine_ver = Column(String(20))
    barcode_type = Column(String(50))
    barcode_prefix = Column(String(50))
    barcode_number = Column(String(50))
    reanalysis_times = Column(Integer)
    experiment_times = Column(Integer)
    allanalysis_times = Column(Integer)
    experiment_no = Column(String(50))
    sample_con = Column(Float)
    sample_status = Column(String(50))
    unqualifytime = Column(DateTime)
    report_path = Column(String(255))
    report_raw_path = Column(String(255))
    version = Column(Integer, default=1)
    run_type = Column(Enum('initial', 'supplement', 'retest'), default='initial')
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False)
    sample = relationship("Sample", back_populates="sequences")
    batch = relationship("Batch", back_populates="sequences")
    sequence_run = relationship("SequenceRun", uselist=False, back_populates="sequencing", cascade="all, delete-orphan")
    __table_args__ = (
        UniqueConstraint('sample_id', 'batch_id', name='uix_sample_batch'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class SequenceRun(Base):
    __tablename__ = 'sequence_run'
    sequence_id = Column(String(50), ForeignKey('sequence.sequence_id'), primary_key=True, index=True)
    sample_id = Column(String(50), ForeignKey('sample.sample_id'), index=True)
    lab_sequencer_id = Column(String(50))
    barcode = Column(String(50))
    batch_id_path = Column(String(255))
    raw_data_path = Column(String(255))
    data_status = Column(Enum('valid', 'invalid', 'pending'), default='pending')
    process_status = Column(Enum('yes', 'no'), default='no')
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False)
    sequencing = relationship("Sequencing", back_populates="sequence_run")
    process_data = relationship("ProcessData", back_populates="sequence_run", uselist=False)
    used_in_processes = relationship("ProcessedDataDependency", back_populates="sequence_run")
    __table_args__ = {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}

class ProcessData(Base):
    __tablename__ = 'process_data'
    process_id = Column(Integer, autoincrement=True, primary_key=True)
    sequence_id = Column(String(50), ForeignKey('sequence_run.sequence_id'), unique=True, nullable=False)
    process_status = Column(Enum('yes', 'no'), default='no')
    sequence_run = relationship("SequenceRun", back_populates="process_data")
    input_sequences = relationship("ProcessedDataDependency", back_populates="process_data", cascade="all, delete-orphan")
    analysis_inputs = relationship("AnalysisInput", back_populates="process_data")
    __table_args__ = {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}

class ProcessedDataDependency(Base):
    __tablename__ = 'processed_data_dependency'
    id = Column(Integer, autoincrement=True, primary_key=True)
    process_id = Column(Integer, ForeignKey('process_data.process_id'), nullable=False)
    sequence_id = Column(String(50), ForeignKey('sequence_run.sequence_id'), nullable=False)
    process_data = relationship("ProcessData", back_populates="input_sequences")
    sequence_run = relationship("SequenceRun", back_populates="used_in_processes")
    __table_args__ = (
        UniqueConstraint('process_id', 'sequence_id', name='uix_process_sequence'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class AnalysisInput(Base):
    __tablename__ = 'analysis_inputs'
    input_id = Column(String(50), primary_key=True)
    process_id = Column(Integer, ForeignKey('process_data.process_id'), nullable=False)
    sample_id = Column(String(50), ForeignKey('sample.sample_id'), nullable=False)
    project_id = Column(String(50), ForeignKey('project.project_id'), nullable=False)
    project_type = Column(String(50))
    batch_id = Column(String(50), ForeignKey('batch.batch_id'), nullable=False)
    raw_data_path = Column(String(1024), nullable=True)
    parameters = Column(JSON, nullable=True)
    analysis_status = Column(Enum('yes', 'no'), default='no', nullable=False)
    retry_count = Column(Integer, default=0, nullable=False)  # 新增字段
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False)
    process_data = relationship("ProcessData", back_populates="analysis_inputs")
    sample = relationship("Sample", back_populates="analysis_inputs")
    project = relationship("Project", back_populates="analysis_inputs")
    batch = relationship("Batch", back_populates="analysis_inputs")
    analysis_task = relationship("AnalysisTask", back_populates="analysis_input", cascade="all, delete-orphan")
    __table_args__ = (
        Index('idx_analysis_status', 'analysis_status'),
        Index('idx_project_id', 'project_id'),
        Index('idx_sample_id', 'sample_id'),
        Index('idx_batch_id', 'batch_id'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class AnalysisTask(Base):
    __tablename__ = 'analysis_tasks'
    task_id = Column(String(50), primary_key=True)
    sample_id = Column(String(50), ForeignKey('sample.sample_id'), nullable=False)
    project_id = Column(String(50), ForeignKey('project.project_id'), nullable=False)
    input_id = Column(String(50), ForeignKey('analysis_inputs.input_id'), nullable=False)
    project_type = Column(String(50), nullable=True)
    analysis_path = Column(String(255), nullable=True)
    analysis_status = Column(Enum('pending', 'running', 'completed', 'failed'), default='pending', nullable=False)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    delivery_time = Column(DateTime, nullable=True)
    remark = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False)
    analysis_input = relationship("AnalysisInput", back_populates="analysis_task")
    sample = relationship("Sample")
    project = relationship("Project")
    __table_args__ = (
        Index('idx_analysis_status', 'analysis_status'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class InputFileMetadata(Base):
    __tablename__ = 'input_file_metadata'
    file_name = Column(String(255), primary_key=True, nullable=False)
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    __table_args__ = {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}

class FieldCorrection(Base):
    __tablename__ = 'field_corrections'
    correction_id = Column(String(50), primary_key=True)
    table_name = Column(String(50), nullable=False)
    record_id = Column(String(50), nullable=False)
    field_name = Column(String(50), nullable=False)
    old_value = Column(Text, nullable=True)
    new_value = Column(Text, nullable=True)
    operator = Column(String(50), nullable=False)
    notes = Column(Text, nullable=True)
    correction_time = Column(DateTime, default=func.current_timestamp(), nullable=False)
    __table_args__ = (
        Index('idx_table_record', 'table_name', 'record_id'),
        Index('idx_operator', 'operator'),
        Index('idx_correction_time', 'correction_time'),
        Index('idx_field_change', 'table_name', 'field_name', 'record_id'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class BatchProcessRecord(Base):
    __tablename__ = 'batch_process_record'
    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(50), ForeignKey('batch.batch_id'), nullable=False)
    batch_path = Column(String(255), default='-', nullable=False)
    operation_type = Column(Enum('create', 'move', 'backup', 'delete', 'restore', 'archive'), default='create', nullable=False)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    batch = relationship("Batch", back_populates="process_records")
    __table_args__ = (
        Index('idx_batch_id', 'batch_id'),
        Index('idx_operation_type', 'operation_type'),
        Index('idx_created_at', 'created_at'),
        Index('idx_batch_op_time', 'batch_id', 'operation_type', 'created_at'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class SampleAnalysisPathRecord(Base):
    __tablename__ = 'sample_analysis_path_record'
    id = Column(Integer, primary_key=True, autoincrement=True)
    sample_id = Column(String(50), ForeignKey('sample.sample_id'), nullable=False)
    analysis_path = Column(String(255), default='-', nullable=False)
    operation_type = Column(Enum('create', 'reanalysis', 'move', 'backup', 'delete', 'restore', 'archive'), default='create', nullable=False)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    sample = relationship("Sample", back_populates="analysis_path_records")
    __table_args__ = (
        Index('idx_sample_id', 'sample_id'),
        Index('idx_operation_type', 'operation_type'),
        Index('idx_created_at', 'created_at'),
        Index('idx_sample_op_time', 'sample_id', 'operation_type', 'created_at'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )