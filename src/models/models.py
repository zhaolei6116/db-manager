# src/models/models.py
"""
Database models for LIMS data management and analysis task generation.
Updated schema to support high-frequency retesting and analysis task generation.
"""

from sqlalchemy import (
    Column, String, Integer, Float, DateTime, Enum, Text, JSON, 
    ForeignKey, Index, UniqueConstraint
)
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class Project(Base):
    """存储订单信息"""
    __tablename__ = 'project'
    
    project_id = Column(String(50), primary_key=True, comment="JSON: Client (e.g., SD250726162017)")
    custom_name = Column(String(100), comment="JSON: Custom_name (e.g., 有康生物)")
    user_name = Column(String(50), comment="JSON: user_name (e.g., 有康)")
    mobile = Column(String(20), comment="JSON: Mobile (e.g., 13385717187)")
    remarks = Column(String(255), comment="JSON: Remarks")
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False)
    
    # Relationships
    samples = relationship("Sample", back_populates="project")
    sequences = relationship("Sequence", back_populates="project")
    analysis_tasks = relationship("AnalysisTask", back_populates="project")
    
    __table_args__ = (
        Index('idx_project_id', 'project_id'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class Sample(Base):
    """存储样本基本信息"""
    __tablename__ = 'sample'
    
    sample_id = Column(String(50), primary_key=True, comment="JSON: Detect_no (e.g., T22507265020)")
    project_id = Column(String(50), ForeignKey('project.project_id'), comment="关联 project.project_id")
    sample_name = Column(String(100), comment="JSON: Sample_name (e.g., GH-2-16S)")
    sample_type = Column(String(50), comment="JSON: Sample_type (e.g., dna)")
    sample_type_raw = Column(String(50), comment="JSON: Sample_type_raw (e.g., 菌体)")
    resistance_type = Column(String(50), comment="JSON: Resistance_type")
    species_name = Column(String(100), comment="JSON: Species_name")
    genome_size = Column(String(50), comment="JSON: Genome_size")
    data_volume = Column(String(50), comment="JSON: Data_volume")
    ref = Column(Text, comment="JSON: Ref")
    plasmid_length = Column(Integer, comment="JSON: PLASMID_LENGTH")
    length = Column(Integer, comment="JSON: Length")
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False)
    
    # Relationships
    project = relationship("Project", back_populates="samples")
    sequences = relationship("Sequence", back_populates="sample")
    
    __table_args__ = (
        Index('idx_project_sample', 'project_id', 'sample_id'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class Batch(Base):
    """存储批次信息"""
    __tablename__ = 'batch'
    
    batch_id = Column(String(50), primary_key=True, comment="JSON: Batch_id (e.g., 25072909)")
    sequencer_id = Column(String(50), comment="JSON: Sequencer_id (e.g., 06)")
    laboratory = Column(String(10), comment="JSON: Laboratory (e.g., T)")
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False)
    
    # Relationships
    sequences = relationship("Sequence", back_populates="batch")
    
    __table_args__ = (
        Index('idx_batch_id', 'batch_id'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class Sequence(Base):
    """存储测序信息（含 project_type 和 project_id）"""
    __tablename__ = 'sequence'
    
    sequence_id = Column(String(50), primary_key=True, comment="自动生成 (e.g., Seq_{uuid})")
    sample_id = Column(String(50), ForeignKey('sample.sample_id'), comment="关联 sample.sample_id")
    project_id = Column(String(50), ForeignKey('project.project_id'), comment="关联 project.project_id")
    batch_id = Column(String(50), ForeignKey('batch.batch_id'), comment="关联 batch.batch_id")
    project_type = Column(String(50), comment="JSON: Project (e.g., 细菌鉴定(16S))")
    board = Column(String(50), comment="JSON: Board")
    board_id = Column(String(50), comment="JSON: Board_id")
    machine_ver = Column(String(20), comment="JSON: Machine_ver")
    barcode_type = Column(String(50), comment="JSON: Barcode_type")
    barcode_prefix = Column(String(50), comment="JSON: Barcode_prefix")
    barcode_number = Column(String(50), comment="JSON: Barcode_number")
    barcode = Column(String(50), comment="组合: Barcode_prefix + Barcode_number")
    reanalysis_times = Column(Integer, comment="JSON: Reanalysis_times")
    experiment_times = Column(Integer, comment="JSON: Experiment_times")
    allanalysis_times = Column(Integer, comment="JSON: Allanalysis_times")
    experiment_no = Column(String(50), comment="JSON: Experiment_no")
    sample_con = Column(Float, comment="JSON: Sample_con")
    sample_status = Column(String(50), comment="JSON: Sample_status")
    unqualifytime = Column(DateTime, comment="JSON: Unqualifytime")
    report_path = Column(String(255), comment="JSON: Report_path")
    report_raw_path = Column(String(255), comment="JSON: Report_raw_path")
    lab_sequencer_id = Column(String(50), comment="组合: Laboratory + Sequencer_id")
    batch_id_path = Column(String(255), comment="模板生成: /{path}/{lab_sequencer_id}/{batch_id}")
    raw_data_path = Column(String(255), comment="模板生成: /{batch_id_path}/{barcode}")
    data_status = Column(Enum('valid', 'invalid', 'pending'), default='pending', comment="数据验证状态")
    process_status = Column(Enum('yes', 'no'), default='no', comment="处理状态")
    parameters = Column(JSON, default=None, comment="基于 config.yaml 和 project_type")
    analysis_status = Column(Enum('yes', 'no'), default='no', comment="分析状态")
    version = Column(Integer, default=1, comment="补测版本号")
    run_type = Column(Enum('initial', 'supplement', 'retest'), default='initial', comment="测序类型")
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False)
    
    # Relationships
    sample = relationship("Sample", back_populates="sequences")
    project = relationship("Project", back_populates="sequences")
    batch = relationship("Batch", back_populates="sequences")
    
    __table_args__ = (
        UniqueConstraint('sample_id', 'batch_id', 'project_type', 'barcode', name='uix_sequence'),
        Index('idx_sequence_filter', 'project_id', 'project_type', 'data_status', 'analysis_status'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class AnalysisTask(Base):
    """存储分析任务（按 project_id + project_type 分组）"""
    __tablename__ = 'analysis_tasks'
    
    task_id = Column(String(50), primary_key=True, comment="自动生成: project_id_project_type_retry_count")
    project_id = Column(String(50), ForeignKey('project.project_id'), comment="关联 project.project_id")
    project_type = Column(String(50), comment="JSON: Project (e.g., 细菌鉴定(16S))")
    sample_ids = Column(JSON, comment="GROUP_CONCAT(sequence.sample_id)")
    analysis_path = Column(String(255), comment="模板生成: /path/to/{project_id}/{project_type}")
    analysis_status = Column(Enum('pending', 'running', 'completed', 'failed'), default='pending')
    retry_count = Column(Integer, default=0, comment="重分析计数")
    parameters = Column(JSON, comment="合并 sequence.parameters")
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    delivery_time = Column(DateTime)
    remark = Column(Text)
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False)
    
    # Relationships
    project = relationship("Project", back_populates="analysis_tasks")
    
    __table_args__ = (
        Index('idx_task_filter', 'project_id', 'project_type', 'analysis_status'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class InputFileMetadata(Base):
    """存储 JSON 文件处理状态"""
    __tablename__ = 'input_file_metadata'
    
    file_name = Column(String(255), primary_key=True, comment="JSON 文件名")
    process_status = Column(Enum('pending', 'success', 'failed'), default='pending')
    created_at = Column(DateTime, default=func.current_timestamp(), nullable=False)
    
    __table_args__ = {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}

class FieldCorrections(Base):
    """存储变更日志"""
    __tablename__ = 'field_corrections'
    
    correction_id = Column(String(50), primary_key=True, comment="自动生成 UUID")
    table_name = Column(String(50), nullable=False)
    record_id = Column(String(50), nullable=False)
    field_name = Column(String(50), nullable=False)
    old_value = Column(Text)
    new_value = Column(Text)
    operator = Column(String(50), nullable=False, default='system')
    operation_type = Column(Enum('update', 'create', 'reanalysis', 'move', 'backup', 'delete', 'restore', 'archive'), default='update')
    notes = Column(Text)
    correction_time = Column(DateTime, default=func.current_timestamp(), nullable=False)
    
    __table_args__ = (
        Index('idx_corrections', 'table_name', 'record_id'),
        Index('idx_operator', 'operator'),
        Index('idx_correction_time', 'correction_time'),
        Index('idx_field_change', 'table_name', 'field_name', 'record_id'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )