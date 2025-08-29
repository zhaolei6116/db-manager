from sqlalchemy import (
    Column, String, Integer, Float, DateTime, Enum, Text, JSON, 
    ForeignKey, text, Index, UniqueConstraint
)
from sqlalchemy.orm import relationship
from core.database import Base

class Project(Base):
    __tablename__ = 'project'
    project_id = Column(String(50), primary_key=True)
    custom_name = Column(String(100))
    user_name = Column(String(50))
    mobile = Column(String(20))
    remarks = Column(String(255))
    created_at = Column(DateTime, default=text('CURRENT_TIMESTAMP'), nullable=False)
    updated_at = Column(DateTime, default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'), nullable=False)
    samples = relationship("Sample", back_populates="project")
    analysis_inputs = relationship("AnalysisInput", back_populates="project")

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
    created_at = Column(DateTime, default=text('CURRENT_TIMESTAMP'), nullable=False)
    updated_at = Column(DateTime, default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'), nullable=False)
    project = relationship("Project", back_populates="samples")
    sequences = relationship("Sequencing", back_populates="sample")
    analysis_inputs = relationship("AnalysisInput", back_populates="sample")
    analysis_path_records = relationship("SampleAnalysisPathRecord", back_populates="sample", cascade="all, delete-orphan")

class Batch(Base):
    __tablename__ = 'batch'
    batch_id = Column(String(50), primary_key=True)
    sequencer_id = Column(String(50))
    laboratory = Column(String(10))
    created_at = Column(DateTime, default=text('CURRENT_TIMESTAMP'), nullable=False)
    updated_at = Column(DateTime, default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'), nullable=False)
    sequences = relationship("Sequencing", back_populates="batch")
    analysis_inputs = relationship("AnalysisInput", back_populates="batch")
    process_records = relationship("BatchProcessRecord", back_populates="batch", cascade="all, delete-orphan")

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
    created_at = Column(DateTime, default=text('CURRENT_TIMESTAMP'), nullable=False)
    updated_at = Column(DateTime, default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'), nullable=False)
    sample = relationship("Sample", back_populates="sequences")
    batch = relationship("Batch", back_populates="sequences")
    sequence_run = relationship("SequenceRun", uselist=False, back_populates="sequencing", cascade="all, delete-orphan")
    __table_args__ = (
        UniqueConstraint('sample_id', 'batch_id', name='uix_sample_batch'),
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
    created_at = Column(DateTime, default=text('CURRENT_TIMESTAMP'), nullable=False)
    updated_at = Column(DateTime, default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'), nullable=False)
    sequencing = relationship("Sequencing", back_populates="sequence_run")
    process_data = relationship("ProcessData", back_populates="sequence_run", uselist=False)
    used_in_processes = relationship("ProcessedDataDependency", back_populates="sequence_run")

class ProcessData(Base):
    __tablename__ = 'process_data'
    process_id = Column(Integer, autoincrement=True, primary_key=True)
    sequence_id = Column(String(50), ForeignKey('sequence_run.sequence_id'), unique=True, nullable=False)
    process_status = Column(Enum('yes', 'no'), default='no')
    
    # 关系定义
    sequence_run = relationship("SequenceRun", back_populates="process_data")
    input_sequences = relationship("ProcessedDataDependency", back_populates="process_data", cascade="all, delete-orphan")
    analysis_inputs = relationship("AnalysisInput", back_populates="process_data")

    # 可选：直接访问关联的 sequence_run 列表（通过关联表）
    @property
    def all_input_runs(self):
        return [item.sequence_run for item in self.input_sequences]

class ProcessedDataDependency(Base):
    __tablename__ = 'processed_data_dependency'
    id = Column(Integer, autoincrement=True, primary_key=True)
    process_id = Column(Integer, ForeignKey('process_data.process_id'), nullable=False)
    sequence_id = Column(String(50), ForeignKey('sequence_run.sequence_id'), nullable=False)

    # 唯一约束：避免重复插入相同的 (process_id, sequence_id)
    __table_args__ = (
        UniqueConstraint('process_id', 'sequence_id', name='uix_process_sequence'),
        {'mysql_charset': 'utf8mb4'}
    )

    # 关系定义
    process_data = relationship("ProcessData", back_populates="input_sequences")
    sequence_run = relationship("SequenceRun", back_populates="used_in_processes")

class AnalysisInput(Base):
    __tablename__ = 'analysis_inputs'

    # 主键：INPUT_{uuid}，熟数据唯一标识
    input_id = Column(String(50), primary_key=True)

    # 外键字段
    process_id = Column(Integer, ForeignKey('process_data.process_id'), nullable=False)
    sample_id = Column(String(50), ForeignKey('sample.sample_id'), nullable=False)
    project_id = Column(String(50), ForeignKey('project.project_id'), nullable=False)
    project_type = Column(String(50))  # 可作为普通字段，也可关联枚举表
    batch_id = Column(String(50), ForeignKey('batch.batch_id'), nullable=False)

    # 熟数据路径（通过逻辑填充，非外键）
    raw_data_path = Column(String(1024), nullable=True)

    # 生成参数（JSON 格式）
    parameters = Column(JSON, nullable=True)

    # 分析状态
    analysis_status = Column(Enum('yes', 'no'), default='no', nullable=False)

    # 时间戳
    created_at = Column(
        DateTime,
        default=text('CURRENT_TIMESTAMP'),
        nullable=False
    )
    updated_at = Column(
        DateTime,
        default=text('CURRENT_TIMESTAMP'),
        onupdate=text('CURRENT_TIMESTAMP'),
        nullable=False
    )

    # 关系定义
    process_data = relationship("ProcessData", back_populates="analysis_inputs")
    sample = relationship("Sample", back_populates="analysis_inputs")
    project = relationship("Project", back_populates="analysis_inputs")
    batch = relationship("Batch", back_populates="analysis_inputs")
    analysis_task = relationship("AnalysisTask", back_populates="analysis_input", cascade="all, delete-orphan")

    # 索引定义
    __table_args__ = (
        Index('idx_analysis_status', 'analysis_status'),
        Index('idx_project_id', 'project_id'),
        Index('idx_sample_id', 'sample_id'),
        Index('idx_batch_id', 'batch_id'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class AnalysisTask(Base):
    __tablename__ = 'analysis_tasks'

    # 主键：TASK_{uuid}，分析任务唯一标识
    task_id = Column(String(50), primary_key=True)

    # 外键字段
    sample_id = Column(String(50), ForeignKey('sample.sample_id'), nullable=False)
    project_id = Column(String(50), ForeignKey('project.project_id'), nullable=False)
    input_id = Column(String(50), ForeignKey('analysis_inputs.input_id'), nullable=False)

    # 项目类型（可冗余存储，便于查询）
    project_type = Column(String(50), nullable=True)

    # 分析路径
    analysis_path = Column(String(255), nullable=True)

    # 分析状态
    analysis_status = Column(
        Enum('pending', 'running', 'completed', 'failed'),
        default='pending',
        nullable=False
    )

    # 时间字段
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    delivery_time = Column(DateTime, nullable=True)

    # 备注（如失败原因）
    remark = Column(Text, nullable=True)

    # 时间戳
    created_at = Column(
        DateTime,
        default=text('CURRENT_TIMESTAMP'),
        nullable=False
    )
    updated_at = Column(
        DateTime,
        default=text('CURRENT_TIMESTAMP'),
        onupdate=text('CURRENT_TIMESTAMP'),
        nullable=False
    )

    # 关系定义
    analysis_input = relationship("AnalysisInput", back_populates="analysis_task")
    sample = relationship("Sample")
    project = relationship("Project")

    # 索引
    __table_args__ = ( 
        Index('idx_analysis_status', 'analysis_status'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class InputFileMetadata(Base):
    __tablename__ = 'input_file_metadata'

    # 主键：JSON 文件名，如 T22507265020.json
    file_name = Column(String(255), primary_key=True, nullable=False)

    # 记录生成时间，自动填充
    created_at = Column(DateTime, default=text('CURRENT_TIMESTAMP'), nullable=False)

    __table_args__ = (
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class FieldCorrection(Base):
    __tablename__ = 'field_corrections'

    # 主键：CORR_{uuid}，修正记录唯一标识
    correction_id = Column(String(50), primary_key=True)

    # 被修改的元信息
    table_name = Column(String(50), nullable=False)        # 如 'project', 'sample', 'sequence_run'
    record_id = Column(String(50), nullable=False)         # 如 'P202508001', 'S001', 'RUN_001'
    field_name = Column(String(50), nullable=False)        # 字段名，如 'custom_name', 'sample_concentration'

    # 值变更
    old_value = Column(Text, nullable=True)                # 旧值（允许 NULL，如新增字段）
    new_value = Column(Text, nullable=True)                # 新值

    # 操作信息
    operator = Column(String(50), nullable=False)          # 操作人（用户名或系统账号）
    notes = Column(Text, nullable=True)                    # 备注，如“客户反馈姓名错误”

    # 时间
    correction_time = Column(DateTime, default=text('CURRENT_TIMESTAMP'), nullable=False)

    # 索引
    __table_args__ = (
        Index('idx_table_record', 'table_name', 'record_id'),
        Index('idx_operator', 'operator'),
        Index('idx_correction_time', 'correction_time'),
        Index('idx_field_change', 'table_name', 'field_name', 'record_id'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class BatchProcessRecord(Base):
    __tablename__ = 'batch_process_record'

    # 自增主键
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 关联批次
    batch_id = Column(String(50), ForeignKey('batch.batch_id'), nullable=False)

    # 当前/目标路径
    batch_path = Column(String(255), default='-', nullable=False)

    # 操作类型
    operation_type = Column(
        Enum('create', 'move', 'backup', 'delete', 'restore', 'archive'),
        default='create',
        nullable=False
    )

    # 备注
    notes = Column(Text, nullable=True)

    # 操作时间
    created_at = Column(
        DateTime,
        default=text('CURRENT_TIMESTAMP'),
        nullable=False
    )

    # 关系
    batch = relationship("Batch", back_populates="process_records")

    # 索引
    __table_args__ = (
        Index('idx_batch_id', 'batch_id'),
        Index('idx_operation_type', 'operation_type'),
        Index('idx_created_at', 'created_at'),
        Index('idx_batch_op_time', 'batch_id', 'operation_type', 'created_at'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )

class SampleAnalysisPathRecord(Base):
    __tablename__ = 'sample_analysis_path_record'

    # 自增主键
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 外键：关联 sample 表
    sample_id = Column(String(50), ForeignKey('sample.sample_id'), nullable=False)

    # 分析路径
    analysis_path = Column(String(255), default='-', nullable=False)

    # 操作类型
    operation_type = Column(
        Enum('create', 'reanalysis', 'move', 'backup', 'delete', 'restore', 'archive'),
        default='create',
        nullable=False
    )

    # 备注
    notes = Column(Text, nullable=True)

    # 记录生成时间
    created_at = Column(
        DateTime,
        default=text('CURRENT_TIMESTAMP'),
        nullable=False
    )

    # 关系：反向关联 sample
    sample = relationship("Sample", back_populates="analysis_path_records")

    # 索引
    __table_args__ = (
        Index('idx_sample_id', 'sample_id'),
        Index('idx_operation_type', 'operation_type'),
        Index('idx_created_at', 'created_at'),
        Index('idx_sample_op_time', 'sample_id', 'operation_type', 'created_at'),
        {'mysql_charset': 'utf8mb4', 'mysql_engine': 'InnoDB'}
    )
