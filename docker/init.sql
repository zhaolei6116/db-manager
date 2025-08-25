-- 创建 project 表：存储项目信息
CREATE TABLE project (
    project_id VARCHAR(50) PRIMARY KEY,   -- Client字段
    custom_name VARCHAR(100),              -- Custom_name
    user_name VARCHAR(50),                -- user_name
    mobile VARCHAR(20),                   -- Mobile
    remarks VARCHAR(255),                         -- Remarks
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,        -- 记录生成时间，自动生成时间戳
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP   -- 记录更新时间，自动生成时间戳
);

-- 创建 sample 表：存储样本信息
CREATE TABLE sample (
    sample_id VARCHAR(50) PRIMARY KEY,                    -- Detect_no字段，作为样本唯一标识
    project_id VARCHAR(50),                               -- 外键，关联project表
    sample_name VARCHAR(100),                             -- Sample_name字段，样本名称
    sample_type VARCHAR(50),                              -- Sample_type字段，样本类型
    sample_type_raw VARCHAR(50),                          -- Sample_type_raw字段，原始样本类型
    resistance_type VARCHAR(50),                          -- Resistance_type字段，抗性类型
    project_type VARCHAR(100),                                 -- Project字段，项目类型
    species_name VARCHAR(100),                            -- Species_name字段，物种名称
    genome_size VARCHAR(50),                              -- Genome_size字段，基因组大小
    data_volume VARCHAR(50),                              -- Data_volume字段，数据量
    ref LONGTEXT,                                             -- Ref字段，参考序列
    plasmid_length INT,                                   -- PLASMID_LENGTH字段，质粒长度
    length INT,                                           -- 片段 Length字段，长度
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,        -- 记录生成时间，自动生成时间戳
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- 记录更新时间，自动生成时间戳
    FOREIGN KEY (project_id) REFERENCES project(project_id)
);

-- 创建 batch 表：存储 batch_ID 和对应路径
CREATE TABLE batch (
    batch_id VARCHAR(50)  PRIMARY KEY,                    -- Batch_id字段，批次ID， 作为批次唯一标识
    sequencer_id VARCHAR(50),                             -- Sequencer_id字段，测序仪ID
    laboratory VARCHAR(10),                               -- Laboratory字段，实验室
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,        -- 记录生成时间，自动生成时间戳
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- 记录更新时间，自动生成时间戳
);

-- 创建 sequence 表：存储测序信息
CREATE TABLE sequence (
    sequence_id VARCHAR(50) PRIMARY KEY,                       -- 自动生成，如RUN_{uuid}
    sample_id VARCHAR(50),                                -- 外键，关联sample表
    batch_id VARCHAR(50),                                 -- 外键，关联batch表
    board VARCHAR(50),                                    -- Board字段，板号
    board_id VARCHAR(50),                                 -- Board_id字段，板ID   
    machine_ver VARCHAR(20),                              -- Machine_ver字段，机器版本
    barcode_type VARCHAR(50),                             -- Barcode_type字段，条码类型
    barcode_prefix VARCHAR(50),                           -- Barcode_prefix字段，条码前缀
    barcode_number VARCHAR(50),                           -- Barcode_number字段，条码编号   
    reanalysis_times INT,                                 -- Reanalysis_times字段，重分析次数
    experiment_times INT,                                 -- Experiment_times字段，实验次数
    allanalysis_times INT,                                -- Allanalysis_times字段，所有分析次数
    experiment_no VARCHAR(50),                            -- Experiment_no字段，实验号
    sample_con FLOAT,                                     -- Sample_con字段，样本浓度
    sample_status VARCHAR(50),                            -- Sample_status字段，样本状态
    unqualifytime DATETIME,                               -- Unqualifytime字段，不合格时间
    report_path VARCHAR(255),                             -- Report_path字段，报告路径
    report_raw_path VARCHAR(255),                         -- Report_raw_path字段，原始报告路径
    version INT DEFAULT 1,                                -- 字段修正版本号
    run_type ENUM('initial', 'supplement', 'retest') DEFAULT 'initial', -- 测序类型
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,        -- 记录生成时间，自动生成时间戳
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- 记录更新时间，自动生成时间戳
    FOREIGN KEY (sample_id) REFERENCES sample(sample_id),
    FOREIGN KEY (batch_id) REFERENCES batch(batch_id),
    UNIQUE (sample_id, batch_id)
);

-- 创建 sequence_run 表
CREATE TABLE sequence_run (
    sequence_id VARCHAR(50) PRIMARY KEY,                  -- 与sequence表中的 sequence_id 一一对应
    sample_id VARCHAR(50),                                -- 外键，关联sample表
    lab_sequencer_id VARCHAR(50),                         -- sequence表 laboratory 与 sequencer_id 信息组合
    barcode VARCHAR(50),                                  -- sequence表 barcode_prefix 与 barcode_number 信息组合
    batch_id_path VARCHAR(255),                           -- /{path}/{lab_sequencer_id}/{batch_id}/ path为外接config设置
    raw_data_path VARCHAR(255),                           -- 原始数据路径，由模板生成
    data_status ENUM('valid', 'invalid', 'pending') DEFAULT 'pending', -- 数据状态
    process_status ENUM('yes', 'no') DEFAULT 'no',        -- 样本是否进入处理
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,        -- 记录生成时间，自动生成时间戳
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- 记录更新时间，自动生成时间戳
    FOREIGN KEY (sequence_id) REFERENCES sequence(sequence_id)
);

-- 创建 process_data 表
CREATE TABLE process_data (
    process_id INT AUTO_INCREMENT PRIMARY KEY,
    sequence_id VARCHAR(50) ,                          -- 外键，关联sequence_run表
    process_status ENUM('yes', 'no') DEFAULT 'no',
    UNIQUE (process_id, sequence_id)
);

-- 创建 processed_data_run 表（关联表）多对多表
CREATE TABLE processed_data_run (
    id INT AUTO_INCREMENT PRIMARY KEY,
    process_id INT,                                       -- 外键，关联 process_data 表
    sequence_id VARCHAR(50),                              -- 外键，关联 sequence_run 表
    UNIQUE (process_id, sequence_id),
    FOREIGN KEY (process_id) REFERENCES process_data(process_id),
    FOREIGN KEY (sequence_id) REFERENCES sequence_run(sequence_id)
);

-- 创建 analysis_inputs 表
CREATE TABLE analysis_inputs (
    input_id VARCHAR(50) PRIMARY KEY,                     -- INPUT_{uuid}，熟数据唯一标识
    process_id INT,                                       -- 外键，关联process_data 表
    sample_id VARCHAR(50),                                -- 外键，关联sample表
    project_id VARCHAR(50),                               -- 项目编号，关联project表
    project_type VARCHAR(50),                             -- 项目类型
    batch_id VARCHAR(50),                                 -- 批次号
    raw_data_path VARCHAR(1024),                           -- 熟数据路径 通过 processed_data_run表获取
    parameters JSON,                                      -- 生成参数。根据不同的项目类型获取相关的字段信息，生成json
    analysis_status ENUM('yes', 'no') DEFAULT 'no',       -- 是否分析，没有分析的，会再启动分析，默认是no 未分析，支持手动修改
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,        -- 记录生成时间，自动生成时间戳
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- 记录更新时间，自动生成时间戳
    FOREIGN KEY (process_id) REFERENCES process_data(process_id),
    FOREIGN KEY (sample_id) REFERENCES sample(sample_id),
    FOREIGN KEY (project_id) REFERENCES project(project_id),
    FOREIGN KEY (batch_id) REFERENCES batch(batch_id),
    INDEX(analysis_status),
    INDEX(project_id),
    INDEX(sample_id),
    INDEX(batch_id)
);

-- 创建 analysis_tasks 表 存储分析任务信息
CREATE TABLE analysis_tasks (
    task_id VARCHAR(50) PRIMARY KEY,                      -- TASK_{uuid}，分析任务唯一标识
    sample_id VARCHAR(50),                                -- 外键，关联sample表
    project_id VARCHAR(50),                               -- 项目编号，关联project表
    input_id VARCHAR(50),                                 -- 外键，关联analysis_inputs表
    project_type VARCHAR(50),                             -- 项目类型
    analysis_path VARCHAR(255),                           -- 分析路径
    analysis_status ENUM('pending', 'running', 'completed', 'failed') DEFAULT 'pending', -- 分析状态
    start_time DATETIME,                                  -- 开始时间
    end_time DATETIME,                                    -- 结束时间
    delivery_time DATETIME,                               -- 交付时间
    remark TEXT,                                          -- 分析状态的备注，如失败原因，等
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,        -- 记录生成时间，自动生成时间戳
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- 记录更新时间，自动生成时间戳
    FOREIGN KEY (sample_id) REFERENCES sample(sample_id),
    FOREIGN KEY (input_id) REFERENCES analysis_inputs(input_id),
    FOREIGN KEY (project_id) REFERENCES project(project_id),
    INDEX(analysis_status)
    
);

-- 创建 input_file_metadata 表（辅助表）
CREATE TABLE input_file_metadata (
    file_name VARCHAR(255) PRIMARY KEY,                   -- JSON文件名，如T22507265020.json
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP         -- 记录生成时间，自动生成时间戳
);

-- 创建 field_corrections 表 存储字段修正记录
CREATE TABLE field_corrections (
    correction_id VARCHAR(50) PRIMARY KEY,                -- CORR_{uuid}，修正记录唯一标识
    table_name VARCHAR(50),                               -- 修正的表名，如 project、sample、sequencing_runs
    record_id VARCHAR(50),                                -- 修正记录的ID，如 project_id、sample_id、run_id
    field_name VARCHAR(50),                               -- 修正字段名，如 custom_name、sample_con
    old_value TEXT,                                       -- 旧值
    new_value TEXT,                                       -- 新值
    operator VARCHAR(50),                                 -- 操作人
    notes TEXT,                                           -- 备注
    correction_time DATETIME DEFAULT CURRENT_TIMESTAMP    -- 修正时间
);

-- 创建 batch_process_record 表 批次号对应的路径记录
CREATE TABLE batch_process_record (
    id INT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(50),                                 -- 外键，关联batch表
    batch_path VARCHAR(255),                              -- 默认是"-"
    notes TEXT,                                           -- 备注
    correction_time DATETIME DEFAULT CURRENT_TIMESTAMP,   -- 记录生成时间
    FOREIGN KEY (batch_id) REFERENCES batch(batch_id)
);

-- 创建 sample_analysis_path_record 表
CREATE TABLE sample_analysis_path_record (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sample_id VARCHAR(50),                                -- 外键，关联sample表
    analysis_path VARCHAR(255),                           -- 默认是"-"
    notes TEXT,                                           -- 备注
    correction_time DATETIME DEFAULT CURRENT_TIMESTAMP,   -- 记录生成时间
    FOREIGN KEY (sample_id) REFERENCES sample(sample_id),
    UNIQUE (sample_id, correction_time)
);
