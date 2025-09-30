-- 1. project: 存储订单信息
CREATE TABLE project (
    project_id VARCHAR(50) PRIMARY KEY,         -- JSON: Client (e.g., SD250726162017)
    custom_name VARCHAR(100),                  -- JSON: Custom_name (e.g., 有康生物)
    user_name VARCHAR(50),                     -- JSON: user_name (e.g., 有康)
    mobile VARCHAR(20),                        -- JSON: Mobile (e.g., 13385717187)
    remarks VARCHAR(255),                      -- JSON: Remarks (e.g., "")
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_project_id (project_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. sample: 存储样本基本信息
CREATE TABLE sample (
    sample_id VARCHAR(50) PRIMARY KEY,         -- JSON: Detect_no (e.g., T22507265020)
    project_id VARCHAR(50),                    -- JSON: Client (关联 project.project_id)
    sample_name VARCHAR(100),                  -- JSON: Sample_name (e.g., GH-2-16S)
    sample_type VARCHAR(50),                   -- JSON: Sample_type (e.g., dna)
    sample_type_raw VARCHAR(50),               -- JSON: Sample_type_raw (e.g., 菌体)
    resistance_type VARCHAR(50),               -- JSON: Resistance_type (e.g., "")
    species_name VARCHAR(100),                 -- JSON: Species_name (e.g., "-")
    genome_size VARCHAR(50),                   -- JSON: Genome_size (e.g., "-")
    data_volume VARCHAR(50),                   -- JSON: Data_volume (e.g., "-")
    ref LONGTEXT,                             -- JSON: Ref (e.g., "N")
    plasmid_length INT,                        -- JSON: PLASMID_LENGTH (e.g., 0)
    length INT,                                -- JSON: Length (e.g., 0)
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (project_id) REFERENCES project(project_id),
    INDEX idx_project_sample (project_id, sample_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. batch: 存储批次信息（高重复率，几十到几百样本）
CREATE TABLE batch (
    batch_id VARCHAR(50) PRIMARY KEY,          -- JSON: Batch_id (e.g., 25072909)
    sequencer_id VARCHAR(50),                  -- JSON: Sequencer_id (e.g., 06)
    laboratory VARCHAR(10),                    -- JSON: Laboratory (e.g., T)
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_batch_id (batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. sequence: 存储测序信息（含 project_type 和 project_id）
CREATE TABLE sequence (
    sequence_id VARCHAR(50) PRIMARY KEY,       -- 自动生成 (e.g., RUN_{uuid})
    sample_id VARCHAR(50),                     -- JSON: Detect_no (关联 sample.sample_id)
    project_id VARCHAR(50),                    -- JSON: Client (关联 project.project_id)
    batch_id VARCHAR(50),                      -- JSON: Batch_id (关联 batch.batch_id)
    project_type VARCHAR(50),                  -- JSON: Project (e.g., 细菌鉴定(16S))
    board VARCHAR(50),                         -- JSON: Board (e.g., T250729004)
    board_id VARCHAR(50),                      -- JSON: Board_id (e.g., H2)
    machine_ver VARCHAR(20),                   -- JSON: Machine_ver (e.g., V3)
    barcode_type VARCHAR(50),                  -- JSON: Barcode_type (e.g., CW-Bar16bp-2208-1)
    barcode_prefix VARCHAR(50),                -- JSON: Barcode_prefix (e.g., barcode)
    barcode_number VARCHAR(50),                -- JSON: Barcode_number (e.g., 0688)
    barcode VARCHAR(50),                       -- 组合: Barcode_prefix + Barcode_number (e.g., barcode0688)
    reanalysis_times INT,                      -- JSON: Reanalysis_times (e.g., 0)
    experiment_times INT,                      -- JSON: Experiment_times (e.g., 1)
    allanalysis_times INT,                     -- JSON: Allanalysis_times (e.g., 0)
    experiment_no VARCHAR(50),                 -- JSON: Experiment_no (e.g., "")
    sample_con FLOAT,                          -- JSON: Sample_con (e.g., 378)
    sample_status VARCHAR(50),                 -- JSON: Sample_status (e.g., 合格)
    unqualifytime DATETIME,                    -- JSON: Unqualifytime (e.g., 1970-01-01 08:00:00)
    report_path VARCHAR(255),                  -- JSON: Report_path (e.g., /kfservice/t/primecx/25072909/T22507265020.zip)
    report_raw_path VARCHAR(255),              -- JSON: Report_raw_path (e.g., /kfservice/t/primecx/25072909/T22507265020_rawdata.zip)
    lab_sequencer_id VARCHAR(50),             -- 组合: Laboratory + Sequencer_id (e.g., T06)
    batch_id_path VARCHAR(255),                -- 模板生成: /{path}/{lab_sequencer_id}/{batch_id}
    raw_data_path VARCHAR(255),                -- 模板生成: /{batch_id_path}/{barcode}
    data_status ENUM('valid', 'invalid', 'pending') DEFAULT 'pending', -- 逻辑: Sample_status="合格" → 'valid'
    process_status ENUM('yes', 'no') DEFAULT 'no', -- 默认 'no'，处理后更新
    parameters JSON,                           -- 基于 config.yaml 和 project_type (e.g., {"genome_size": "-"})
    analysis_status ENUM('yes', 'no') DEFAULT 'no', -- 默认 'no'，分析后更新
    version INT DEFAULT 1,                     -- 默认 1，补测时 MAX(version)+1
    run_type ENUM('initial', 'supplement', 'retest') DEFAULT 'initial', -- 检查 UNIQUE(sample_id, batch_id, project_type, barcode)
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (sample_id) REFERENCES sample(sample_id),
    FOREIGN KEY (project_id) REFERENCES project(project_id),
    FOREIGN KEY (batch_id) REFERENCES batch(batch_id),
    UNIQUE KEY uix_sequence (sample_id, batch_id, project_type, barcode),
    INDEX idx_sequence_filter (project_id, project_type, data_status, analysis_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5. analysis_tasks: 存储分析任务（按 project_id + project_type 分组）
CREATE TABLE analysis_tasks (
    task_id VARCHAR(50) PRIMARY KEY,           -- 自动生成: CONCAT(project_id, '_', project_type, '_', retry_count)
    project_id VARCHAR(50),                    -- JSON: Client (关联 project.project_id)
    project_type VARCHAR(50),                  -- JSON: Project (e.g., 细菌鉴定(16S))
    sample_ids JSON,                           -- GROUP_CONCAT(sequence.sample_id)
    analysis_path VARCHAR(255),                -- 模板生成: /path/to/{project_id}/{project_type}
    analysis_status ENUM('pending', 'running', 'completed', 'failed') DEFAULT 'pending',
    retry_count INT DEFAULT 0,                 -- 默认 0，重分析时 ++
    parameters JSON,                           -- 合并 sequence.parameters
    start_time DATETIME,
    end_time DATETIME,
    delivery_time DATETIME,
    remark TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (project_id) REFERENCES project(project_id),
    INDEX idx_task_filter (project_id, project_type, analysis_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 6. input_file_metadata: 存储 JSON 文件日志
CREATE TABLE input_file_metadata (
    file_name VARCHAR(255) PRIMARY KEY,        -- JSON 文件名 (e.g., T22507265020.json)
    process_status ENUM('pending', 'success', 'failed') DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 7. field_corrections: 存储变更日志
CREATE TABLE field_corrections (
    correction_id VARCHAR(50) PRIMARY KEY,     -- 自动生成 (e.g., UUID)
    table_name VARCHAR(50),                    -- 变更表名 (e.g., sequence)
    record_id VARCHAR(50),                     -- 变更记录ID (e.g., SEQ001)
    field_name VARCHAR(50),                    -- 变更字段 (e.g., data_status)
    old_value TEXT,                            -- 旧值 (e.g., valid)
    new_value TEXT,                            -- 新值 (e.g., invalid)
    operator VARCHAR(50),                      -- 操作者
    operation_type ENUM('update', 'create', 'reanalysis', 'move', 'backup', 'delete', 'restore', 'archive'),
    notes TEXT,                                -- 备注
    correction_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_corrections (table_name, record_id),
    INDEX idx_operator (operator),
    INDEX idx_correction_time (correction_time),
    INDEX idx_field_change (table_name, field_name, record_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
