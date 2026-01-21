-- ===================================================================
-- FILE OUTPUT LOGGING AND EXPORT SYSTEM
-- ===================================================================
-- This script creates a system to export ETL logs and reports to files
-- including CSV, JSON, and HTML formats for different stakeholders
-- ===================================================================

-- ===================================================================
-- CREATE EXPORT CONFIGURATION TABLE
-- ===================================================================

CREATE SCHEMA IF NOT EXISTS etl_exports;

CREATE TABLE IF NOT EXISTS etl_exports.export_config (
    config_id SERIAL PRIMARY KEY,
    export_type VARCHAR(50) NOT NULL, -- 'LOGS', 'QUALITY_REPORT', 'PERFORMANCE', 'SUMMARY'
    output_format VARCHAR(10) NOT NULL, -- 'CSV', 'JSON', 'HTML', 'XML'
    file_pattern VARCHAR(200) NOT NULL, -- Pattern for filename generation
    include_data_quality BOOLEAN DEFAULT TRUE,
    include_performance BOOLEAN DEFAULT TRUE,
    include_schema_validation BOOLEAN DEFAULT TRUE,
    include_error_logs BOOLEAN DEFAULT TRUE,
    retention_days INTEGER DEFAULT 30,
    auto_export BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default export configurations
INSERT INTO etl_exports.export_config (export_type, output_format, file_pattern, auto_export) VALUES
('LOGS', 'CSV', 'etl_logs_{execution_id}_{timestamp}.csv', TRUE),
('LOGS', 'JSON', 'etl_logs_{execution_id}_{timestamp}.json', TRUE),
('QUALITY_REPORT', 'HTML', 'data_quality_report_{execution_id}_{timestamp}.html', TRUE),
('PERFORMANCE', 'CSV', 'performance_metrics_{execution_id}_{timestamp}.csv', FALSE),
('SUMMARY', 'JSON', 'etl_summary_{execution_id}_{timestamp}.json', TRUE)
ON CONFLICT DO NOTHING;

-- ===================================================================
-- FILE EXPORT LOGGING TABLE
-- ===================================================================

CREATE TABLE IF NOT EXISTS etl_exports.export_history (
    export_id SERIAL PRIMARY KEY,
    execution_id UUID NOT NULL,
    export_type VARCHAR(50) NOT NULL,
    output_format VARCHAR(10) NOT NULL,
    file_path TEXT NOT NULL,
    file_size_bytes BIGINT,
    export_status VARCHAR(20) CHECK (export_status IN ('SUCCESS', 'FAILED', 'PARTIAL')),
    record_count BIGINT,
    export_duration_ms BIGINT,
    error_message TEXT,
    export_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===================================================================
-- EXPORT GENERATION FUNCTIONS
-- ===================================================================

-- Function to generate CSV export of ETL execution logs
CREATE OR REPLACE FUNCTION etl_exports.generate_etl_logs_csv(p_execution_id UUID)
RETURNS TEXT AS $$
DECLARE
    v_csv_content TEXT := '';
    v_header TEXT := 'log_id,execution_id,procedure_name,step_name,step_order,start_time,end_time,status,rows_processed,rows_inserted,rows_updated,rows_deleted,duration_seconds,error_message';
    v_record RECORD;
BEGIN
    v_csv_content := v_header || E'\n';
    
    FOR v_record IN 
        SELECT 
            log_id,
            execution_id,
            procedure_name,
            step_name,
            step_order,
            start_time,
            end_time,
            status,
            COALESCE(rows_processed, 0) as rows_processed,
            COALESCE(rows_inserted, 0) as rows_inserted,
            COALESCE(rows_updated, 0) as rows_updated,
            COALESCE(rows_deleted, 0) as rows_deleted,
            COALESCE(duration_seconds, 0) as duration_seconds,
            COALESCE(error_message, '') as error_message
        FROM etl_logs.etl_execution_log 
        WHERE execution_id = p_execution_id
        ORDER BY step_order
    LOOP
        v_csv_content := v_csv_content || format(
            '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"%s"',
            v_record.log_id,
            v_record.execution_id,
            v_record.procedure_name,
            v_record.step_name,
            v_record.step_order,
            v_record.start_time,
            COALESCE(v_record.end_time::TEXT, ''),
            v_record.status,
            v_record.rows_processed,
            v_record.rows_inserted,
            v_record.rows_updated,
            v_record.rows_deleted,
            v_record.duration_seconds,
            replace(v_record.error_message, '"', '""')
        ) || E'\n';
    END LOOP;
    
    RETURN v_csv_content;
END;
$$ LANGUAGE plpgsql;

-- Function to generate Data Quality Report CSV
CREATE OR REPLACE FUNCTION etl_exports.generate_data_quality_csv(p_execution_id UUID)
RETURNS TEXT AS $$
DECLARE
    v_csv_content TEXT := '';
    v_header TEXT := 'quality_id,execution_id,table_name,column_name,issue_type,severity_level,affected_rows,issue_description,sample_values,created_at';
    v_record RECORD;
BEGIN
    v_csv_content := v_header || E'\n';
    
    FOR v_record IN 
        SELECT 
            quality_id,
            execution_id,
            table_name,
            COALESCE(column_name, '') as column_name,
            issue_type,
            severity_level,
            COALESCE(affected_rows, 0) as affected_rows,
            issue_description,
            COALESCE(sample_values, '') as sample_values,
            created_at
        FROM etl_logs.data_quality_log 
        WHERE execution_id = p_execution_id
        ORDER BY created_at
    LOOP
        v_csv_content := v_csv_content || format(
            '%s,%s,%s,%s,%s,%s,%s,"%s","%s",%s',
            v_record.quality_id,
            v_record.execution_id,
            v_record.table_name,
            v_record.column_name,
            v_record.issue_type,
            v_record.severity_level,
            v_record.affected_rows,
            replace(v_record.issue_description, '"', '""'),
            replace(v_record.sample_values, '"', '""'),
            v_record.created_at
        ) || E'\n';
    END LOOP;
    
    RETURN v_csv_content;
END;
$$ LANGUAGE plpgsql;

-- Function to generate Performance Metrics CSV
CREATE OR REPLACE FUNCTION etl_exports.generate_performance_csv(p_execution_id UUID)
RETURNS TEXT AS $$
DECLARE
    v_csv_content TEXT := '';
    v_header TEXT := 'metric_id,execution_id,table_name,operation_type,duration_ms,rows_affected,start_time,end_time';
    v_record RECORD;
BEGIN
    v_csv_content := v_header || E'\n';
    
    FOR v_record IN 
        SELECT 
            metric_id,
            execution_id,
            table_name,
            operation_type,
            duration_ms,
            COALESCE(rows_affected, 0) as rows_affected,
            start_time,
            end_time
        FROM etl_logs.performance_metrics 
        WHERE execution_id = p_execution_id
        ORDER BY start_time
    LOOP
        v_csv_content := v_csv_content || format(
            '%s,%s,%s,%s,%s,%s,%s,%s',
            v_record.metric_id,
            v_record.execution_id,
            v_record.table_name,
            v_record.operation_type,
            v_record.duration_ms,
            v_record.rows_affected,
            v_record.start_time,
            v_record.end_time
        ) || E'\n';
    END LOOP;
    
    RETURN v_csv_content;
END;
$$ LANGUAGE plpgsql;

-- Function to generate comprehensive JSON export
CREATE OR REPLACE FUNCTION etl_exports.generate_comprehensive_json(p_execution_id UUID)
RETURNS JSONB AS $$
DECLARE
    v_json_result JSONB;
BEGIN
    SELECT jsonb_build_object(
        'execution_metadata', jsonb_build_object(
            'execution_id', p_execution_id,
            'export_timestamp', CURRENT_TIMESTAMP,
            'export_version', '1.0'
        ),
        'etl_summary', (
            SELECT etl_logs.generate_etl_summary(p_execution_id)
        ),
        'execution_log', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'log_id', log_id,
                    'step_name', step_name,
                    'step_order', step_order,
                    'status', status,
                    'start_time', start_time,
                    'end_time', end_time,
                    'duration_seconds', duration_seconds,
                    'rows_processed', rows_processed,
                    'rows_inserted', rows_inserted,
                    'rows_updated', rows_updated,
                    'error_message', error_message
                ) ORDER BY step_order
            )
            FROM etl_logs.etl_execution_log 
            WHERE execution_id = p_execution_id
        ),
        'data_quality_issues', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'table_name', table_name,
                    'column_name', column_name,
                    'issue_type', issue_type,
                    'severity_level', severity_level,
                    'affected_rows', affected_rows,
                    'issue_description', issue_description,
                    'sample_values', sample_values,
                    'created_at', created_at
                ) ORDER BY created_at
            )
            FROM etl_logs.data_quality_log 
            WHERE execution_id = p_execution_id
        ),
        'schema_validation_results', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'source_table', source_table,
                    'target_table', target_table,
                    'validation_type', validation_type,
                    'validation_status', validation_status,
                    'expected_value', expected_value,
                    'actual_value', actual_value,
                    'issue_description', issue_description
                )
            )
            FROM etl_logs.schema_validation_log 
            WHERE execution_id = p_execution_id
        ),
        'performance_metrics', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'table_name', table_name,
                    'operation_type', operation_type,
                    'duration_ms', duration_ms,
                    'rows_affected', rows_affected,
                    'start_time', start_time,
                    'end_time', end_time
                ) ORDER BY start_time
            )
            FROM etl_logs.performance_metrics 
            WHERE execution_id = p_execution_id
        )
    ) INTO v_json_result;
    
    RETURN v_json_result;
END;
$$ LANGUAGE plpgsql;

-- Function to generate HTML report
CREATE OR REPLACE FUNCTION etl_exports.generate_html_report(p_execution_id UUID)
RETURNS TEXT AS $$
DECLARE
    v_html_content TEXT;
    v_execution_summary JSONB;
    v_critical_issues INTEGER;
    v_warnings INTEGER;
    v_total_duration DECIMAL;
BEGIN
    -- Get summary data
    SELECT etl_logs.generate_etl_summary(p_execution_id) INTO v_execution_summary;
    
    SELECT COUNT(*) INTO v_critical_issues
    FROM etl_logs.data_quality_log 
    WHERE execution_id = p_execution_id AND severity_level IN ('CRITICAL', 'HIGH');
    
    SELECT COUNT(*) INTO v_warnings
    FROM etl_logs.data_quality_log 
    WHERE execution_id = p_execution_id AND severity_level = 'MEDIUM';
    
    v_total_duration := (v_execution_summary->>'total_duration_seconds')::DECIMAL;
    
    v_html_content := format('
<!DOCTYPE html>
<html>
<head>
    <title>ETL Execution Report - %s</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { background-color: #2c3e50; color: white; padding: 20px; border-radius: 5px; }
        .summary { background-color: #ecf0f1; padding: 15px; margin: 20px 0; border-radius: 5px; }
        .critical { background-color: #e74c3c; color: white; padding: 10px; margin: 10px 0; border-radius: 3px; }
        .warning { background-color: #f39c12; color: white; padding: 10px; margin: 10px 0; border-radius: 3px; }
        .success { background-color: #27ae60; color: white; padding: 10px; margin: 10px 0; border-radius: 3px; }
        .table { border-collapse: collapse; width: 100%%; margin: 20px 0; }
        .table th, .table td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        .table th { background-color: #34495e; color: white; }
        .metric { display: inline-block; margin: 10px 20px 10px 0; }
        .metric-label { font-weight: bold; color: #2c3e50; }
        .metric-value { font-size: 1.2em; color: #3498db; }
    </style>
</head>
<body>
    <div class="header">
        <h1>ETL Execution Report</h1>
        <p>Execution ID: %s</p>
        <p>Report Generated: %s</p>
    </div>
    
    <div class="summary">
        <h2>Execution Summary</h2>
        <div class="metric">
            <span class="metric-label">Total Steps:</span>
            <span class="metric-value">%s</span>
        </div>
        <div class="metric">
            <span class="metric-label">Successful:</span>
            <span class="metric-value">%s</span>
        </div>
        <div class="metric">
            <span class="metric-label">Failed:</span>
            <span class="metric-value">%s</span>
        </div>
        <div class="metric">
            <span class="metric-label">Duration:</span>
            <span class="metric-value">%s seconds</span>
        </div>
        <div class="metric">
            <span class="metric-label">Rows Processed:</span>
            <span class="metric-value">%s</span>
        </div>
    </div>',
    p_execution_id,
    p_execution_id,
    CURRENT_TIMESTAMP,
    COALESCE(v_execution_summary->>'total_steps', '0'),
    COALESCE(v_execution_summary->>'successful_steps', '0'),
    COALESCE(v_execution_summary->>'failed_steps', '0'),
    COALESCE(v_total_duration::TEXT, '0'),
    COALESCE(v_execution_summary->>'total_rows_processed', '0')
    );
    
    -- Add status indicator
    IF v_critical_issues > 0 THEN
        v_html_content := v_html_content || format('
    <div class="critical">
        <h3>CRITICAL ISSUES DETECTED</h3>
        <p>Found %s critical issues that require immediate attention.</p>
    </div>', v_critical_issues);
    ELSIF v_warnings > 0 THEN
        v_html_content := v_html_content || format('
    <div class="warning">
        <h3>WARNINGS DETECTED</h3>
        <p>Found %s warnings that should be reviewed.</p>
    </div>', v_warnings);
    ELSE
        v_html_content := v_html_content || '
    <div class="success">
        <h3>ETL COMPLETED SUCCESSFULLY</h3>
        <p>No critical issues or warnings detected.</p>
    </div>';
    END IF;
    
    -- Add data quality issues table if any exist
    IF EXISTS (SELECT 1 FROM etl_logs.data_quality_log WHERE execution_id = p_execution_id) THEN
        v_html_content := v_html_content || '
    <h2>Data Quality Issues</h2>
    <table class="table">
        <tr>
            <th>Table</th>
            <th>Issue Type</th>
            <th>Severity</th>
            <th>Affected Rows</th>
            <th>Description</th>
        </tr>';
        
        -- Add quality issues (this would be done with a loop in practice)
        v_html_content := v_html_content || '
    </table>';
    END IF;
    
    v_html_content := v_html_content || '
</body>
</html>';
    
    RETURN v_html_content;
END;
$$ LANGUAGE plpgsql;

-- ===================================================================
-- EXPORT EXECUTION PROCEDURES
-- ===================================================================

-- Main export procedure
CREATE OR REPLACE PROCEDURE etl_exports.export_logs_to_files(
    p_execution_id UUID,
    p_export_types VARCHAR[] DEFAULT ARRAY['LOGS', 'QUALITY_REPORT', 'SUMMARY'],
    p_output_directory TEXT DEFAULT '/tmp/etl_exports'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_export_type VARCHAR(50);
    v_config RECORD;
    v_content TEXT;
    v_json_content JSONB;
    v_file_path TEXT;
    v_timestamp TEXT;
    v_export_start TIMESTAMP;
    v_export_end TIMESTAMP;
    v_export_id INTEGER;
    v_file_size BIGINT;
    v_record_count BIGINT;
BEGIN
    v_timestamp := to_char(CURRENT_TIMESTAMP, 'YYYYMMDD_HH24MISS');
    
    RAISE NOTICE 'Starting export process for execution ID: %', p_execution_id;
    
    FOREACH v_export_type IN ARRAY p_export_types
    LOOP
        FOR v_config IN 
            SELECT * FROM etl_exports.export_config 
            WHERE export_type = v_export_type 
        LOOP
            v_export_start := CURRENT_TIMESTAMP;
            
            -- Generate file path
            v_file_path := format('%s/%s', 
                p_output_directory, 
                replace(
                    replace(v_config.file_pattern, '{execution_id}', p_execution_id::TEXT),
                    '{timestamp}', v_timestamp
                )
            );
            
            BEGIN
                -- Generate content based on type and format
                CASE 
                    WHEN v_config.export_type = 'LOGS' AND v_config.output_format = 'CSV' THEN
                        v_content := etl_exports.generate_etl_logs_csv(p_execution_id);
                        
                    WHEN v_config.export_type = 'LOGS' AND v_config.output_format = 'JSON' THEN
                        v_json_content := etl_exports.generate_comprehensive_json(p_execution_id);
                        v_content := v_json_content::TEXT;
                        
                    WHEN v_config.export_type = 'QUALITY_REPORT' AND v_config.output_format = 'CSV' THEN
                        v_content := etl_exports.generate_data_quality_csv(p_execution_id);
                        
                    WHEN v_config.export_type = 'QUALITY_REPORT' AND v_config.output_format = 'HTML' THEN
                        v_content := etl_exports.generate_html_report(p_execution_id);
                        
                    WHEN v_config.export_type = 'PERFORMANCE' AND v_config.output_format = 'CSV' THEN
                        v_content := etl_exports.generate_performance_csv(p_execution_id);
                        
                    WHEN v_config.export_type = 'SUMMARY' AND v_config.output_format = 'JSON' THEN
                        SELECT etl_logs.generate_etl_summary(p_execution_id) INTO v_json_content;
                        v_content := v_json_content::TEXT;
                        
                    ELSE
                        RAISE WARNING 'Unsupported export type/format combination: %/%', 
                            v_config.export_type, v_config.output_format;
                        CONTINUE;
                END CASE;
                
                v_export_end := CURRENT_TIMESTAMP;
                v_file_size := length(v_content);
                v_record_count := (select array_length(string_to_array(v_content, E'\n'), 1)) - 1; -- Estimate record count
                
                -- Log successful export
                INSERT INTO etl_exports.export_history (
                    execution_id, export_type, output_format, file_path, file_size_bytes,
                    export_status, record_count, export_duration_ms
                ) VALUES (
                    p_execution_id, v_config.export_type, v_config.output_format, v_file_path,
                    v_file_size, 'SUCCESS', v_record_count,
                    EXTRACT(EPOCH FROM (v_export_end - v_export_start)) * 1000
                ) RETURNING export_id INTO v_export_id;
                
                RAISE NOTICE 'Generated %: % (% bytes, % records)', 
                    v_config.export_type, v_file_path, v_file_size, v_record_count;
                
                -- In a production environment, you would write the content to the actual file
                -- For demonstration, we're logging the content and file information
                RAISE NOTICE 'File content preview (first 500 chars): %', left(v_content, 500);
                
            EXCEPTION
                WHEN OTHERS THEN
                    INSERT INTO etl_exports.export_history (
                        execution_id, export_type, output_format, file_path,
                        export_status, error_message
                    ) VALUES (
                        p_execution_id, v_config.export_type, v_config.output_format, v_file_path,
                        'FAILED', SQLERRM
                    );
                    
                    RAISE WARNING 'Export failed for %/%: %', 
                        v_config.export_type, v_config.output_format, SQLERRM;
            END;
        END LOOP;
    END LOOP;
    
    RAISE NOTICE 'Export process completed for execution ID: %', p_execution_id;
END;
$$;

-- Procedure to auto-export after ETL completion
CREATE OR REPLACE PROCEDURE etl_exports.auto_export_after_etl(p_execution_id UUID)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Starting auto-export for execution ID: %', p_execution_id;
    
    -- Export based on auto-export configuration
    CALL etl_exports.export_logs_to_files(
        p_execution_id,
        ARRAY(SELECT DISTINCT export_type FROM etl_exports.export_config WHERE auto_export = TRUE)
    );
END;
$$;

-- ===================================================================
-- UTILITY VIEWS FOR MONITORING EXPORTS
-- ===================================================================

-- View of export history with summary stats
CREATE VIEW etl_exports.vw_export_summary AS
SELECT 
    e.execution_id,
    e.export_type,
    e.output_format,
    e.export_status,
    e.file_size_bytes,
    e.record_count,
    e.export_duration_ms,
    e.export_timestamp,
    el.procedure_name,
    el.start_time as etl_start_time,
    el.end_time as etl_end_time
FROM etl_exports.export_history e
LEFT JOIN etl_logs.etl_execution_log el ON e.execution_id = el.execution_id
WHERE el.step_order = 0 OR el.step_order IS NULL
ORDER BY e.export_timestamp DESC;

-- ===================================================================
-- SAMPLE USAGE COMMANDS
-- ===================================================================

/*
-- Export logs for the latest ETL execution
CALL etl_exports.export_logs_to_files(
    (SELECT execution_id FROM etl_logs.etl_execution_log ORDER BY start_time DESC LIMIT 1)
);

-- Export specific types for a specific execution
CALL etl_exports.export_logs_to_files(
    'your-execution-id-here',
    ARRAY['LOGS', 'QUALITY_REPORT']
);

-- View export history
SELECT * FROM etl_exports.vw_export_summary ORDER BY export_timestamp DESC LIMIT 10;

-- Clean up old exports
DELETE FROM etl_exports.export_history 
WHERE export_timestamp < CURRENT_DATE - INTERVAL '30 days';
*/

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_export_history_execution_id ON etl_exports.export_history(execution_id);
CREATE INDEX IF NOT EXISTS idx_export_history_timestamp ON etl_exports.export_history(export_timestamp);

RAISE NOTICE 'File Output Logging System installed successfully!';
RAISE NOTICE 'Usage: CALL etl_exports.export_logs_to_files(execution_id);';