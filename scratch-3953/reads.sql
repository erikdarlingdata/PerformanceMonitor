\set ON_ERROR_STOP on
SET search_path = collect, public;
\pset format unaligned
\pset pager off
\set pre 'EXPLAIN (ANALYZE, BUFFERS)'

\set wend1 '''2026-09-25 17:05:06'''
\set w1s   '''2026-09-25 16:05:06'''
\set w24s  '''2026-09-24 17:05:06'''
\set w7s   '''2026-09-18 17:05:06'''

\qecho ##### WINDOW 1h #####
\set ws :w1s
\set wend :wend1
CREATE OR REPLACE TEMP VIEW d_cur AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                                 ORDER BY collection_time DESC, execution_count DESC) AS rn
    FROM collect.query_store_stats
    WHERE server_id = 1 AND collection_time >= :ws AND collection_time <= :wend
) AS x WHERE rn = 1;

\set top 100
\set src d_cur
\set q scratch-3953/q_wpf.sql
\qecho -- BASELINE wpf 1h run1
\i :q
\qecho -- BASELINE wpf 1h run2
\i :q
\set top 25
\set src d_cur
\set q scratch-3953/q_mcp.sql
\qecho -- BASELINE mcp 1h run1
\i :q
\qecho -- BASELINE mcp 1h run2
\i :q
\qecho -- BASELINE slicer 1h run1
\i scratch-3953/q_slicer_raw.sql
\qecho -- BASELINE slicer 1h run2
\i scratch-3953/q_slicer_raw.sql

CREATE OR REPLACE TEMP VIEW d_shape AS SELECT * FROM collect.proto_a WHERE server_id = 1 AND collection_time >= :ws AND collection_time <= :wend;
\set srctbl collect.proto_a
\set top 100
\set src d_shape
\set q scratch-3953/q_wpf.sql
\qecho -- SHAPE a wpf 1h run1
\i :q
\qecho -- SHAPE a wpf 1h run2
\i :q
\set top 25
\set src d_shape
\set q scratch-3953/q_mcp.sql
\qecho -- SHAPE a mcp 1h run1
\i :q
\qecho -- SHAPE a mcp 1h run2
\i :q
\qecho -- SHAPE a slicer 1h run1
\i scratch-3953/q_slicer_wide.sql
\qecho -- SHAPE a slicer 1h run2
\i scratch-3953/q_slicer_wide.sql

CREATE OR REPLACE TEMP VIEW d_shape AS SELECT * FROM collect.proto_b WHERE server_id = 1 AND collection_time >= :ws AND collection_time <= :wend;
\set srctbl collect.proto_b
\set top 100
\set src d_shape
\set q scratch-3953/q_wpf.sql
\qecho -- SHAPE b wpf 1h run1
\i :q
\qecho -- SHAPE b wpf 1h run2
\i :q
\set top 25
\set src d_shape
\set q scratch-3953/q_mcp.sql
\qecho -- SHAPE b mcp 1h run1
\i :q
\qecho -- SHAPE b mcp 1h run2
\i :q
\qecho -- SHAPE b slicer 1h run1
\i scratch-3953/q_slicer_wide.sql
\qecho -- SHAPE b slicer 1h run2
\i scratch-3953/q_slicer_wide.sql

CREATE OR REPLACE TEMP VIEW d_shape AS SELECT * FROM collect.proto_c8 WHERE server_id = 1 AND collection_time >= :ws AND collection_time <= :wend;
\set srctbl collect.proto_c8
\set top 100
\set src d_shape
\set q scratch-3953/q_wpf.sql
\qecho -- SHAPE c wpf 1h run1
\i :q
\qecho -- SHAPE c wpf 1h run2
\i :q
\set top 25
\set src d_shape
\set q scratch-3953/q_mcp.sql
\qecho -- SHAPE c mcp 1h run1
\i :q
\qecho -- SHAPE c mcp 1h run2
\i :q
\qecho -- SHAPE c slicer 1h run1
\i scratch-3953/q_slicer_wide.sql
\qecho -- SHAPE c slicer 1h run2
\i scratch-3953/q_slicer_wide.sql

\qecho ##### WINDOW 24h #####
\set ws :w24s
\set wend :wend1
CREATE OR REPLACE TEMP VIEW d_cur AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                                 ORDER BY collection_time DESC, execution_count DESC) AS rn
    FROM collect.query_store_stats
    WHERE server_id = 1 AND collection_time >= :ws AND collection_time <= :wend
) AS x WHERE rn = 1;

\set top 100
\set src d_cur
\set q scratch-3953/q_wpf.sql
\qecho -- BASELINE wpf 24h run1
\i :q
\qecho -- BASELINE wpf 24h run2
\i :q
\set top 25
\set src d_cur
\set q scratch-3953/q_mcp.sql
\qecho -- BASELINE mcp 24h run1
\i :q
\qecho -- BASELINE mcp 24h run2
\i :q
\qecho -- BASELINE slicer 24h run1
\i scratch-3953/q_slicer_raw.sql
\qecho -- BASELINE slicer 24h run2
\i scratch-3953/q_slicer_raw.sql

CREATE OR REPLACE TEMP VIEW d_shape AS SELECT * FROM collect.proto_a WHERE server_id = 1 AND collection_time >= :ws AND collection_time <= :wend;
\set srctbl collect.proto_a
\set top 100
\set src d_shape
\set q scratch-3953/q_wpf.sql
\qecho -- SHAPE a wpf 24h run1
\i :q
\qecho -- SHAPE a wpf 24h run2
\i :q
\set top 25
\set src d_shape
\set q scratch-3953/q_mcp.sql
\qecho -- SHAPE a mcp 24h run1
\i :q
\qecho -- SHAPE a mcp 24h run2
\i :q
\qecho -- SHAPE a slicer 24h run1
\i scratch-3953/q_slicer_wide.sql
\qecho -- SHAPE a slicer 24h run2
\i scratch-3953/q_slicer_wide.sql

CREATE OR REPLACE TEMP VIEW d_shape AS SELECT * FROM collect.proto_b WHERE server_id = 1 AND collection_time >= :ws AND collection_time <= :wend;
\set srctbl collect.proto_b
\set top 100
\set src d_shape
\set q scratch-3953/q_wpf.sql
\qecho -- SHAPE b wpf 24h run1
\i :q
\qecho -- SHAPE b wpf 24h run2
\i :q
\set top 25
\set src d_shape
\set q scratch-3953/q_mcp.sql
\qecho -- SHAPE b mcp 24h run1
\i :q
\qecho -- SHAPE b mcp 24h run2
\i :q
\qecho -- SHAPE b slicer 24h run1
\i scratch-3953/q_slicer_wide.sql
\qecho -- SHAPE b slicer 24h run2
\i scratch-3953/q_slicer_wide.sql

CREATE OR REPLACE TEMP VIEW d_shape AS SELECT * FROM collect.proto_c8 WHERE server_id = 1 AND collection_time >= :ws AND collection_time <= :wend;
\set srctbl collect.proto_c8
\set top 100
\set src d_shape
\set q scratch-3953/q_wpf.sql
\qecho -- SHAPE c wpf 24h run1
\i :q
\qecho -- SHAPE c wpf 24h run2
\i :q
\set top 25
\set src d_shape
\set q scratch-3953/q_mcp.sql
\qecho -- SHAPE c mcp 24h run1
\i :q
\qecho -- SHAPE c mcp 24h run2
\i :q
\qecho -- SHAPE c slicer 24h run1
\i scratch-3953/q_slicer_wide.sql
\qecho -- SHAPE c slicer 24h run2
\i scratch-3953/q_slicer_wide.sql

\qecho ##### WINDOW 7d #####
\set ws :w7s
\set wend :wend1
CREATE OR REPLACE TEMP VIEW d_cur AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                                 ORDER BY collection_time DESC, execution_count DESC) AS rn
    FROM collect.query_store_stats
    WHERE server_id = 1 AND collection_time >= :ws AND collection_time <= :wend
) AS x WHERE rn = 1;

\set top 100
\set src d_cur
\set q scratch-3953/q_wpf.sql
\qecho -- BASELINE wpf 7d run1
\i :q
\qecho -- BASELINE wpf 7d run2
\i :q
\set top 25
\set src d_cur
\set q scratch-3953/q_mcp.sql
\qecho -- BASELINE mcp 7d run1
\i :q
\qecho -- BASELINE mcp 7d run2
\i :q
\qecho -- BASELINE slicer 7d run1
\i scratch-3953/q_slicer_raw.sql
\qecho -- BASELINE slicer 7d run2
\i scratch-3953/q_slicer_raw.sql

CREATE OR REPLACE TEMP VIEW d_shape AS SELECT * FROM collect.proto_a WHERE server_id = 1 AND collection_time >= :ws AND collection_time <= :wend;
\set srctbl collect.proto_a
\set top 100
\set src d_shape
\set q scratch-3953/q_wpf.sql
\qecho -- SHAPE a wpf 7d run1
\i :q
\qecho -- SHAPE a wpf 7d run2
\i :q
\set top 25
\set src d_shape
\set q scratch-3953/q_mcp.sql
\qecho -- SHAPE a mcp 7d run1
\i :q
\qecho -- SHAPE a mcp 7d run2
\i :q
\qecho -- SHAPE a slicer 7d run1
\i scratch-3953/q_slicer_wide.sql
\qecho -- SHAPE a slicer 7d run2
\i scratch-3953/q_slicer_wide.sql

CREATE OR REPLACE TEMP VIEW d_shape AS SELECT * FROM collect.proto_b WHERE server_id = 1 AND collection_time >= :ws AND collection_time <= :wend;
\set srctbl collect.proto_b
\set top 100
\set src d_shape
\set q scratch-3953/q_wpf.sql
\qecho -- SHAPE b wpf 7d run1
\i :q
\qecho -- SHAPE b wpf 7d run2
\i :q
\set top 25
\set src d_shape
\set q scratch-3953/q_mcp.sql
\qecho -- SHAPE b mcp 7d run1
\i :q
\qecho -- SHAPE b mcp 7d run2
\i :q
\qecho -- SHAPE b slicer 7d run1
\i scratch-3953/q_slicer_wide.sql
\qecho -- SHAPE b slicer 7d run2
\i scratch-3953/q_slicer_wide.sql

CREATE OR REPLACE TEMP VIEW d_shape AS SELECT * FROM collect.proto_c8 WHERE server_id = 1 AND collection_time >= :ws AND collection_time <= :wend;
\set srctbl collect.proto_c8
\set top 100
\set src d_shape
\set q scratch-3953/q_wpf.sql
\qecho -- SHAPE c wpf 7d run1
\i :q
\qecho -- SHAPE c wpf 7d run2
\i :q
\set top 25
\set src d_shape
\set q scratch-3953/q_mcp.sql
\qecho -- SHAPE c mcp 7d run1
\i :q
\qecho -- SHAPE c mcp 7d run2
\i :q
\qecho -- SHAPE c slicer 7d run1
\i scratch-3953/q_slicer_wide.sql
\qecho -- SHAPE c slicer 7d run2
\i scratch-3953/q_slicer_wide.sql

\qecho ##### PLAN_REGRESSION + DRILLDOWN (fixed 14d/15d lookback) #####
\set lastexec '''2026-09-11 17:05:06'''
\set collbound '''2026-09-10 17:05:06'''
\set srctbl collect.proto_v143
\set etypefilter ''
\qecho -- baseline_v143 planreg run1
\i scratch-3953/q_planreg.sql
\qecho -- baseline_v143 planreg run2
\i scratch-3953/q_planreg.sql
\qecho -- baseline_v143 drill run1
\i scratch-3953/q_drill.sql
\qecho -- baseline_v143 drill run2
\i scratch-3953/q_drill.sql
\set srctbl collect.proto_a
\set etypefilter AND execution_type_desc = 'Regular'
\qecho -- shape_a planreg run1
\i scratch-3953/q_planreg.sql
\qecho -- shape_a planreg run2
\i scratch-3953/q_planreg.sql
\qecho -- shape_a drill run1
\i scratch-3953/q_drill.sql
\qecho -- shape_a drill run2
\i scratch-3953/q_drill.sql
\set srctbl collect.proto_b
\set etypefilter AND execution_type_desc = 'Regular'
\qecho -- shape_b planreg run1
\i scratch-3953/q_planreg.sql
\qecho -- shape_b planreg run2
\i scratch-3953/q_planreg.sql
\qecho -- shape_b drill run1
\i scratch-3953/q_drill.sql
\qecho -- shape_b drill run2
\i scratch-3953/q_drill.sql
\set srctbl collect.proto_v143
\set etypefilter ''
\qecho -- shape_c_v143 planreg run1
\i scratch-3953/q_planreg.sql
\qecho -- shape_c_v143 planreg run2
\i scratch-3953/q_planreg.sql
\qecho -- shape_c_v143 drill run1
\i scratch-3953/q_drill.sql
\qecho -- shape_c_v143 drill run2
\i scratch-3953/q_drill.sql

\qecho ##### DONE READS #####
