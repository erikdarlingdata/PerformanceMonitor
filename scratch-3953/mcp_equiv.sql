\set ON_ERROR_STOP on
SET search_path = collect, public;
\pset format unaligned

\set wend '''2026-09-25 17:05:06'''
\set w24s '''2026-09-24 17:05:06'''

-- Pick etype/module with real rows on both sides
\qecho -- candidate counts
SELECT execution_type_desc, module_name, count(*) FROM collect.proto_a
WHERE server_id = 1 AND collection_time >= :w24s AND collection_time <= :wend
AND execution_type_desc = 'Aborted' AND module_name = 'dbo.usp_proc_50'
GROUP BY execution_type_desc, module_name;

CREATE OR REPLACE TEMP VIEW d_cur24 AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                                 ORDER BY collection_time DESC, execution_count DESC) AS rn
    FROM collect.query_store_stats
    WHERE server_id = 1 AND collection_time >= :w24s AND collection_time <= :wend
) AS x WHERE rn = 1;

\set etype 'Aborted'
\set modname 'dbo.usp_proc_50'

CREATE TEMP TABLE mcp_raw AS
SELECT database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role,
       MAX(module_name) AS module_name,
       CAST(SUM(execution_count) AS bigint) AS total_executions,
       round((AVG(CAST(avg_duration_us AS double precision)) / 1000.0)::numeric, 6) AS avg_duration_ms,
       round((AVG(CAST(avg_cpu_time_us AS double precision)) / 1000.0)::numeric, 6) AS avg_cpu_time_ms
FROM d_cur24
WHERE rn = 1 AND execution_type_desc = :'etype' AND module_name = :'modname'
GROUP BY database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role;

\qecho -- shape a MCP equivalence ($6/$7 set)
CREATE TEMP TABLE mcp_a AS
SELECT database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role,
       MAX(module_name) AS module_name,
       CAST(SUM(execution_count) AS bigint) AS total_executions,
       round((AVG(CAST(avg_duration_us AS double precision)) / 1000.0)::numeric, 6) AS avg_duration_ms,
       round((AVG(CAST(avg_cpu_time_us AS double precision)) / 1000.0)::numeric, 6) AS avg_cpu_time_ms
FROM collect.proto_a
WHERE server_id = 1 AND collection_time >= :w24s AND collection_time <= :wend
AND execution_type_desc = :'etype' AND module_name = :'modname'
GROUP BY database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role;

SELECT 'a rows raw=' || (SELECT count(*) FROM mcp_raw) || ' wide=' || (SELECT count(*) FROM mcp_a)
    || ' raw-wide=' || (SELECT count(*) FROM (SELECT * FROM mcp_raw EXCEPT ALL SELECT * FROM mcp_a) d)
    || ' wide-raw=' || (SELECT count(*) FROM (SELECT * FROM mcp_a EXCEPT ALL SELECT * FROM mcp_raw) d);

\qecho -- shape b MCP equivalence ($6/$7 set)
CREATE TEMP TABLE mcp_b AS
SELECT database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role,
       MAX(module_name) AS module_name,
       CAST(SUM(execution_count) AS bigint) AS total_executions,
       round((AVG(CAST(avg_duration_us AS double precision)) / 1000.0)::numeric, 6) AS avg_duration_ms,
       round((AVG(CAST(avg_cpu_time_us AS double precision)) / 1000.0)::numeric, 6) AS avg_cpu_time_ms
FROM collect.proto_b
WHERE server_id = 1 AND collection_time >= :w24s AND collection_time <= :wend
AND execution_type_desc = :'etype' AND module_name = :'modname'
GROUP BY database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role;

SELECT 'b rows raw=' || (SELECT count(*) FROM mcp_raw) || ' wide=' || (SELECT count(*) FROM mcp_b)
    || ' raw-wide=' || (SELECT count(*) FROM (SELECT * FROM mcp_raw EXCEPT ALL SELECT * FROM mcp_b) d)
    || ' wide-raw=' || (SELECT count(*) FROM (SELECT * FROM mcp_b EXCEPT ALL SELECT * FROM mcp_raw) d);

\qecho -- shape c (proto_c8) MCP equivalence ($6/$7 set)
CREATE TEMP TABLE mcp_c AS
SELECT database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role,
       MAX(module_name) AS module_name,
       CAST(SUM(execution_count) AS bigint) AS total_executions,
       round((AVG(CAST(avg_duration_us AS double precision)) / 1000.0)::numeric, 6) AS avg_duration_ms,
       round((AVG(CAST(avg_cpu_time_us AS double precision)) / 1000.0)::numeric, 6) AS avg_cpu_time_ms
FROM collect.proto_c8
WHERE server_id = 1 AND collection_time >= :w24s AND collection_time <= :wend
AND execution_type_desc = :'etype' AND module_name = :'modname'
GROUP BY database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role;

SELECT 'c rows raw=' || (SELECT count(*) FROM mcp_raw) || ' wide=' || (SELECT count(*) FROM mcp_c)
    || ' raw-wide=' || (SELECT count(*) FROM (SELECT * FROM mcp_raw EXCEPT ALL SELECT * FROM mcp_c) d)
    || ' wide-raw=' || (SELECT count(*) FROM (SELECT * FROM mcp_c EXCEPT ALL SELECT * FROM mcp_raw) d);

\qecho ##### DONE MCP EQUIV #####
