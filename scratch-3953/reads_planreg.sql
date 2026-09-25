\set ON_ERROR_STOP on
SET search_path = collect, public;
\pset format unaligned
\pset pager off
\set pre 'EXPLAIN (ANALYZE, BUFFERS)'

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
\set etypefilter 'AND execution_type_desc = ''Regular'''
\qecho -- shape_a planreg run1
\i scratch-3953/q_planreg.sql
\qecho -- shape_a planreg run2
\i scratch-3953/q_planreg.sql
\qecho -- shape_a drill run1
\i scratch-3953/q_drill.sql
\qecho -- shape_a drill run2
\i scratch-3953/q_drill.sql

\set srctbl collect.proto_b
\set etypefilter 'AND execution_type_desc = ''Regular'''
\qecho -- shape_b planreg run1
\i scratch-3953/q_planreg.sql
\qecho -- shape_b planreg run2
\i scratch-3953/q_planreg.sql
\qecho -- shape_b drill run1
\i scratch-3953/q_drill.sql
\qecho -- shape_b drill run2
\i scratch-3953/q_drill.sql

\qecho -- shape_c uses proto_v143 unchanged: same as baseline_v143 above, not re-run

\qecho ##### DONE PLANREG #####
