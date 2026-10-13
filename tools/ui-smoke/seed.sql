-- Seed for the web UI smoke check (#5701). Run with psql AFTER the service has started against the throwaway store
-- (it migrates the store and seeds config.config_monitored_servers from darling.ui-smoke.json).
--
--   psql -h 127.0.0.1 -p <port> -U darling -d darling -v ON_ERROR_STOP=1 -f seed.sql
--
-- Rules, so the assertions in pages.json can be exact:
--   * Every time is relative to the store's own clock: now() AT TIME ZONE 'UTC', cut to the minute. Nothing is dated.
--   * Every count is fixed: the same rows on every run, whatever the minute.
--   * The target is the one SQL Server the config registers ("ui-sql", an address nothing listens on, so the service
--     never collects and never overwrites what is seeded here).
--   * To cover another table, add one more section below, in the same shape, and name what it feeds. Keep the
--     collection_id / log_id ranges apart (the ranges are listed in each section) so rows never collide.
--
-- Seeded (SQL Server target "ui-sql", 6 hours back, one row a minute unless said otherwise):
--   cpu_utilization_stats 360 | memory_stats 360 | wait_stats 4 types x 360 | file_io_stats 2 files x 360
--   query_stats 12 queries (3 databases x 4) x 36 collections (every 10 minutes) | server_properties 1
--   database_size_stats 3 databases x 2 files, newest minute | collection_log 5 collectors x 72 (every 5 minutes), all success

\set ON_ERROR_STOP on
BEGIN;

-- The registry row the config seed made. server_name in every collect table is the STORAGE name (the host, for a SQL
-- Server entry without a database), the name in the sidebar and the URL is the config name "ui-sql".
SELECT server_id AS sid, host AS sname
FROM config.config_monitored_servers
WHERE name = 'ui-sql'
\gset

-- The observed-registry row (the service writes it on a first connect, which never happens here).
INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, sql_major_version, engine_kind, created_date, modified_date)
VALUES (:sid, :'sname', 'ui-sql', TRUE, 3, 16, 'sqlserver',
        (now() AT TIME ZONE 'UTC') - interval '7 days', now() AT TIME ZONE 'UTC')
ON CONFLICT (server_id) DO UPDATE SET
    server_name = EXCLUDED.server_name, display_name = EXCLUDED.display_name, is_enabled = TRUE,
    sql_engine_edition = 3, sql_major_version = 16, engine_kind = 'sqlserver', modified_date = EXCLUDED.modified_date;

-- The minutes: m = 0 is the current minute, m = 359 is six hours earlier.
CREATE TEMP TABLE ui_minutes ON COMMIT DROP AS
SELECT m, date_trunc('minute', now() AT TIME ZONE 'UTC') - make_interval(mins => m) AS t
FROM generate_series(0, 359) AS m;

-- cpu_utilization_stats: Overview and CPU tabs. collection_id 1000000 + m.
INSERT INTO collect.cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization, sample_time_utc)
SELECT 1000000 + m, t, :sid, :'sname', t, 20 + (m % 30), 5 + (m % 3), t
FROM ui_minutes;

-- memory_stats: Overview and Memory tabs. collection_id 2000000 + m.
INSERT INTO collect.memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb,
    total_page_file_mb, available_page_file_mb, system_memory_state, sql_memory_model, target_server_memory_mb, total_server_memory_mb,
    buffer_pool_mb, plan_cache_mb, max_workers_count, current_workers_count)
SELECT 2000000 + m, t, :sid, :'sname', 32768, 6000 + (m % 40) * 50, 40960, 30000, 'Available physical memory is high', 'CONVENTIONAL',
       28672, 24000 + (360 - m) * 5, 18000, 1200 + (m % 20), 512, 80 + (m % 10)
FROM ui_minutes;

-- wait_stats: Overview and Wait Stats tabs. Four types, cumulative counters plus the per-interval deltas the reads use.
-- collection_id 3000000 + 1000 * type + m.
INSERT INTO collect.wait_stats (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
    delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
SELECT 3000000 + 1000 * w.n + m.m, m.t, :sid, :'sname', w.wait_type,
       (360 - m.m) * w.tasks, (360 - m.m) * w.ms, (360 - m.m) * w.sig,
       w.tasks, w.ms, w.sig, 60
FROM ui_minutes m
CROSS JOIN (VALUES (1, 'PAGEIOLATCH_SH', 40, 4000, 40), (2, 'CXPACKET', 120, 9000, 600),
                   (3, 'LCK_M_X', 6, 3000, 5), (4, 'WRITELOG', 90, 1500, 30)) AS w(n, wait_type, tasks, ms, sig);

-- file_io_stats: Overview and I/O tabs. Two files of one database. collection_id 4000000 + 1000 * file + m.
INSERT INTO collect.file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
    num_of_reads, num_of_writes, read_bytes, write_bytes, io_stall_read_ms, io_stall_write_ms,
    delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
SELECT 4000000 + 1000 * f.n + m.m, m.t, :sid, :'sname', 'SalesDb', f.file_name, f.file_type, f.physical_name, f.size_mb,
       (360 - m.m) * f.reads, (360 - m.m) * f.writes, (360 - m.m) * f.reads * 8192, (360 - m.m) * f.writes * 8192,
       (360 - m.m) * f.reads * 2, (360 - m.m) * f.writes,
       f.reads, f.writes, f.reads * 8192, f.writes * 8192, f.reads * 2 + (m.m % 5), f.writes + (m.m % 3), 60
FROM ui_minutes m
CROSS JOIN (VALUES (1, 'SalesDb', 'ROWS', 'D:\data\SalesDb.mdf', 20480, 300, 80), (2, 'SalesDb_log', 'LOG', 'L:\log\SalesDb_log.ldf', 4096, 5, 200))
    AS f(n, file_name, file_type, physical_name, size_mb, reads, writes);

-- query_stats: Queries tab, 12 queries over 3 databases, 36 collections each (every 10 minutes).
-- collection_id 5000000 + 1000 * query + c. Cumulative counters rise with c; the delta columns are one interval.
INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, creation_time,
    last_execution_time, execution_count, total_worker_time, total_elapsed_time, total_logical_reads, total_logical_writes, total_physical_reads, total_rows,
    min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time, query_text, sql_handle, plan_handle,
    delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows,
    sample_interval_seconds)
SELECT 5000000 + 1000 * q.n + c.c, c.t, :sid, :'sname', q.db, '0x' || lpad(to_hex(q.n * 4099), 16, '0'), '0x' || lpad(to_hex(q.n * 7919), 16, '0'),
       c.t - interval '2 days', c.t,
       (36 - c.c) * q.execs, (36 - c.c) * q.execs * q.cpu_us, (36 - c.c) * q.execs * q.cpu_us * 2, (36 - c.c) * q.execs * q.reads, (36 - c.c) * q.execs / 10,
       (36 - c.c) * q.execs / 50, (36 - c.c) * q.execs * 3,
       q.cpu_us / 2, q.cpu_us * 3, q.cpu_us, q.cpu_us * 6,
       q.query_text, '0x02' || lpad(to_hex(q.n), 8, '0'), '0x06' || lpad(to_hex(q.n), 8, '0'),
       q.execs, q.execs * q.cpu_us, q.execs * q.cpu_us * 2, q.execs * q.reads, q.execs / 10, q.execs / 50, q.execs * 3, 600
FROM (SELECT cc AS c, date_trunc('minute', now() AT TIME ZONE 'UTC') - make_interval(mins => cc * 10) AS t FROM generate_series(0, 35) AS cc) c
CROSS JOIN (VALUES
    (1,  'SalesDb',     'SELECT TOP (100) o.OrderId, o.Total FROM dbo.Orders AS o WHERE o.CustomerId = @CustomerId ORDER BY o.OrderDate DESC;', 600, 4200, 900),
    (2,  'SalesDb',     'UPDATE dbo.Orders SET Status = @Status WHERE OrderId = @OrderId;', 900, 800, 120),
    (3,  'SalesDb',     'SELECT c.CustomerId, SUM(o.Total) FROM dbo.Customers AS c JOIN dbo.Orders AS o ON o.CustomerId = c.CustomerId GROUP BY c.CustomerId;', 40, 95000, 24000),
    (4,  'SalesDb',     'INSERT dbo.OrderLines (OrderId, ProductId, Qty) VALUES (@OrderId, @ProductId, @Qty);', 1200, 300, 30),
    (5,  'InventoryDb', 'SELECT p.ProductId, p.OnHand FROM dbo.Products AS p WHERE p.WarehouseId = @WarehouseId;', 500, 2500, 600),
    (6,  'InventoryDb', 'UPDATE dbo.Products SET OnHand = OnHand - @Qty WHERE ProductId = @ProductId;', 700, 900, 150),
    (7,  'InventoryDb', 'SELECT w.WarehouseId, COUNT(*) FROM dbo.Warehouses AS w JOIN dbo.Products AS p ON p.WarehouseId = w.WarehouseId GROUP BY w.WarehouseId;', 30, 60000, 15000),
    (8,  'InventoryDb', 'DELETE dbo.StockMoves WHERE MovedAt < @Cutoff;', 5, 150000, 80000),
    (9,  'ReportingDb', 'SELECT * FROM dbo.DailySales WHERE SaleDate >= @From;', 80, 30000, 7000),
    (10, 'ReportingDb', 'SELECT TOP (50) * FROM dbo.TopCustomers ORDER BY Revenue DESC;', 60, 22000, 5000),
    (11, 'ReportingDb', 'EXEC dbo.RefreshDailySales;', 2, 400000, 90000),
    (12, 'ReportingDb', 'SELECT COUNT(*) FROM dbo.AuditTrail WHERE ActorId = @ActorId;', 150, 8000, 1800))
    AS q(n, db, query_text, execs, cpu_us, reads);

-- server_properties: Overview. One row, the newest minute. collection_id 6000000.
INSERT INTO collect.server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition,
    cpu_count, hyperthread_ratio, physical_memory_mb, socket_count, cores_per_socket, is_hadr_enabled, is_clustered, sqlserver_start_time)
SELECT 6000000, t, :sid, :'sname', 'Enterprise Edition (64-bit)', '16.0.4135.4', 'RTM', 3, 8, 2, 32768, 1, 8, FALSE, FALSE, t - interval '9 days'
FROM ui_minutes WHERE m = 0;

-- database_size_stats: Overview and Health. 3 databases x data and log file, the newest minute. collection_id 7000000 + 10 * db + file.
INSERT INTO collect.database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc,
    file_name, physical_name, total_size_mb, used_size_mb, auto_growth_mb, max_size_mb, recovery_model_desc, compatibility_level, state_desc,
    volume_mount_point, volume_total_mb, volume_free_mb)
SELECT 7000000 + 10 * d.n + f.fid, mm.t, :sid, :'sname', d.db, 4 + d.n, f.fid, f.kind,
       d.db || f.suffix, f.dir || d.db || f.suffix, f.total_mb, f.total_mb / 2, 256, -1, 'FULL', 160, 'ONLINE',
       f.dir, 512000, 300000
FROM (SELECT t FROM ui_minutes WHERE m = 0) mm
CROSS JOIN (VALUES (1, 'SalesDb'), (2, 'InventoryDb'), (3, 'ReportingDb')) AS d(n, db)
CROSS JOIN (VALUES (1, 'ROWS', '.mdf', 'D:\data\', 20480), (2, 'LOG', '_log.ldf', 'L:\log\', 4096)) AS f(fid, kind, suffix, dir, total_mb);

-- collection_log: Health and Collection Health. 5 collectors, every 5 minutes, all success. log_id 8000000 + 1000 * collector + k.
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT 8000000 + 1000 * cl.n + k.k, :sid, :'sname', cl.name,
       date_trunc('minute', now() AT TIME ZONE 'UTC') - make_interval(mins => k.k * 5), 120 + k.k, 'SUCCESS', cl.rows_collected
FROM generate_series(0, 71) AS k(k)
CROSS JOIN (VALUES (1, 'wait_stats', 4), (2, 'cpu_utilization_stats', 1), (3, 'memory_stats', 1), (4, 'file_io_stats', 2), (5, 'query_stats', 12))
    AS cl(n, name, rows_collected);

COMMIT;

-- One line a person (or the log) can read to see what landed.
SELECT 'ui-smoke seed: server_id ' || :sid || ' (' || :'sname' || ')' AS seeded,
       (SELECT count(*) FROM collect.cpu_utilization_stats WHERE server_id = :sid) AS cpu_rows,
       (SELECT count(*) FROM collect.query_stats WHERE server_id = :sid) AS query_rows,
       (SELECT count(*) FROM collect.wait_stats WHERE server_id = :sid) AS wait_rows;
