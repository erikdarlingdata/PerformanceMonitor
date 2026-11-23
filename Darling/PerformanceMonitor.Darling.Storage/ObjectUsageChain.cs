/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The persisted object usage chain (V299, #9349): the per-snapshot facts, the per-index contributions and the index presence runs that
/// <c>get_object_usage</c> reads instead of every index row of every snapshot in its window.
///
/// <para><b>Derived, never kept longer than its source.</b> All three tables are computed from <c>collect.index_object_stats</c>. The two
/// per-snapshot tables are hypertables on <c>collection_time</c> where TimescaleDB is present (<see cref="TimescaleSupport.EnsureObjectUsageHypertablesAsync"/>),
/// compressed on the same schedule and chunk interval as <c>index_object_stats</c>, and a plain table with the time-sliced DELETE otherwise.
/// The retention pass purges them at the horizon it resolves for <c>index_object_stats</c> (<c>DarlingRetention.PurgeAsync</c>), so a per-collector
/// retention override applies to all three at once and a derived row never outlives its raw rows. <see cref="RunsTable"/> is a plain table the same
/// retention pass purges: a closed run until it is older than both the raw horizon and the oldest raw chunk still stored (<c>drop_chunks</c> keeps a raw chunk until
/// all of it is past the horizon, and the full fold still reads those rows), and an open run of a pair that has no raw row left. The builder only writes runs.</para>
/// </summary>
public static class ObjectUsageChain
{
    /// <summary>The store schema version that creates these tables.</summary>
    public const int Version = 299;

    /// <summary>The collector whose table the chain derives from. Its retention horizon is the chain's.</summary>
    public const string SourceCollector = "index_object_stats";

    /// <summary>One row per (server, database, snapshot): the chain step's facts.</summary>
    public const string SnapshotsTable = "collect.object_usage_snapshots";

    /// <summary>One row per (server, database, snapshot, index) where the step contributed or anything moved.</summary>
    public const string DeltasTable = "collect.object_usage_deltas";

    /// <summary>Index presence and identity runs. A plain table on every store.</summary>
    public const string RunsTable = "collect.object_usage_runs";

    /// <summary>The time column of the two per-snapshot tables.</summary>
    public const string TimeColumn = "collection_time";

    /// <summary>The two per-snapshot tables, bare names as the catalog reads them. Hypertables where TimescaleDB is present.</summary>
    public static readonly string[] PerSnapshotTables = ["object_usage_snapshots", "object_usage_deltas"];

    /// <summary>
    /// The compression segmentby of both per-snapshot hypertables: the reader's filters are (server_id, database_name), then a time range.
    /// The order-by is TimescaleDB's default, <c>collection_time DESC</c>.
    /// </summary>
    public const string CompressionSegmentBy = "server_id, database_name";

    /// <summary>
    /// The three tables as the V299 rung runs them. Idempotent. The rung embeds this text, so editing it after it ships edits a shipped rung: add a
    /// new rung instead. Plain tables here on every store; the hypertable conversion and the compression settings are applied afterwards by the
    /// service where TimescaleDB is present, as for <c>collection_log</c>, because a migration runs before the extension is created on a fresh
    /// store. Both primary keys lead with the identity and end in <c>collection_time</c> or past it, so they are valid on a hypertable. Needs no
    /// GRANT: the <c>collect</c> schema's blanket SELECT covers a table a migration introduces, and the owner role that runs the service writes it.
    /// </summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.object_usage_snapshots
(
    server_id integer NOT NULL,
    database_name text NOT NULL,
    collection_time timestamp NOT NULL,
    step_version smallint NOT NULL,
    answering_server_name text,
    sqlserver_start_time timestamp,
    node_key text NOT NULL,
    compared_with timestamp,
    kept_start timestamp,
    reasons integer NOT NULL,
    counted smallint NOT NULL,
    index_resets integer NOT NULL,
    blind_from timestamp,
    built_at timestamp NOT NULL,
    CONSTRAINT pk_object_usage_snapshots PRIMARY KEY (server_id, database_name, collection_time)
);

CREATE TABLE IF NOT EXISTS collect.object_usage_deltas
(
    server_id integer NOT NULL,
    database_name text NOT NULL,
    collection_time timestamp NOT NULL,
    object_id integer NOT NULL,
    index_id integer NOT NULL,
    reads bigint NOT NULL,
    updates bigint NOT NULL,
    last_read timestamp,
    last_update timestamp,
    CONSTRAINT pk_object_usage_deltas PRIMARY KEY (server_id, database_name, collection_time, object_id, index_id)
);

CREATE TABLE IF NOT EXISTS collect.object_usage_runs
(
    server_id integer NOT NULL,
    database_name text NOT NULL,
    object_id integer NOT NULL,
    index_id integer NOT NULL,
    first_time timestamp NOT NULL,
    last_time timestamp,
    schema_name text NOT NULL,
    object_name text NOT NULL,
    is_indexed_view boolean NOT NULL,
    is_memory_optimized boolean,
    CONSTRAINT pk_object_usage_runs PRIMARY KEY (server_id, database_name, object_id, index_id, first_time)
);
";
}
