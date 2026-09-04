/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// A collector definition is the shared, engine-neutral "monitoring brain" for one collector:
/// the T-SQL sent to the monitored server, the result-row mapping, the delta rules, and the
/// payload column order. Both SKUs (portable Lite writing DuckDB, Darling writing Postgres)
/// run the SAME definition, so a change lands once and the compiler forces every host to keep up
/// — this is what makes behavioral parity structural rather than manual (headless plan v5.1).
/// Definitions are stateless and thread-safe; per-cycle state rides in <see cref="CollectorContext"/>.
/// </summary>
/// <typeparam name="TRow">The definition's materialized result-row shape.</typeparam>
public interface ICollectorDefinition<TRow> : ICollectorSchemaInfo
{
    /// <summary>
    /// Per-collector command timeout override in seconds, for collectors whose sweep is far
    /// heavier than the default budget (index_object_stats: 300 s per database — the #1135 fix).
    /// Null = the host's default timeout. Applies to the main/per-item/per-database commands;
    /// enumeration queries always use the host default, matching the originals.
    /// </summary>
    int? CommandTimeoutSecondsOverride { get; }

    /* AppliesTo(CollectorTargetInfo) is declared on the base ICollectorSchemaInfo — the single
       authoritative target gate, evaluable by name off CollectorCatalog.All without the row type. */

    /// <summary>
    /// True when the query must run once per database with a per-database connection (Azure SQL
    /// DB scopes some DMVs to the connected database — e.g. dm_io_virtual_file_stats). The host
    /// enumerates databases, opens each connection, and calls <see cref="ReadAsync"/> per reader,
    /// aggregating rows; a database that errors is skipped and logged, matching the original
    /// collectors.
    /// </summary>
    bool RunsPerDatabase(CollectorTargetInfo target);

    /// <summary>
    /// Time column the host should read its latest already-collected value of (from the host's
    /// own store) before building the query — exposed to the definition as
    /// <see cref="CollectorContext.Watermark"/> for server-side filters and client-side dedup.
    /// Null when the collector needs no watermark (the common case).
    /// </summary>
    string? WatermarkColumn { get; }

    /// <summary>
    /// Numeric (bigint) column the host should read its latest already-collected value of (from the
    /// host's own store) before building the query — exposed to the definition as
    /// <see cref="CollectorContext.NumericWatermark"/> for server-side filters and client-side dedup
    /// on a monotonic identity/sequence column (job_history's <c>instance_id</c>). The bigint twin of
    /// <see cref="WatermarkColumn"/>. Null when the collector needs no numeric watermark (the common
    /// case — every existing collector).
    /// </summary>
    string? NumericWatermarkColumn { get; }

    /* StateKeys is declared on the base ICollectorSchemaInfo — like AppliesTo and YieldsOnLockTimeout,
       so the declaring collectors are enumerable off CollectorCatalog.All without the row type. */

    /// <summary>
    /// Table column that scopes <see cref="WatermarkColumn"/> per database when the collector
    /// <see cref="RunsPerDatabase"/> (Azure SQL DB). Non-null means each per-database run gets its
    /// own watermark (<c>MAX(WatermarkColumn) WHERE PerDatabaseWatermarkColumn = db</c>) and the
    /// host rebuilds the query per database — one busy database's newer event can then never
    /// watermark past another database's older event still sitting in its ring buffer (the XE
    /// collectors' per-database sessions dispatch independently). Null (the common case) keeps the
    /// single server-wide watermark and the build-once-per-cycle query. Ignored when
    /// <see cref="RunsPerDatabase"/> is false for the target — the server-wide watermark is already
    /// exact there.
    /// </summary>
    string? PerDatabaseWatermarkColumn { get; }

    /// <summary>
    /// Per-database row count at or above which the host logs a WARNING naming the server and database
    /// (#1556): a per-database read that returns this many rows in a single cycle is producing far
    /// more than a healthy volume and its oldest rows were trimmed by the definition's own server-side
    /// backstop (query_store's <c>TOP</c>). Null (the common case) = no cap, no warning. One const with
    /// the SQL <c>TOP</c>: the runner warns exactly when the definition's own backstop engaged. Applied
    /// on BOTH per-database shapes — the enumerated item loop, and the Azure per-database connection
    /// loop, which has no enumerated item but the same one-command-per-database structure (#1836).
    /// </summary>
    int? PerItemRowCountWarnThreshold { get; }

    /// <summary>
    /// Cumulative per-database TEXT byte budget (#1556): a definition carrying large nvarchar(max)
    /// payloads (query_store's query text + plan XML) enforces this budget CLIENT-SIDE inside its own
    /// read — <see cref="ReadItemAsync"/> on the enumerated path, <see cref="ReadAsync"/> on the Azure
    /// per-database path (#1836) — accumulating the materialized text bytes and stopping the read once
    /// the budget is reached, signalling truncation via <see cref="CollectorContext.PerItemTextBudgetExceeded"/>
    /// so the host surfaces the same WARNING. This is the PRIMARY memory bound: a row COUNT cap does not
    /// bound BYTES (50k rows each carrying a 40KB plan is 2GB), so the byte budget is what keeps peak
    /// allocation bounded. Null (the common case) = no budget. Enforced by the definition, not the host —
    /// only the definition holds the reader.
    /// </summary>
    int? PerItemTextByteBudget { get; }

    /// <summary>
    /// WALL-CLOCK ceiling for one per-database unit of work — the watermark refresh, the command, and the
    /// whole drain (#2150). Null (the common case) = unbounded, exactly as before.
    ///
    /// <para><b>Why the command timeout is not this.</b> <c>CommandTimeout</c> bounds the wait for a network
    /// read, and SqlClient RESETS it on every read that arrives — so a result set that trickles rows
    /// continuously never trips it, however long it takes in total. A 100-minute read under a 30-second
    /// timeout is the documented behaviour, not a bug, which is why the field report in #2150 shows six
    /// per-database passes of up to 99.8 minutes against a 30-second timeout.</para>
    ///
    /// <para><b>What exceeding it means.</b> The item is abandoned and reported as a per-item FAILURE, and
    /// the cycle continues to the next database — the same treatment an offline database gets. Nothing is
    /// silently dropped: a collector with a watermark did not advance it, so the abandoned range is simply
    /// re-read next cycle. For <c>query_store</c> that failure also feeds the #2111 consecutive-failure
    /// count, so the window NARROWS on the next pass instead of retrying the same impossible width — a
    /// bound that converges rather than one that just repeats.</para>
    ///
    /// <para>Host-enforced rather than definition-enforced, unlike the byte budget: only the host owns the
    /// cancellation token and the loop, and the point is to bound the definition's own read.</para>
    /// </summary>
    /* Declared on the base ICollectorSchemaInfo since #2864, alongside AppliesTo and
       YieldsOnLockTimeout and for the same reason: the catalog holds ICollectorSchemaInfo, so a
       host holding only a collector NAME could not ask this question at all. Not restated here,
       because two declarations of one contract are two things to drift. */

    /// <summary>
    /// Builds the T-SQL (and any bound parameters) for this cycle. Constant for most collectors;
    /// target-aware definitions branch on <see cref="CollectorContext.Target"/> and
    /// <see cref="CollectorContext.Watermark"/>.
    /// </summary>
    CollectorQuery BuildQuery(CollectorContext context);

    /// <summary>
    /// Optional second query run best-effort on the same (single-path) connection after
    /// <see cref="ReadAsync"/> — e.g. server_properties' WS5 health probe. Null = none (the
    /// common case). The host isolates its failure: any exception is logged at debug and the
    /// cycle proceeds with the primary rows unchanged, so a supplemental can never fail the
    /// collector. Not executed for per-database collectors.
    /// </summary>
    CollectorQuery? BuildSupplementalQuery(CollectorContext context);

    /// <summary>Merges the supplemental reader's data into the already-read rows.</summary>
    ValueTask ApplySupplementalAsync(List<TRow> rows, DbDataReader reader, CollectorContext context, CancellationToken cancellationToken);

    /// <summary>
    /// True when this definition's batch may return an OPTIONAL trailing (item_name, error_text) result
    /// set AFTER its payload rows, naming items it reached but could not probe (#1851) — the payload
    /// path's half of the probe-failure contract #1837 gave the enumerating collectors. The host reads it
    /// through <see cref="EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync"/> once
    /// <see cref="ReadAsync"/> returns, summarizing it onto the run's collection_log row and logging the
    /// per-item errors capped. Set by a definition whose own server-side cursor would otherwise discard
    /// per-database failures in a CATCH, leaving a database silently absent from a SUCCESS row.
    ///
    /// <para>False (the common case) means the host never advances the reader past the payload — the
    /// pre-#1851 behavior exactly. The declaration is required rather than inferred because a payload
    /// reader can legitimately carry several result sets that belong to the definition itself
    /// (<c>tempdb_stats</c> reads two), and a trailing-set read must never mistake one of those for
    /// failures. An enumeration needs no such flag: its first result set is a bare item list, so anything
    /// after it can only be the failure set.</para>
    ///
    /// <para>Declaring it does NOT oblige every run to produce the set. A declaring collector that
    /// returns only its payload reads as zero failures and no note, which is what lets one definition
    /// cover a shape that only some targets take — <c>database_size_stats</c> emits the set from its
    /// on-prem cursor and runs a single cursor-less query on Azure SQL DB.</para>
    ///
    /// <para>Not consulted on the per-database or enumeration paths: those read through their own
    /// contracts.</para>
    /// </summary>
    bool EmitsProbeFailures { get; }

    /// <summary>
    /// Optional enumeration shape (the "[db].sys.sp_executesql" idiom): when non-null, the host
    /// runs this query first (single string column, e.g. database names), then executes
    /// <see cref="BuildPerItemQuery"/> once per item ON THE SAME CONNECTION, feeding each reader
    /// to <see cref="ReadItemAsync"/>. An item whose query fails with a SqlException is skipped
    /// with a warning, matching the original collectors. Zero items short-circuits the cycle.
    /// <see cref="ReadAsync"/> is not called for enumerating collectors.
    /// </summary>
    CollectorQuery? BuildEnumerationQuery(CollectorContext context);

    /// <summary>
    /// Optional quick scalar probe on the enumeration path, run once after items are listed
    /// (only when at least one item exists) and before the per-item loop — e.g. query_store's
    /// live PRODUCTVERSION check that decides its version-gated columns (deliberately probed
    /// per cycle rather than trusting cached connection status, which can be version-unknown).
    /// The host runs it best-effort with a short timeout and exposes the scalar as
    /// <see cref="CollectorContext.EnumerationProbeResult"/>; on any failure the result stays
    /// null and the definition uses its documented default. Null = no probe (the common case).
    /// </summary>
    CollectorQuery? BuildEnumerationProbe(CollectorContext context);

    /// <summary>Builds the per-item query for one enumerated item (e.g. one database).</summary>
    CollectorQuery BuildPerItemQuery(string item, CollectorContext context);

    /// <summary>Reads one enumerated item's result rows, appending to the shared accumulator.</summary>
    ValueTask ReadItemAsync(string item, DbDataReader reader, List<TRow> rows, CollectorContext context, CancellationToken cancellationToken);

    /// <summary>
    /// Materializes result rows from the query's reader, applying any definition-owned filtering.
    /// Runs entirely in the SQL phase so hosts can time SQL and storage phases separately.
    /// </summary>
    ValueTask<List<TRow>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken);

    /// <summary>
    /// Emits one row's payload through the writer in <see cref="PayloadColumns"/> order,
    /// computing any deltas via <see cref="CollectorContext.Deltas"/>.
    /// </summary>
    void WritePayload(TRow row, ICollectorRowWriter writer, CollectorContext context);
}
