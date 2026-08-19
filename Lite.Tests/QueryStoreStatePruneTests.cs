/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Real-DuckDB pins for #2188 on Lite's side — retiring the per-database <c>collector_state</c> rows
/// query_store leaves behind for databases the server no longer has.
///
/// <para><b>Why Lite is affected at all</b>, which the first cut of #2188 got wrong: Lite writes no
/// <c>planwm:</c> (it never sets <c>CollectorContext.CapturePlanXml</c>), but its own backfill worker writes
/// <c>done:</c> and <c>hole:</c> per database, and it only ever deletes a hole it SERVICES or expires. A
/// dropped database can do neither — its hole can never be dug and its tail can never drain — so its markers
/// were kept forever, the identical defect the issue reported against Darling's watermark.</para>
///
/// <para><b>What has to be right is the input, not the delete.</b> query_store's enumeration is filtered by
/// ONLINE state, AG primary-ness, exclusions, a vendor-name screen and <c>HAS_DBACCESS</c>, so pruning on
/// absence from it would delete LIVE state for a database that is merely parked. The prune reads
/// <c>database_states</c> — the unfiltered <c>sys.databases</c> snapshot — which is why
/// <see cref="Prune_KeepsAParkedDatabase_ThatNoEnumerationWouldReturn"/> is the load-bearing test here rather
/// than the delete case.</para>
///
/// <para>Real DuckDB rather than a mock, for the reason its siblings in this project give: the whole change
/// is one SQL statement, and DuckDB's dialect is where it can be wrong (<c>starts_with</c>, the NULL-valued
/// MAX over an empty snapshot, <c>NOT IN</c> against a subquery). None of that is visible above the store.</para>
/// </summary>
public sealed class QueryStoreStatePruneTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 21880;
    private const int NeighborServerId = 21881;

    /// <summary>The snapshot's collection_time; state rows are dated relative to it.</summary>
    private static readonly DateTime Newest = new(2026, 8, 11, 9, 0, 0, DateTimeKind.Unspecified);

    private static readonly DateTime BeforeNewest = Newest.AddHours(-1);

    private readonly DuckDbInitializer _duckDb;
    private readonly Pruner _pruner;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public QueryStoreStatePruneTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _pruner = new Pruner(fixture.DuckDb);
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>Exposes the runner's protected prune and state accessors; only <c>_duckDb</c> is exercised.</summary>
    private sealed class Pruner(DuckDbInitializer duckDb, ILogger<RemoteCollectorService>? logger = null)
        : RemoteCollectorService(duckDb, serverManager: null!, scheduleManager: null!, logger)
    {
        public Task PruneAsync(int serverId) =>
            PruneOrphanedQueryStoreDatabaseStateAsync(serverId, CancellationToken.None);

        /// <summary>The #2191 Azure arm, which prunes against the registration's own database rather than
        /// against a database_states snapshot.</summary>
        public Task PruneForeignAsync(int serverId, string ownDatabase) =>
            PruneForeignQueryStoreDatabaseStateAsync(serverId, ownDatabase, CancellationToken.None);
    }

    /// <summary>
    /// Captures formatted log lines so the prune's DIAGNOSTIC can be asserted, not just its effect.
    /// <para>#2205: correctness already matched Darling exactly, and the gap was forensic — Lite logged a
    /// COUNT where Darling names the retired keys, so on Lite you could see that three rows went without
    /// seeing WHICH databases' watermark and backfill state was retired. A count cannot be told apart from
    /// a mistaken prune of the same size, which is the case an operator most needs to see.</para>
    /// </summary>
    private sealed class CapturingLogger : ILogger<RemoteCollectorService>
    {
        public List<string> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Lines.Add(formatter(state, exception));
    }

    private static string Done(string database) => QueryStoreBackfillState.DoneKeyPrefix + database;
    private static string Hole(string database) => QueryStoreBackfillState.HoleKeyPrefix + database;

    private static string EncodedHole() => QueryStoreBackfillState.EncodeHole(
        new DateTime(2026, 8, 10, 0, 0, 0, DateTimeKind.Utc),
        new DateTime(2026, 8, 10, 6, 0, 0, DateTimeKind.Utc));

    [Fact]
    public async Task Prune_RetiresTheBackfillMarkersOfDroppedDatabases_AndKeepsTheRest()
    {
        await SeedSnapshotAsync(Newest, "Live", "App");

        /* An OLDER snapshot still naming the dropped databases: if the prune read any snapshot but the
           newest, nothing would ever be retired. */
        await SeedSnapshotAsync(Newest.AddMinutes(-15), "Live", "App", "Dropped", "AppArchive");

        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("Live"), "2026-08-11T09:00:00Z");
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Hole("Live"), EncodedHole());
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("Dropped"), "2026-08-11T09:00:00Z");
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Hole("Dropped"), EncodedHole());

        /* The name-shape trap: writing the anti-join with a prefix match instead of an equality would spare
           AppArchive forever, because "done:App" is a prefix of "done:AppArchive". For an issue whose whole
           subject is database name churn, that case belongs here. */
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("AppArchive"), "2026-08-11T09:00:00Z");

        /* Not database-keyed: the prefix filter is what protects it from a prune written as "every key of
           this collector". */
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, "unrelated-bookkeeping", "keep me");

        /* Another collector, and another server whose database really was dropped here — server scoping is
           the difference between pruning one server and pruning every server in the file. */
        await SeedStateAsync(ServerId, DefaultTraceEventsCollector.Instance.Name,
            DefaultTraceEventsCollector.LastTraceFilePathStateKey, @"S:\MSSQL\Log\log_766.trc");
        await SeedStateAsync(NeighborServerId, QueryStoreBackfillState.StateCollectorName, Done("Dropped"), "2026-08-11T09:00:00Z");

        await _pruner.PruneAsync(ServerId);

        Assert.Null(await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("Dropped")));
        Assert.Null(await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Hole("Dropped")));
        Assert.Null(await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("AppArchive")));

        Assert.Equal("2026-08-11T09:00:00Z", await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("Live")));
        Assert.Equal(EncodedHole(), await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Hole("Live")));
        Assert.Equal("keep me", await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, "unrelated-bookkeeping"));
        Assert.Equal(@"S:\MSSQL\Log\log_766.trc", await ValueAsync(ServerId, DefaultTraceEventsCollector.Instance.Name,
            DefaultTraceEventsCollector.LastTraceFilePathStateKey));
        Assert.Equal("2026-08-11T09:00:00Z", await ValueAsync(NeighborServerId, QueryStoreBackfillState.StateCollectorName, Done("Dropped")));

        /* Idempotent — it runs on every query_store cycle, so a second pass must touch nothing. */
        await _pruner.PruneAsync(ServerId);
        Assert.Equal(4L, await CountAsync(ServerId));
    }

    [Fact]
    public async Task Prune_KeepsAParkedDatabase_ThatNoEnumerationWouldReturn()
    {
        /* The assertion this change exists for. Parked is OFFLINE, so query_store's enumeration (which
           screens state_desc = ONLINE) never returns it — but it is still in sys.databases and still very
           much a database. A prune keyed on the enumeration deletes its state and silently costs a full
           re-drain of its history; a prune keyed on sys.databases leaves it alone. */
        await SeedSnapshotAsync(Newest, ("Live", "ONLINE"), ("Parked", "OFFLINE"));

        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("Parked"), "2026-08-11T09:00:00Z");
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Hole("Parked"), EncodedHole());

        await _pruner.PruneAsync(ServerId);

        Assert.Equal("2026-08-11T09:00:00Z", await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("Parked")));
        Assert.Equal(EncodedHole(), await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Hole("Parked")));
    }

    [Fact]
    public async Task Prune_WithNoSnapshot_RetiresNothing()
    {
        /* The guard that stops a hygiene sweep becoming a data event. DuckDB's MAX over zero rows is NULL
           and `updated_at < NULL` is NULL, so the delete matches nothing — but that is a property of the
           dialect, not of the intent, which is exactly why it is pinned against a real store.

           A server with no snapshot is reachable in ordinary use: Lite never collects database_states on
           Azure SQL DB, and a store whose rows have been archived out looks the same from here. */
        await SeedSnapshotAsync(Newest, NeighborServerId, ("SomeOtherServersDatabase", "ONLINE"));

        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("Alpha"), "a");
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Hole("Beta"), EncodedHole());

        await _pruner.PruneAsync(ServerId);

        Assert.Equal("a", await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("Alpha")));
        Assert.Equal(EncodedHole(), await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Hole("Beta")));
    }

    [Fact]
    public async Task Prune_LeavesStateWrittenAfterTheSnapshot()
    {
        /* Existing is not CURRENT. If database_states stops collecting, its newest snapshot freezes, and
           every database created after that instant is missing from it while being perfectly alive —
           pruning on presence alone would retire such a database's markers on every cycle forever. A
           snapshot cannot judge a row written after it was taken. */
        await SeedSnapshotAsync(Newest, "OldDb");

        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName,
            Done("BornAfterTheSnapshot"), "new", updatedAt: Newest.AddMinutes(30));

        /* And the control: dropped BEFORE the snapshot froze, so its last write precedes it and it is still
           prunable. Without this the test would also pass for a prune that had simply stopped working. */
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName,
            Done("DroppedLongAgo"), "old", updatedAt: BeforeNewest);

        await _pruner.PruneAsync(ServerId);

        Assert.Equal("new", await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("BornAfterTheSnapshot")));
        Assert.Null(await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("DroppedLongAgo")));
    }

    [Fact]
    public async Task Prune_RunsTheWatermarkStatementToo_EvenThoughLiteWritesNone()
    {
        /* Lite iterates the SHARED QueryStorePerDatabaseState.PrunableKeys, which carries planwm: even
           though Lite never writes it. That is deliberate: the day plan capture is enabled here, the prune
           is already in place rather than being a thing somebody has to remember. Today it must simply be
           harmless — one delete matching nothing — which is what this checks by proving a planted planwm:
           row for a DROPPED database is retired by the same pass. */
        await SeedSnapshotAsync(Newest, "Live");
        await SeedStateAsync(ServerId, QueryStorePlanXmlState.StateCollectorName,
            QueryStorePlanXmlState.WatermarkKeyPrefix + "Dropped", "900000:1786449600");

        await _pruner.PruneAsync(ServerId);

        Assert.Null(await ValueAsync(ServerId, QueryStorePlanXmlState.StateCollectorName,
            QueryStorePlanXmlState.WatermarkKeyPrefix + "Dropped"));
    }

    /* ---------------- helpers ---------------- */

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private Task SeedSnapshotAsync(DateTime when, params string[] databases)
        => SeedSnapshotAsync(when, ServerId, Array.ConvertAll(databases, db => (db, "ONLINE")));

    private Task SeedSnapshotAsync(DateTime when, params (string Db, string State)[] databases)
        => SeedSnapshotAsync(when, ServerId, databases);

    private async Task SeedSnapshotAsync(DateTime when, int serverId, params (string Db, string State)[] databases)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        var dbId = 1;
        foreach (var (db, state) in databases)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby)
VALUES ($1, $2, $3, $4, $5, $6, $7, false)";
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
            cmd.Parameters.Add(new DuckDBParameter { Value = when });
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = "LITE-PRUNE-SRV" });
            cmd.Parameters.Add(new DuckDBParameter { Value = db });
            cmd.Parameters.Add(new DuckDBParameter { Value = dbId++ });
            cmd.Parameters.Add(new DuckDBParameter { Value = state });
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task SeedStateAsync(int serverId, string owner, string key, string value, DateTime? updatedAt = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT OR REPLACE INTO collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = owner });
        cmd.Parameters.Add(new DuckDBParameter { Value = key });
        cmd.Parameters.Add(new DuckDBParameter { Value = value });
        cmd.Parameters.Add(new DuckDBParameter { Value = updatedAt ?? BeforeNewest });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<string?> ValueAsync(int serverId, string owner, string key)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText =
            "SELECT state_value FROM collector_state WHERE server_id = $1 AND collector_name = $2 AND state_key = $3";
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = owner });
        cmd.Parameters.Add(new DuckDBParameter { Value = key });
        var value = await cmd.ExecuteScalarAsync();
        return value is DBNull or null ? null : (string)value;
    }

    private async Task<long> CountAsync(int serverId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM collector_state WHERE server_id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// #2205: the log line must NAME the pruned keys, matching Darling's wording field for field.
    ///
    /// <para>Watched red before it went green: with the previous <c>ExecuteNonQueryAsync</c> +
    /// counter, the message read "Pruned 2 query_store state row(s)…" and contained neither key, so both
    /// key assertions failed while the row-level assertions passed — which is exactly the shape of the
    /// defect. The <c>DELETE … RETURNING</c> it now depends on was verified against real DuckDB 1.5.5
    /// before the change, using the shipped SQL string rather than a copy.</para>
    /// </summary>
    [Fact]
    public async Task Prune_LogNamesTheRetiredKeys_NotJustACount()
    {
        var logger = new CapturingLogger();
        var pruner = new Pruner(_duckDb, logger);

        await SeedSnapshotAsync(Newest, "Live");
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("Live"), "2026-08-11T09:00:00Z");
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("DroppedOne"), "2026-08-11T09:00:00Z");
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Hole("DroppedTwo"), EncodedHole());

        await pruner.PruneAsync(ServerId);

        var line = Assert.Single(logger.Lines, l => l.Contains("pruned", StringComparison.OrdinalIgnoreCase));

        /* The keys themselves — the whole point of the change. */
        Assert.Contains(Done("DroppedOne"), line, StringComparison.Ordinal);
        Assert.Contains(Hole("DroppedTwo"), line, StringComparison.Ordinal);

        /* And it must not name a database that was NOT pruned, or the log is worse than a count. */
        Assert.DoesNotContain("Live", line, StringComparison.Ordinal);

        /* Same fields as DarlingCollectorRunner's line, so one sentence serves both SKUs. */
        Assert.Contains($"[server_id {ServerId}]", line, StringComparison.Ordinal);
        Assert.Contains("2 query_store state row(s)", line, StringComparison.Ordinal);
    }
    /* ─────────────── #2191: the Azure arm, pruned against the registration's own database ─────────────── */

    /// <summary>
    /// The Azure prune keeps the registration's own database and retires every sibling — executed against
    /// real DuckDB, so this covers the statement itself (<c>starts_with</c> plus
    /// <c>state_key &lt;&gt; prefix || own</c>) rather than its text.
    ///
    /// <para>These keys are what #2220 left behind: before that fix each Azure registration swept every
    /// sibling database on the logical server and wrote a watermark for it under its OWN server_id. And
    /// <c>collector_state</c> carries no retention, so they would persist indefinitely rather than ageing out
    /// with the collected rows.</para>
    /// </summary>
    [Fact]
    public async Task ForeignPrune_KeepsThisRegistrationsDatabase_AndRetiresItsSiblings()
    {
        /* Deliberately NO database_states snapshot: an Azure server never has one, and the point of this arm
           is that it does not need one. */
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("Payments"), "2026-08-11T09:00:00Z");
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Hole("Payments"), EncodedHole());
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("Sibling-A"), "2026-08-11T09:00:00Z");
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Hole("Sibling-B"), EncodedHole());

        /* The prefix trap, same one the on-prem test carries: an inequality written as a prefix comparison
           would spare "PaymentsArchive" because "Payments" is a prefix of it. */
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("PaymentsArchive"), "2026-08-11T09:00:00Z");

        /* Not database-keyed, and another collector, and another server — all three must survive. */
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, "unrelated-bookkeeping", "keep me");
        await SeedStateAsync(ServerId, DefaultTraceEventsCollector.Instance.Name,
            DefaultTraceEventsCollector.LastTraceFilePathStateKey, @"S:\MSSQL\Log\log_766.trc");
        await SeedStateAsync(NeighborServerId, QueryStoreBackfillState.StateCollectorName, Done("Sibling-A"), "2026-08-11T09:00:00Z");

        await _pruner.PruneForeignAsync(ServerId, "Payments");

        Assert.Null(await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("Sibling-A")));
        Assert.Null(await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Hole("Sibling-B")));
        Assert.Null(await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("PaymentsArchive")));

        Assert.Equal("2026-08-11T09:00:00Z", await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("Payments")));
        Assert.Equal(EncodedHole(), await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Hole("Payments")));
        Assert.Equal("keep me", await ValueAsync(ServerId, QueryStoreBackfillState.StateCollectorName, "unrelated-bookkeeping"));
        Assert.Equal(@"S:\MSSQL\Log\log_766.trc", await ValueAsync(ServerId, DefaultTraceEventsCollector.Instance.Name,
            DefaultTraceEventsCollector.LastTraceFilePathStateKey));
        Assert.Equal("2026-08-11T09:00:00Z", await ValueAsync(NeighborServerId, QueryStoreBackfillState.StateCollectorName, Done("Sibling-A")));

        /* Idempotent: it runs every query_store cycle, so a second pass must touch nothing. */
        await _pruner.PruneForeignAsync(ServerId, "Payments");
        Assert.Equal(4L, await CountAsync(ServerId));
    }

    /// <summary>
    /// An empty own-database short-circuits BEFORE the statement runs, deleting nothing.
    ///
    /// <para>This is the arm's safety property rather than an edge case. A registration naming no database is
    /// a registration of the logical SERVER, whose legitimate database set is everything on it — so running
    /// the single-name prune there would delete every live watermark it has. The caller gates on
    /// <c>AzureSweepScope</c>, and the method guards again itself, because the consequence of getting it
    /// wrong is silent and total.</para>
    /// </summary>
    [Theory]
    [InlineData("")]
    public async Task ForeignPrune_WithNoOwnDatabase_RetiresNothing(string ownDatabase)
    {
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("Payments"), "2026-08-11T09:00:00Z");
        await SeedStateAsync(ServerId, QueryStoreBackfillState.StateCollectorName, Done("Sibling-A"), "2026-08-11T09:00:00Z");

        await _pruner.PruneForeignAsync(ServerId, ownDatabase);

        Assert.Equal(2L, await CountAsync(ServerId));
    }

    /// <summary>
    /// Every per-database prefix is pruned, not just the backfill ones — the watermark prefix included, even
    /// though Lite writes none today (it never sets <c>CapturePlanXml</c>). Pinned for the same reason the
    /// on-prem twin pins it: the shared prefix list is what stops a prefix being pruned on one SKU and
    /// orphaning on the other, and a Lite-only omission would be invisible on Darling.
    /// </summary>
    [Fact]
    public async Task ForeignPrune_CoversTheWatermarkPrefixToo()
    {
        var foreignWatermark = QueryStorePlanXmlState.KeyFor("Sibling-A");
        var ownWatermark = QueryStorePlanXmlState.KeyFor("Payments");

        await SeedStateAsync(ServerId, QueryStorePlanXmlState.StateCollectorName, foreignWatermark, "8140");
        await SeedStateAsync(ServerId, QueryStorePlanXmlState.StateCollectorName, ownWatermark, "8150");

        await _pruner.PruneForeignAsync(ServerId, "Payments");

        Assert.Null(await ValueAsync(ServerId, QueryStorePlanXmlState.StateCollectorName, foreignWatermark));
        Assert.Equal("8150", await ValueAsync(ServerId, QueryStorePlanXmlState.StateCollectorName, ownWatermark));
    }
}
