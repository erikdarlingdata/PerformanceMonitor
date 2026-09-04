/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The V109 rung - what an abandoned collection cycle was DOING (#2864).
///
/// <para><b>Why it exists.</b> A cycle the #2673 wall-clock budget abandons records <c>ABANDONED</c> with
/// <c>rows_collected = 0</c>, and that zero is rows STORED - which an abandoned cycle never does. So the row
/// could not separate a target that sent nothing from one that sent 149 rows and went silent: a stalled
/// target and a stalled stream, wanting different fixes. V108 made the phases queryable and the first
/// production capture read <c>open:104ms drain:119,945ms rows=0</c>, proving the time was in the drain and
/// unable to say whether the drain was slow or empty.</para>
/// </summary>
public class CollectionLogDrainForensicsStoreTests
{
    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("collection-log-drain-forensics", PgMigrations.Scripts.Single(s => s.Version == 109).Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(versions.Count, versions.Distinct().Count());
    }

    /// <summary>
    /// The rung adds all five columns AND refreshes the passthrough view. The view half is not decoration:
    /// Postgres freezes a view's <c>SELECT *</c> column list at CREATE, so without the refresh an UPGRADED
    /// store would keep serving the pre-V109 list forever while a fresh one worked - the V14/V80/V108 lesson,
    /// and the failure mode that is invisible until someone upgrades rather than installs.
    /// </summary>
    [Fact]
    public void TheRungAddsEveryColumnAndRefreshesThePassthroughView()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 109).Sql;

        foreach (var column in new[]
                 {
                     "drain_rows_read", "drain_bytes_read", "drain_last_read_ms",
                     "target_session_id", "sweep_peer_max_ms",
                 })
        {
            Assert.Contains(column, sql, StringComparison.Ordinal);
        }

        Assert.Contains("ADD COLUMN IF NOT EXISTS", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW collect.v_collection_log", sql, StringComparison.Ordinal);

        /* Nullable with no DEFAULT, deliberately: a catalog-only change stays instant on a large compressed
           hypertable, and a row written before this rung genuinely does not know any of this. A DEFAULT 0
           would claim a measured drain that delivered nothing on every historical row. */
        Assert.DoesNotContain("NOT NULL", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DEFAULT", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The viewer's connect-time gate must map a fully-migrated store to EXACTLY this rung. A sentinel added
    /// without its own arm would leave a V109 store reporting 108 and the viewer showing an upgrade banner
    /// against a store that is current.
    /// </summary>
    [Fact]
    public void TheViewerProbeSentinelAndTopArmAreBothPresent()
    {
        var viewer = ReadSource("Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.cs");

        Assert.Contains("column_name = 'drain_last_read_ms'", viewer, StringComparison.Ordinal);
        Assert.Contains("reader.GetBoolean(84)", viewer, StringComparison.Ordinal);
        Assert.Contains("hasCollectionLogDrainForensics", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var all = Enumerable.Repeat((object)true, method.GetParameters().Length).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);
    }

    /// <summary>The MCP read has to actually select the columns, or the rung is invisible where it is read.</summary>
    [Fact]
    public void TheMcpCollectionLogReadSelectsTheForensics()
    {
        foreach (var column in new[]
                 {
                     "drain_rows_read", "drain_bytes_read", "drain_last_read_ms",
                     "target_session_id", "sweep_peer_max_ms",
                 })
        {
            Assert.Contains(column, DarlingDataReader.CollectionLogSql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The counting reader reports what the drain delivered. This is the whole mechanism: 66 collectors write
    /// their own read loop, so the count comes from decorating the reader rather than editing every one.
    /// </summary>
    [Fact]
    public async Task TheCountingReaderRecordsRowsBytesAndTheLastReadReading()
    {
        var watch = Stopwatch.StartNew();
        var counting = new DrainCountingDataReader(new FakeRowReader(["alpha", "beta"]), watch);

        Assert.Equal(-1, counting.LastReadElapsedMs);
        Assert.Equal(0, counting.RowsRead);

        while (await counting.ReadAsync(CancellationToken.None))
        {
            _ = counting.GetString(0);
        }

        Assert.Equal(2, counting.RowsRead);

        /* UTF-16 bytes off the string getter: "alpha" + "beta" = 9 chars = 18 bytes. Not the wire size, and
           the column name says so - see the type's remarks for why the honest scope is also the useful one. */
        Assert.Equal(18, counting.PayloadBytes);

        Assert.True(counting.LastReadElapsedMs >= 0, "a successful read must stamp the drain reading");
    }

    /// <summary>
    /// A reader that returns no row leaves the last-read reading at -1, NOT 0.
    ///
    /// <para>This is the distinction the whole change turns on. 0 is a real, reachable answer - row 1 arrived
    /// instantly - so a sentinel that collided with it would let "nothing ever arrived" read as "everything
    /// arrived at once", which is the opposite diagnosis. The production capture this was written for read
    /// zero rows across a 119,945 ms drain.</para>
    /// </summary>
    [Fact]
    public async Task ADrainThatDeliveredNothingIsNotADrainThatDeliveredInstantly()
    {
        var counting = new DrainCountingDataReader(new FakeRowReader([]), Stopwatch.StartNew());

        Assert.False(await counting.ReadAsync(CancellationToken.None));
        Assert.Equal(0, counting.RowsRead);
        Assert.Equal(-1, counting.LastReadElapsedMs);
        Assert.NotEqual(0, counting.LastReadElapsedMs);
    }

    /// <summary>
    /// The worker classifies a run it has only the NAME of, so the "is this one of the heavy budgeted
    /// collectors" question is answered from the DEFINITION rather than a name list kept beside it. A list
    /// would be correct today and silently wrong the moment a fifth collector earned a budget.
    /// </summary>
    [Fact]
    public void TheBudgetedHeaviesAreDerivedFromTheCatalogNotAHardcodedList()
    {
        Assert.True(CollectorCatalog.HasWallClockBudget(ProcedureStatsCollector.Instance.Name));
        Assert.True(CollectorCatalog.HasWallClockBudget(QueryStatsCollector.Instance.Name));

        /* wait_stats is the archetypal peer: it is what "were the ordinary collectors in this body slow too"
           is asking about, and it must never be excluded from the mark as though it were a heavy. */
        Assert.False(CollectorCatalog.HasWallClockBudget("wait_stats"));

        /* An unknown name is not one of the budgeted heavies, and reading it as light degrades gracefully. */
        Assert.False(CollectorCatalog.HasWallClockBudget("no_such_collector"));
    }

    /// <summary>
    /// The forensics travel as ONE value gated on the same MEASURED flag as the phases, so a caller cannot
    /// persist a row count without the last-read reading that makes it meaningful.
    /// </summary>
    [Fact]
    public void TheForensicsAreAbsentUnlessThePathMeasuredThem()
    {
        Assert.Null(new CollectorRunResult(0, 0, 0).Drain);

        var measured = new CollectorRunResult(
            0, 120051, 0, Abandoned: true, ServerPhasesMeasured: true,
            ServerOpenMs: 104, ServerDrainMs: 119945,
            ServerRowsRead: 0, ServerBytesRead: 0, ServerLastReadMs: -1, TargetSessionId: 77);

        var drain = Assert.NotNull(measured.Drain) is var _ ? measured.Drain!.Value : default;
        Assert.Equal(0, drain.RowsRead);
        Assert.Equal(-1, drain.LastReadMs);
        Assert.Equal(77, drain.TargetSessionId);
    }

    /// <summary>
    /// The in-memory -1 sentinel must never reach the store as a literal (#2864 review).
    ///
    /// <para>The reachable case is not exotic: the wall-clock budget can fire INSIDE
    /// <c>ExecuteReaderAsync</c>, before the counting reader is constructed at all. The abandon arm then
    /// returns with the phases MEASURED (the open was stamped from its own finally) but the counts still at
    /// their -1 default. Unguarded, that writes a literal -1 into a bigint column and breaks this rung's own
    /// documented invariant - that a stored count is always a real non-negative number, and -1 is a value no
    /// real count can take. Every figure is guarded, not just the last-read one.</para>
    /// </summary>
    [Fact]
    public void TheUnmeasuredSentinelIsNeverWrittenAsALiteral()
    {
        /* Exactly the shape the abandon-during-open path produces. */
        var openStall = new CollectorRunResult(
            0, 120051, 0, Abandoned: true, ServerPhasesMeasured: true,
            ServerOpenMs: 120051, ServerDrainMs: 0);

        var drain = openStall.Drain!.Value;
        Assert.Equal(-1, drain.RowsRead);
        Assert.Equal(-1, drain.BytesRead);
        Assert.Equal(-1, drain.LastReadMs);

        /* Each of the three is guarded independently at the write, so none can leak. */
        var writer = ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingObservability.cs");
        foreach (var guard in new[]
                 {
                     "drain.Value.RowsRead >= 0", "drain.Value.BytesRead >= 0", "drain.Value.LastReadMs >= 0",
                     /* #2884: the session-id twin. 0 is the provider's not-populated state, never a real
                        SPID or backend pid, and unguarded it wrote as a real-looking id no join could land. */
                     "drain.Value.TargetSessionId is int spid && spid > 0",
                 })
        {
            Assert.Contains(guard, writer, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The session id is captured AFTER the open round trip, exactly once, and the helper normalizes the
    /// provider's not-populated 0 to null (#2884).
    ///
    /// <para>SqlConnection.ServerProcessId is not reliably populated until the connection has round-tripped
    /// a command. The original capture sat before ExecuteReaderAsync, so the runs it mattered most for —
    /// budget-abandoned cycles, where the id is the join key to waiting_tasks and the peer snapshots —
    /// recorded a literal 0. Ordering is pinned on the source because it IS a source property: the capture
    /// call must come after the ExecuteReaderAsync await it depends on, and a refactor that hoists it back
    /// above the open reintroduces #2884 while compiling clean and passing every behavioral test that
    /// cannot construct a real SqlConnection.</para>
    /// </summary>
    [Fact]
    public void TheSessionIdIsCapturedAfterTheOpenRoundTrip()
    {
        var runner = ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs");

        const string openCall = "opened = await command.ExecuteReaderAsync(";
        const string captureCall = "context.TargetSessionId = TryReadTargetSessionId(";

        var open = runner.IndexOf(openCall, StringComparison.Ordinal);
        var capture = runner.IndexOf(captureCall, StringComparison.Ordinal);

        Assert.True(open >= 0, "the server-scoped open call moved; re-anchor this pin");
        Assert.True(capture >= 0, "the session-id capture is gone entirely");
        Assert.True(capture > open,
            "the session id must be captured AFTER ExecuteReaderAsync has round-tripped (#2884) — " +
            "before it, SqlConnection.ServerProcessId reads 0 on exactly the abandoned cycles it exists to explain");
        Assert.Equal(capture, runner.LastIndexOf(captureCall, StringComparison.Ordinal));

        /* The helper's own normalization: 0 is not a session id and must become null at the source. */
        Assert.Contains("return raw > 0 ? raw : null;", runner, StringComparison.Ordinal);
    }

    /// <summary>
    /// A string read through the indexer counts once, not zero times (#2864 review). "A collector cannot
    /// forget to count" is this decorator's whole claim, and forwarding the indexer straight to the inner
    /// reader would have made it quietly false for the first collector that used the idiomatic syntax.
    /// </summary>
    [Fact]
    public async Task TheIndexerCountsTheSameAsTheTypedGetter()
    {
        var byOrdinal = new DrainCountingDataReader(new FakeRowReader(["abcd"]), Stopwatch.StartNew());
        await byOrdinal.ReadAsync(CancellationToken.None);
        _ = byOrdinal[0];
        Assert.Equal(8, byOrdinal.PayloadBytes);

        var byName = new DrainCountingDataReader(new FakeRowReader(["abcd"]), Stopwatch.StartNew());
        await byName.ReadAsync(CancellationToken.None);
        _ = byName["value"];
        Assert.Equal(8, byName.PayloadBytes);
    }

    /// <summary>
    /// The peer mark is captured at DISPATCH and handed to the run, never re-read at completion.
    ///
    /// <para><c>query_store</c> and <c>plan_correction</c> are dispatched fire-and-forget and run for
    /// 100-230s, while the 15s sweep resets and rebuilds the mark several times over. Reading it at
    /// completion attributes those rows to an unrelated later tick - and those two are among the very
    /// heavies this diagnostic exists to explain, so the late read is wrong exactly where it matters most.</para>
    /// </summary>
    [Fact]
    public void ThePeerMarkIsCapturedAtDispatchNotAtCompletion()
    {
        var worker = ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs");

        Assert.Contains("var peerMaxAtDispatchMs = PeerMaxOrNull(server);", worker, StringComparison.Ordinal);
        Assert.Contains("RunDetachedAsync(server, runner, name, peerMaxAtDispatchMs, cancellationToken)", worker, StringComparison.Ordinal);

        /* The write uses the captured parameter. Re-reading live server state here is the defect. */
        Assert.Contains("result.Drain, peerMaxAtDispatchMs, _logger", worker, StringComparison.Ordinal);
        Assert.DoesNotContain("result.Drain, PeerMaxOrNull(server)", worker, StringComparison.Ordinal);

        /* The two callers that are not a scheduled body pass null rather than folding a previous body's
           bookkeeping into their rows. */
        Assert.Equal(2, Regex.Matches(worker, @"peerMaxAtDispatchMs: null").Count);

        /* Every LogCollectionAsync inside RunOneAsync carries the body's mark, including the early-return
           YIELDED / PERMISSIONS / ERROR / SESSION_MISSING arms (#2864 review). Those are exactly the rows
           where "were this body's other collectors also slow" is useful - a lock-timeout yield during a
           sweep-wide slowdown is a different finding from one on a healthy body - and the column's own
           rationale is that a ratio needs a denominator from ordinary rows, not only from failures. */
        Assert.DoesNotContain("sweepPeerMaxMs: null", worker, StringComparison.Ordinal);

        /* drain STAYS null on those arms: nothing was drained, so there is nothing to describe. */
        Assert.Equal(7, Regex.Matches(worker, @"drain: null").Count);
    }

    /// <summary>
    /// NULL means NOT RECORDED and nothing more - pinned because the first version of this rung claimed more
    /// than that, in three places at once (#2864 review).
    ///
    /// <para>The claim was that a NULL count beside a NULL last-read identifies a row written before the
    /// rung. It is false in two reachable ways. An abandon firing inside <c>ExecuteReaderAsync</c> never
    /// constructs the counting reader, so a genuine V109 row guards all three to NULL - the case
    /// <see cref="TheUnmeasuredSentinelIsNeverWrittenAsALiteral"/> constructs directly. And no per-database
    /// ENUMERATED collector sets the measured flag at all, so <c>query_store</c> and the <c>Pg*Stats</c>
    /// family read NULL forever on a fully current store. A dashboard built on the claim would misclassify
    /// an open-stall and whole collector families as "no data".</para>
    /// </summary>
    [Fact]
    public void NullMeansNotRecordedAndTheDocsDoNotClaimMore()
    {
        foreach (var file in new[]
                 {
                     "Darling/PerformanceMonitor.Darling.Service/DarlingObservability.cs",
                     "Darling/PerformanceMonitor.Darling.Storage/PgMigrations.cs",
                     "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingDataReader.cs",
                 })
        {
            var source = ReadSource(file);
            var v109 = source[source.IndexOf("2864", StringComparison.Ordinal)..];

            Assert.DoesNotContain("predates this rung", v109, StringComparison.Ordinal);
            Assert.DoesNotContain("predates the rung", v109, StringComparison.Ordinal);
        }
    }

    private static string ReadSource(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !File.Exists(Path.Combine(dir, relativePath)))
        {
            dir = Directory.GetParent(dir)?.FullName;
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relativePath));
    }

    /// <summary>A minimal one-string-column reader, so the decorator is exercised without a live provider.</summary>
    private sealed class FakeRowReader(string[] rows) : DbDataReader
    {
        private int _index = -1;

        public override bool Read() => ++_index < rows.Length;
        public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromResult(Read());
        public override string GetString(int ordinal) => rows[_index];
        public override object GetValue(int ordinal) => rows[_index];
        public override int FieldCount => 1;
        public override bool HasRows => rows.Length > 0;
        public override bool IsClosed => false;
        public override int Depth => 0;
        public override int RecordsAffected => 0;
        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(0);
        public override bool GetBoolean(int ordinal) => throw new NotSupportedException();
        public override byte GetByte(int ordinal) => throw new NotSupportedException();
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;
        public override char GetChar(int ordinal) => throw new NotSupportedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;
        public override string GetDataTypeName(int ordinal) => "text";
        public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
        public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
        public override double GetDouble(int ordinal) => throw new NotSupportedException();
        public override IEnumerator GetEnumerator() => throw new NotSupportedException();
        public override Type GetFieldType(int ordinal) => typeof(string);
        public override float GetFloat(int ordinal) => throw new NotSupportedException();
        public override Guid GetGuid(int ordinal) => throw new NotSupportedException();
        public override short GetInt16(int ordinal) => throw new NotSupportedException();
        public override int GetInt32(int ordinal) => throw new NotSupportedException();
        public override long GetInt64(int ordinal) => throw new NotSupportedException();
        public override string GetName(int ordinal) => "value";
        public override int GetOrdinal(string name) => 0;
        public override int GetValues(object[] values) => 0;
        public override bool IsDBNull(int ordinal) => false;
        public override bool NextResult() => false;
    }
}
