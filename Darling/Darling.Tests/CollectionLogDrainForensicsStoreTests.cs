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
