/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// On an Azure SQL Database logical server registered at <c>master</c>, the telemetry arm of the deadlock
/// read stamps each row with the SOURCE database, so the <c>master</c> item's per-database watermark
/// (<c>database_name = 'master'</c>) almost never finds a row and every run re-reads the last ten minutes.
/// The arm keeps its own cursor in collector state: the newest <c>deadlock_time</c> the arm itself returned.
/// </summary>
public class DeadlocksTelemetryCursorLiteTests
{
    private const string Key = "dl_telemetry_cursor";
    private static readonly DateTime Now = new(2026, 8, 26, 12, 5, 0, DateTimeKind.Utc);
    private static DateTime At(int minute, int second) => new(2026, 8, 26, 12, minute, second, DateTimeKind.Utc);

    private static CollectorContext Ctx(
        bool azure = true,
        string? db = "master",
        DateTime? watermark = null,
        IReadOnlyDictionary<string, string>? state = null,
        bool managedInstance = false,
        bool capturePlanXml = false) => new()
    {
        CapturePlanXml = capturePlanXml,
        ServerId = 1,
        ServerName = "s",
        CollectionTime = Now,
        Deltas = null!,
        Target = new CollectorTargetInfo { IsAzureSqlDb = azure, IsAzureManagedInstance = managedInstance },
        CurrentDatabaseName = db,
        Watermark = watermark,
        State = state ?? CollectorContext.NoState,
    };

    private static object[] Row(DateTime t, object source) => new object[] { t, "process1", "<deadlock/>", source };

    /* CapturePlanXml splices victim_query_plan_xml in at ordinal 3, which moves source_database_name to 4. */
    private static object[] PlanRow(DateTime t, object source) => new object[] { t, "process1", "<deadlock/>", "<plan/>", source };

    private static async Task<CollectorContext> ReadAsync(CollectorContext ctx, params object[][] rows)
    {
        using var reader = new Reader(rows.ToArray(), new[] { new object[] { 100L, false } });
        await DeadlocksCollector.Instance.ReadAsync(reader, ctx, CancellationToken.None);
        return ctx;
    }

    private static object? Param(CollectorQuery q, string name) =>
        q.Parameters.FirstOrDefault(p => p.Name == name)?.Value;

    private static IReadOnlyDictionary<string, string> Cursor(DateTime t) =>
        new Dictionary<string, string> { [Key] = t.ToString("o", CultureInfo.InvariantCulture) };

    [Fact]
    public async Task TelemetryRows_StageTheirNewestTime_AndTheNextQueryBindsIt()
    {
        var ctx = await ReadAsync(Ctx(), Row(At(0, 10), "HS"), Row(At(0, 20), "HS"));

        /* Staged for the item, not saved: the host lands it only after the item's rows are written. */
        Assert.False(ctx.PendingState.ContainsKey(Key));
        Assert.Equal(At(0, 20).ToString("o", CultureInfo.InvariantCulture), ctx.StagedItemState[Key]);

        ctx.LandStagedItemState();

        var next = DeadlocksCollector.Instance.BuildQuery(Ctx(state: ctx.PendingState));
        Assert.Equal(At(0, 20).AddMinutes(-10), Param(next, "@telemetry_cutoff_time"));
        Assert.Equal(Now.AddMinutes(-10), Param(next, "@cutoff_time"));
    }

    [Fact]
    public async Task ALaterRowAdvancesTheCursor_AQuietRunStagesNothing_AndItNeverMovesBack()
    {
        var advanced = await ReadAsync(Ctx(state: Cursor(At(0, 20))), Row(At(0, 25), "HS"));
        Assert.Equal(At(0, 25).ToString("o", CultureInfo.InvariantCulture), advanced.StagedItemState[Key]);

        var quiet = await ReadAsync(Ctx(state: Cursor(At(0, 20))));
        Assert.False(quiet.StagedItemState.ContainsKey(Key));

        var older = await ReadAsync(Ctx(state: Cursor(At(0, 20))), Row(At(0, 5), "HS"));
        Assert.Equal(At(0, 20).ToString("o", CultureInfo.InvariantCulture), older.StagedItemState[Key]);
    }

    [Fact]
    public async Task ARingBufferItem_StagesNoCursor_AndKeepsItsOwnWatermark()
    {
        var ctx = await ReadAsync(Ctx(db: "GP", watermark: At(1, 0)), Row(At(1, 30), DBNull.Value));
        Assert.False(ctx.StagedItemState.ContainsKey(Key));

        var q = DeadlocksCollector.Instance.BuildQuery(Ctx(db: "GP", watermark: At(1, 0)));
        /* The ring arm's cutoff is its own cursor, or the stored watermark minus the ten minute window until one exists. */
        Assert.Equal(At(1, 0).AddMinutes(-10), Param(q, "@cutoff_time"));
    }

    [Fact]
    public async Task WithPlanCaptureOn_TheSourceDatabaseIsStillReadFromTheShiftedColumn()
    {
        using var reader = new Reader(
            new[] { PlanRow(At(0, 10), "HS"), PlanRow(At(0, 30), "HS") }, new[] { new object[] { 100L, false } });
        var ctx = Ctx(capturePlanXml: true);

        var rows = await DeadlocksCollector.Instance.ReadAsync(reader, ctx, CancellationToken.None);

        Assert.All(rows, r => Assert.Equal("HS", r.DatabaseName));
        Assert.Equal(At(0, 30).ToString("o", CultureInfo.InvariantCulture), ctx.StagedItemState[Key]);
    }

    [Fact]
    public async Task AFailedWrite_DropsTheStagedCursor_SoNothingIsSavedAndTheNextRunReReadsTheBatch()
    {
        /* The master item reads a telemetry batch, then its write throws while a sibling database succeeds.
           The host drops what the failed item staged; the run still saves PendingState because the sibling
           succeeded, and that save must not carry the cursor. */
        var ctx = await ReadAsync(Ctx(), Row(At(0, 10), "HS"), Row(At(0, 20), "HS"));
        Assert.True(ctx.StagedItemState.ContainsKey(Key));

        ctx.DropStagedItemState();                           // master's write threw
        ctx.PendingState["sibling_state"] = "kept";          // a sibling's own, already-landed state

        Assert.False(ctx.PendingState.ContainsKey(Key));
        Assert.Empty(ctx.StagedItemState);

        var next = DeadlocksCollector.Instance.BuildQuery(Ctx(state: ctx.PendingState));
        Assert.Equal(Now.AddMinutes(-10), Param(next, "@telemetry_cutoff_time"));
    }

    [Fact]
    public async Task ASuccessfulWrite_LandsTheStagedCursor_AndTheShredGateCount()
    {
        var ctx = await ReadAsync(Ctx(), Row(At(0, 10), "HS"), Row(At(0, 20), "HS"));
        Assert.False(ctx.PendingState.ContainsKey(XeShredGate.KeyFor("master")));

        ctx.LandStagedItemState();

        Assert.Equal(At(0, 20).ToString("o", CultureInfo.InvariantCulture), ctx.PendingState[Key]);
        Assert.Equal("100", ctx.PendingState[XeShredGate.KeyFor("master")]);
        Assert.Empty(ctx.StagedItemState);
    }

    [Fact]
    public void BothHosts_LandStagedItemState_OnlyAfterTheItemsWrite_AndDropItBeforeTheRead()
    {
        /* The per-database loops swallow one item's failure and save PendingState if a sibling succeeded, so
           the order in the host IS the guarantee: the landing call must come after the flush, never before. */
        var root = FindRepoRoot();
        Assert.True(root is not null, "repo root not found -- the source pin cannot run");

        foreach (var (host, write) in new[]
        {
            ("Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs", "WriteBatchAsync(pgConnection, definition, batch"),
            ("Lite/Services/RemoteCollectorService.DefinitionRunner.cs", "WriteBatch(duckConnection, definition, batch"),
        })
        {
            var source = File.ReadAllText(Path.Combine(root!, host.Replace('/', Path.DirectorySeparatorChar)));
            var read = source.IndexOf("batch = await definition.ReadAsync(dbReader", StringComparison.Ordinal);
            Assert.True(read > 0, $"{host}: per-database read not found");
            var drop = source.LastIndexOf("context.DropStagedItemState();", read, StringComparison.Ordinal);
            var flush = source.IndexOf(write, read, StringComparison.Ordinal);
            var land = source.IndexOf("context.LandStagedItemState();", read, StringComparison.Ordinal);
            var perDatabaseCatch = source.IndexOf("catch (OutOfMemoryException)", read, StringComparison.Ordinal);

            Assert.True(drop > 0, $"{host} must drop staged item state before each database's read");
            Assert.True(flush > read && land > flush, $"{host} must land staged item state after the item's write");
            Assert.True(land < perDatabaseCatch, $"{host} must land inside the try, so a throw skips it");
        }
    }

    private static string? FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 12 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return null;
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public void OnPremAndManagedInstance_KeepTheirParameterListAndText(bool azure, bool managedInstance)
    {
        var q = DeadlocksCollector.Instance.BuildQuery(Ctx(azure: azure, managedInstance: managedInstance, db: null));

        Assert.Equal(new[] { "@cutoff_time", "@last_execution_count" }, q.Parameters.Select(p => p.Name).ToArray());
        Assert.DoesNotContain("@telemetry_cutoff_time", q.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void ANewerSiblingWatermark_DoesNotMoveTheTelemetryCutoffPastTheCursor()
    {
        var q = DeadlocksCollector.Instance.BuildQuery(Ctx(watermark: At(0, 30), state: Cursor(At(0, 20))));

        Assert.Equal(At(0, 20).AddMinutes(-10), Param(q, "@telemetry_cutoff_time"));
        Assert.Contains("@telemetry_cutoff_time", q.Text, StringComparison.Ordinal);
    }

    private sealed class Reader(object[][] rows, object[][] gate) : DbDataReader
    {
        private readonly object[][][] _sets = { rows, gate };
        private int _set;
        private int _row = -1;
        private object[] Cur => _sets[_set][_row];

        public override bool Read() => ++_row < _sets[_set].Length;
        public override bool NextResult() { if (_set + 1 >= _sets.Length) { return false; } _set++; _row = -1; return true; }
        public override string GetString(int o) => (string)Cur[o];
        public override long GetInt64(int o) => (long)Cur[o];
        public override DateTime GetDateTime(int o) => (DateTime)Cur[o];
        public override bool GetBoolean(int o) => (bool)Cur[o];
        public override bool IsDBNull(int o) => Cur[o] is DBNull;
        public override object GetValue(int o) => Cur[o];
        public override int FieldCount => _sets[_set].Length == 0 ? 0 : _sets[_set][0].Length;
        public override bool HasRows => _sets[_set].Length > 0;
        public override bool IsClosed => false;
        public override int Depth => 0;
        public override int RecordsAffected => -1;
        public override object this[int o] => Cur[o];
        public override object this[string n] => throw new NotSupportedException();
        public override byte GetByte(int o) => throw new NotSupportedException();
        public override long GetBytes(int o, long d, byte[]? b, int bo, int l) => throw new NotSupportedException();
        public override char GetChar(int o) => throw new NotSupportedException();
        public override long GetChars(int o, long d, char[]? b, int bo, int l) => throw new NotSupportedException();
        public override string GetDataTypeName(int o) => throw new NotSupportedException();
        public override decimal GetDecimal(int o) => throw new NotSupportedException();
        public override double GetDouble(int o) => throw new NotSupportedException();
        public override IEnumerator GetEnumerator() => throw new NotSupportedException();
        public override Type GetFieldType(int o) => throw new NotSupportedException();
        public override float GetFloat(int o) => throw new NotSupportedException();
        public override Guid GetGuid(int o) => throw new NotSupportedException();
        public override short GetInt16(int o) => throw new NotSupportedException();
        public override int GetInt32(int o) => throw new NotSupportedException();
        public override string GetName(int o) => throw new NotSupportedException();
        public override int GetOrdinal(string n) => throw new NotSupportedException();
        public override int GetValues(object[] v) => throw new NotSupportedException();
    }
}
