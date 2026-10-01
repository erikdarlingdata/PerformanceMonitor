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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// On an Azure SQL Database master registration the telemetry arm stores other databases' events stamped with
/// their own names, so a database's stored watermark can run ahead of what its OWN ring buffer returned. Each
/// database item's ring-buffer arm therefore keeps its own cursor in collector state: the newest
/// <c>deadlock_time</c> that arm itself returned for that database.
/// </summary>
public class DeadlocksRingCursorTests
{
    private const string ZetaKey = "dl_ring_cursor:zeta";
    private const string AlphaKey = "dl_ring_cursor:alpha";
    private static readonly DateTime Now = new(2026, 8, 26, 12, 5, 0, DateTimeKind.Utc);
    private static DateTime At(int minute, int second) => new(2026, 8, 26, 12, minute, second, DateTimeKind.Utc);
    private static string Iso(DateTime t) => t.ToString("o", CultureInfo.InvariantCulture);

    private static CollectorContext Ctx(
        bool azure = true,
        string? db = "zeta",
        DateTime? watermark = null,
        IReadOnlyDictionary<string, string>? state = null,
        bool managedInstance = false) => new()
    {
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

    private static async Task<CollectorContext> ReadAsync(CollectorContext ctx, params object[][] rows)
    {
        using var reader = new Reader(rows.ToArray(), new[] { new object[] { 100L, false } });
        await DeadlocksCollector.Instance.ReadAsync(reader, ctx, CancellationToken.None);
        return ctx;
    }

    private static object? Param(CollectorQuery q, string name) =>
        q.Parameters.FirstOrDefault(p => p.Name == name)?.Value;

    private static IReadOnlyDictionary<string, string> State(params (string Key, DateTime Time)[] entries) =>
        entries.ToDictionary(e => e.Key, e => Iso(e.Time));

    [Fact]
    public void AStoredWatermarkAheadOfTheRingCursor_DoesNotMoveTheRingCutoff()
    {
        /* Telemetry stored a zeta event at 12:00:30; zeta's own ring buffer last returned 12:00:10, so a
           ring-only event at 12:00:20 must still be read. */
        var q = DeadlocksCollector.Instance.BuildQuery(Ctx(watermark: At(0, 30), state: State((ZetaKey, At(0, 10)))));

        Assert.Equal(At(0, 10), Param(q, "@cutoff_time"));
    }

    [Fact]
    public async Task RingRows_StageThatDatabasesCursor_TelemetryRowsDoNot_AQuietRunStagesNothing_AndItNeverMovesBack()
    {
        var ring = await ReadAsync(Ctx(), Row(At(0, 10), DBNull.Value), Row(At(0, 20), DBNull.Value));
        Assert.Equal(Iso(At(0, 20)), ring.PendingState[ZetaKey]);

        var telemetryOnly = await ReadAsync(Ctx(db: "master"), Row(At(1, 0), "zeta"));
        Assert.DoesNotContain(telemetryOnly.PendingState.Keys, k => k.StartsWith("dl_ring_cursor", StringComparison.Ordinal));

        var mixed = await ReadAsync(Ctx(), Row(At(0, 20), DBNull.Value), Row(At(2, 0), "other"));
        Assert.Equal(Iso(At(0, 20)), mixed.PendingState[ZetaKey]);

        var quiet = await ReadAsync(Ctx(state: State((ZetaKey, At(0, 20)))));
        Assert.DoesNotContain(quiet.PendingState.Keys, k => k.StartsWith("dl_ring_cursor", StringComparison.Ordinal));

        var older = await ReadAsync(Ctx(state: State((ZetaKey, At(0, 20)))), Row(At(0, 5), DBNull.Value));
        Assert.Equal(Iso(At(0, 20)), older.PendingState[ZetaKey]);

        var later = await ReadAsync(Ctx(state: State((ZetaKey, At(0, 20)))), Row(At(0, 25), DBNull.Value));
        Assert.Equal(Iso(At(0, 25)), later.PendingState[ZetaKey]);
    }

    [Fact]
    public async Task AStagedRingCursor_IsWhatTheNextRunBindsAsItsCutoff()
    {
        var ctx = await ReadAsync(Ctx(watermark: At(0, 30)), Row(At(0, 20), DBNull.Value));

        var next = DeadlocksCollector.Instance.BuildQuery(Ctx(watermark: At(0, 30), state: ctx.PendingState));
        Assert.Equal(At(0, 20), Param(next, "@cutoff_time"));
    }

    [Fact]
    public void FirstRunWithNoRingCursor_FallsBackToTheStoredWatermarkMinusTenMinutes_OrTheCollectionTimeMinusTen()
    {
        var withWatermark = DeadlocksCollector.Instance.BuildQuery(Ctx(watermark: At(0, 30)));
        Assert.Equal(At(0, 30).AddMinutes(-10), Param(withWatermark, "@cutoff_time"));

        var without = DeadlocksCollector.Instance.BuildQuery(Ctx());
        Assert.Equal(Now.AddMinutes(-10), Param(without, "@cutoff_time"));
    }

    [Fact]
    public async Task TwoDatabases_KeepSeparateCursors()
    {
        var state = State((ZetaKey, At(0, 10)), (AlphaKey, At(3, 0)));

        var zeta = DeadlocksCollector.Instance.BuildQuery(Ctx(db: "zeta", state: state));
        var alpha = DeadlocksCollector.Instance.BuildQuery(Ctx(db: "alpha", state: state));
        Assert.Equal(At(0, 10), Param(zeta, "@cutoff_time"));
        Assert.Equal(At(3, 0), Param(alpha, "@cutoff_time"));

        var read = await ReadAsync(Ctx(db: "alpha", state: state), Row(At(4, 0), DBNull.Value));
        Assert.Equal(Iso(At(4, 0)), read.PendingState[AlphaKey]);
        Assert.False(read.PendingState.ContainsKey(ZetaKey));
    }

    [Fact]
    public void TheTelemetryArmKeepsItsOwnCutoff_WhateverTheRingCursorIs()
    {
        var q = DeadlocksCollector.Instance.BuildQuery(Ctx(
            db: "master",
            watermark: At(1, 0),
            state: new Dictionary<string, string> { ["dl_telemetry_cursor"] = Iso(At(0, 40)), [ZetaKey] = Iso(At(0, 10)) }));

        Assert.Equal(At(0, 40), Param(q, "@telemetry_cutoff_time"));
    }

    [Fact]
    public void TheRingCursorKeyIsDeclared_ByThePrefixTheStateLoadReturns()
    {
        Assert.Contains(DeadlocksCollector.Instance.StateKeys, k => k.StartsWith("dl_ring_cursor", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public void OnPremAndManagedInstance_KeepTheirParameterListAndText(bool azure, bool managedInstance)
    {
        var q = DeadlocksCollector.Instance.BuildQuery(Ctx(
            azure: azure, managedInstance: managedInstance, db: null, watermark: At(0, 30), state: State((ZetaKey, At(0, 10)))));

        Assert.Equal(new[] { "@cutoff_time", "@last_execution_count" }, q.Parameters.Select(p => p.Name).ToArray());
        Assert.Equal(At(0, 30), Param(q, "@cutoff_time"));
        Assert.DoesNotContain("@telemetry_cutoff_time", q.Text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnPremRingRows_StageNoRingCursor()
    {
        var ctx = await ReadAsync(Ctx(azure: false, db: null), Row(At(0, 10), DBNull.Value));

        Assert.DoesNotContain(ctx.PendingState.Keys, k => k.StartsWith("dl_ring_cursor", StringComparison.Ordinal));
    }

    [Fact]
    public void TheFallbackRereadOfAStoredEvent_IsDroppedByTheExactPreInsertDedupe()
    {
        var time = new DateTime(2026, 8, 26, 12, 0, 20, DateTimeKind.Unspecified);
        const string graph = "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list></deadlock>";
        var stored = new HashSet<(DateTime Time, string Graph)> { (time, graph) };
        var reread = new DeadlocksCollector.Row { DeadlockTime = time, GraphXml = graph, DatabaseName = "zeta" };
        var ringOnly = new DeadlocksCollector.Row { DeadlockTime = time.AddSeconds(5), GraphXml = graph, DatabaseName = "zeta" };

        var kept = DeadlocksCollector.Instance.DropAlreadyStored(new List<DeadlocksCollector.Row> { reread, ringOnly }, stored);

        Assert.Same(ringOnly, Assert.Single(kept));
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
