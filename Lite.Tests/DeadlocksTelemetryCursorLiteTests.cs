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

    private static IReadOnlyDictionary<string, string> Cursor(DateTime t) =>
        new Dictionary<string, string> { [Key] = t.ToString("o", CultureInfo.InvariantCulture) };

    [Fact]
    public async Task TelemetryRows_StageTheirNewestTime_AndTheNextQueryBindsIt()
    {
        var ctx = await ReadAsync(Ctx(), Row(At(0, 10), "HS"), Row(At(0, 20), "HS"));

        Assert.Equal(At(0, 20).ToString("o", CultureInfo.InvariantCulture), ctx.PendingState[Key]);

        var next = DeadlocksCollector.Instance.BuildQuery(Ctx(state: ctx.PendingState));
        Assert.Equal(At(0, 20), Param(next, "@telemetry_cutoff_time"));
        Assert.NotEqual(Now.AddMinutes(-10), Param(next, "@telemetry_cutoff_time"));
        Assert.Equal(Now.AddMinutes(-10), Param(next, "@cutoff_time"));
    }

    [Fact]
    public async Task ALaterRowAdvancesTheCursor_AQuietRunStagesNothing_AndItNeverMovesBack()
    {
        var advanced = await ReadAsync(Ctx(state: Cursor(At(0, 20))), Row(At(0, 25), "HS"));
        Assert.Equal(At(0, 25).ToString("o", CultureInfo.InvariantCulture), advanced.PendingState[Key]);

        var quiet = await ReadAsync(Ctx(state: Cursor(At(0, 20))));
        Assert.False(quiet.PendingState.ContainsKey(Key));

        var older = await ReadAsync(Ctx(state: Cursor(At(0, 20))), Row(At(0, 5), "HS"));
        Assert.Equal(At(0, 20).ToString("o", CultureInfo.InvariantCulture), older.PendingState[Key]);
    }

    [Fact]
    public async Task ARingBufferItem_StagesNoCursor_AndKeepsItsOwnWatermark()
    {
        var ctx = await ReadAsync(Ctx(db: "GP", watermark: At(1, 0)), Row(At(1, 30), DBNull.Value));
        Assert.False(ctx.PendingState.ContainsKey(Key));

        var q = DeadlocksCollector.Instance.BuildQuery(Ctx(db: "GP", watermark: At(1, 0)));
        /* The ring arm's cutoff is its own cursor, or the stored watermark minus the ten minute window until one exists. */
        Assert.Equal(At(1, 0).AddMinutes(-10), Param(q, "@cutoff_time"));
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

        Assert.Equal(At(0, 20), Param(q, "@telemetry_cutoff_time"));
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
