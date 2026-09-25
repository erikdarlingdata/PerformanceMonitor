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
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4200: blocked_process_report and deadlocks read a DEDICATED single-event XE session and cast+shred its
/// ring buffer on every cycle, even on the ~99.98% / ~98.7% of runs (measured on a production fleet) that
/// find nothing new. <see cref="XeShredGate"/> is the pure decision (unit-testable without a server); these
/// tests also pin that both collectors consult it -- read <c>execution_count</c> BEFORE the cast, in the
/// same batch -- and that <c>ReadAsync</c> carries the observed count forward into
/// <see cref="CollectorContext.PendingState"/> regardless of whether the shred ran.
/// </summary>
public sealed class XeShredGateTests
{
    /* ── the pure decision: XeShredGate.ShouldShred ── */

    [Theory]
    [InlineData(null, null, true, "first run: no current, no prior")]
    [InlineData(100L, null, true, "no prior stored (first run, restarted host, lost row)")]
    [InlineData(null, 100L, true, "current count unavailable (session/target missing this cycle)")]
    [InlineData(100L, 100L, false, "unchanged: skip the cast+shred")]
    [InlineData(105L, 100L, true, "grew: shred")]
    [InlineData(95L, 100L, true, "went down: session restarted/recreated, shred")]
    [InlineData(0L, 0L, false, "both zero (a session that has delivered nothing yet, twice) still counts as unchanged")]
    public void ShouldShred_MatchesTheFiveGateShapes(long? current, long? last, bool expected, string because)
    {
        Assert.True(expected == XeShredGate.ShouldShred(current, last), because);
    }

    /* ── KeyFor / ReadLast ── */

    [Fact]
    public void KeyFor_NullDatabase_IsThePlainServerKey_NamedDatabase_IsCompound()
    {
        Assert.Equal("xe_execution_count", XeShredGate.KeyFor(null));
        Assert.Equal("xe_execution_count:ringdb", XeShredGate.KeyFor("ringdb"));
    }

    [Fact]
    public void ReadLast_AbsentBlankOrMalformed_AllReadAsNoPrior()
    {
        var empty = new Dictionary<string, string>();
        Assert.Null(XeShredGate.ReadLast(empty, null));

        var blank = new Dictionary<string, string> { ["xe_execution_count"] = "" };
        Assert.Null(XeShredGate.ReadLast(blank, null));

        var malformed = new Dictionary<string, string> { ["xe_execution_count"] = "not-a-number" };
        Assert.Null(XeShredGate.ReadLast(malformed, null));

        var valid = new Dictionary<string, string> { ["xe_execution_count"] = "12345" };
        Assert.Equal(12345L, XeShredGate.ReadLast(valid, null));
    }

    [Fact]
    public void ReadLast_PerDatabase_DoesNotCollideWithTheServerKey()
    {
        var state = new Dictionary<string, string>
        {
            ["xe_execution_count"] = "1",
            ["xe_execution_count:dbone"] = "10",
            ["xe_execution_count:dbtwo"] = "20",
        };

        Assert.Equal(1L, XeShredGate.ReadLast(state, null));
        Assert.Equal(10L, XeShredGate.ReadLast(state, "dbone"));
        Assert.Equal(20L, XeShredGate.ReadLast(state, "dbtwo"));
        Assert.Null(XeShredGate.ReadLast(state, "dbthree"));
    }

    /* ── BuildQuery: StateKeys, the parameter wiring, and the SQL-order pin ── */

    private static CollectorContext MakeContext(
        bool isAzureSqlDb = false, IReadOnlyDictionary<string, string>? state = null, string? currentDatabaseName = null)
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 9, 24, 12, 0, 0, DateTimeKind.Utc),
            Deltas = null!,
            Target = new CollectorTargetInfo { IsAzureSqlDb = isAzureSqlDb },
            State = state ?? CollectorContext.NoState,
            CurrentDatabaseName = currentDatabaseName,
        };

    [Fact]
    public void BothCollectors_DeclareTheStateKey_SoTheHostLoadsItAtAll()
    {
        Assert.Contains(XeShredGate.StateKey, BlockedProcessReportCollector.Instance.StateKeys);
        Assert.Contains(XeShredGate.StateKey, DeadlocksCollector.Instance.StateKeys);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BlockedProcessReport_BuildQuery_ReadsExecutionCountBeforeTheCast(bool azure)
    {
        var plan = BlockedProcessReportCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: azure));

        var gateRead = plan.Text.IndexOf("@execution_count = xet.execution_count", StringComparison.Ordinal);
        var gateIf = plan.Text.IndexOf("IF @shred_needed = 1", StringComparison.Ordinal);
        var cast = plan.Text.IndexOf("TRY_CAST(xet.target_data AS xml)", StringComparison.Ordinal);

        Assert.True(gateRead >= 0, "no execution_count read");
        Assert.True(gateIf >= 0, "no shred_needed gate");
        Assert.True(cast >= 0, "no cast at all");
        Assert.True(gateRead < gateIf && gateIf < cast, "the gate must read execution_count and branch BEFORE the cast");

        var lastParam = Assert.Single(plan.Parameters, p => p.Name == "@last_execution_count");
        Assert.Equal(CollectorParameterType.BigInt, lastParam.Type);
        Assert.Null(lastParam.Value);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Deadlocks_BuildQuery_ReadsExecutionCountBeforeTheCast(bool azure)
    {
        var plan = DeadlocksCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: azure));

        var gateRead = plan.Text.IndexOf("@execution_count = xet.execution_count", StringComparison.Ordinal);
        var gateIf = plan.Text.IndexOf("IF @shred_needed = 1", StringComparison.Ordinal);
        var cast = plan.Text.IndexOf("TRY_CAST(xet.target_data AS xml)", StringComparison.Ordinal);

        Assert.True(gateRead >= 0, "no execution_count read");
        Assert.True(gateIf >= 0, "no shred_needed gate");
        Assert.True(cast >= 0, "no cast at all");
        Assert.True(gateRead < gateIf && gateIf < cast, "the gate must read execution_count and branch BEFORE the cast");

        var lastParam = Assert.Single(plan.Parameters, p => p.Name == "@last_execution_count");
        Assert.Equal(CollectorParameterType.BigInt, lastParam.Type);
    }

    [Fact]
    public void Deadlocks_Azure_TelemetryBlobBranch_IsNotGated()
    {
        /* The durable telemetry-blob UNION branch has no execution_count of its own and must keep running
           unconditionally -- only the ring-buffer branch's cast is behind the IF. */
        var plan = DeadlocksCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: true));

        var gateIf = plan.Text.IndexOf("IF @shred_needed = 1", StringComparison.Ordinal);
        var endIf = plan.Text.IndexOf("END;", gateIf, StringComparison.Ordinal);
        var telemetry = plan.Text.IndexOf("fn_xe_telemetry_blob_target_read_file", StringComparison.Ordinal);

        Assert.True(telemetry > endIf, "the telemetry-blob read must sit OUTSIDE the gate's IF block");
    }

    [Fact]
    public void BuildQuery_PassesTheStoredPrior_PerServer_AndPerDatabase()
    {
        var serverState = new Dictionary<string, string> { ["xe_execution_count"] = "777" };
        var serverPlan = BlockedProcessReportCollector.Instance.BuildQuery(MakeContext(state: serverState));
        Assert.Equal(777L, Assert.Single(serverPlan.Parameters, p => p.Name == "@last_execution_count").Value);

        var dbState = new Dictionary<string, string> { ["xe_execution_count:ringdb"] = "42" };
        var dbPlan = DeadlocksCollector.Instance.BuildQuery(
            MakeContext(isAzureSqlDb: true, state: dbState, currentDatabaseName: "ringdb"));
        Assert.Equal(42L, Assert.Single(dbPlan.Parameters, p => p.Name == "@last_execution_count").Value);
    }

    /* ── ReadAsync: the trailing result set round-trips into PendingState / Measure ── */

    [Fact]
    public async Task BlockedProcessReport_ReadAsync_PersistsExecutionCount_AndMeasuresGated()
    {
        var context = MakeContext();

        using var reader = FakeCollectorDataReader.WithResultSets(
            Array.Empty<object[]>(),
            new[] { new object[] { 999L, true } });

        var rows = await BlockedProcessReportCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(rows);
        Assert.Equal("999", context.PendingState[XeShredGate.StateKey]);
        Assert.Contains(context.Measurements, m => m.Label == BlockedProcessReportCollector.ShredGatedMeasurement && m.Value == 1);
    }

    [Fact]
    public async Task BlockedProcessReport_ReadAsync_PerDatabase_PersistsUnderTheCompoundKey()
    {
        var context = MakeContext(isAzureSqlDb: true, currentDatabaseName: "ringdb");

        using var reader = FakeCollectorDataReader.WithResultSets(
            Array.Empty<object[]>(),
            new[] { new object[] { 55L, false } });

        await BlockedProcessReportCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal("55", context.PendingState["xe_execution_count:ringdb"]);
    }

    [Fact]
    public async Task BlockedProcessReport_ReadAsync_ExecutionCountUnavailable_WritesNothing()
    {
        var context = MakeContext();

        using var reader = FakeCollectorDataReader.WithResultSets(
            Array.Empty<object[]>(),
            new[] { new object[] { DBNull.Value, true } });

        await BlockedProcessReportCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        /* A NULL execution_count this cycle must not overwrite (or fabricate) a stored prior -- next
           cycle sees the same absence a first run would and takes the conservative full-shred path. */
        Assert.False(context.PendingState.ContainsKey(XeShredGate.StateKey));
    }

    [Fact]
    public async Task Deadlocks_ReadAsync_PersistsExecutionCount()
    {
        var context = MakeContext();

        using var reader = FakeCollectorDataReader.WithResultSets(
            Array.Empty<object[]>(),
            new[] { new object[] { 321L, false } });

        var rows = await DeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(rows);
        Assert.Equal("321", context.PendingState[XeShredGate.StateKey]);
    }
}
