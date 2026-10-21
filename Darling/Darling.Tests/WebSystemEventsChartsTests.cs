/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The web System Events tab's Corruption Events and Contention Events charts (#6295). The chart read's buckets carry each
/// counter's peak and each sick spinlock type's backoffs; the page draws them with <c>pages/system-events-charts.js</c>, run
/// here under Node (<c>web-system-events-charts-harness.mjs</c>, skipped when Node is not installed).
/// </summary>
public sealed class WebSystemEventsChartsBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-system-events-charts-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped chart script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the System Events charts harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the System Events charts harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static SystemHealthRecord Snap(DateTime at, long? bad = null, long? latch = null, string? spin = null, long? backoffs = null) =>
        new() { EventTime = at, BadPagesDetected = bad, LatchWarnings = latch, SickSpinlockType = spin, SpinlockBackoffs = backoffs };

    /// <summary>The seven charts are in the viewer's groups and order, and each leaves its card out when the window has no row.</summary>
    [Fact]
    public void TheSevenCharts_AreInTheViewersGroups_AndHideWhenTheWindowHasNoRows()
    {
        var r = Run();

        Assert.Equal(
            new[]
            {
                "Corruption Events: Bad Pages", "Corruption Events: Dump Requests", "Corruption Events: Access Violations",
                "Corruption Events: Write Access Violations", "Contention Events: Non-Yielding Tasks",
                "Contention Events: Latch Warnings", "Contention Events: Sick Spinlocks",
            },
            r.GetProperty("titles").EnumerateArray().Select(t => t.GetString()).ToArray());
        Assert.True(r.GetProperty("hides").GetBoolean());
    }

    /// <summary>A counter collected as 0 draws a chart; a counter no bucket carried says so instead of drawing a zero line.</summary>
    [Fact]
    public void ACollectedZero_IsDrawn_AndAnAbsentCounter_SaysSo()
    {
        var r = Run();

        Assert.True(r.GetProperty("zeroDrawn").GetProperty("hasChart").GetBoolean());
        Assert.False(r.GetProperty("absent").GetProperty("hasChart").GetBoolean());
        Assert.Equal("No latch warning count was reported in this window.", r.GetProperty("absent").GetProperty("text").GetString());
        var has = r.GetProperty("hasData");
        Assert.True(has.GetProperty("zero").GetBoolean());
        Assert.False(has.GetProperty("absent").GetBoolean());
        Assert.False(has.GetProperty("missingField").GetBoolean());
        Assert.False(has.GetProperty("none").GetBoolean());
    }

    /// <summary>One line per sick spinlock type, with a gap in the buckets that did not name it; no type named is an empty line, not a chart.</summary>
    [Fact]
    public void SickSpinlocks_DrawALinePerType_WithGaps_AndNoneNamedSaysSo()
    {
        var r = Run();

        var points = r.GetProperty("spinPoints").GetProperty("points").EnumerateArray().ToArray();
        Assert.Equal(120, points[0].GetProperty("s0").GetInt32());
        Assert.Equal(JsonValueKind.Null, points[0].GetProperty("s1").ValueKind);
        Assert.Equal(JsonValueKind.Null, points[1].GetProperty("s0").ValueKind);
        Assert.Equal(5, points[2].GetProperty("s1").GetInt32());
        Assert.Equal(
            new[] { "SOS_CACHESTORE", "LOCK_HASH" },
            r.GetProperty("spinPoints").GetProperty("series").EnumerateArray().Select(s => s.GetProperty("label").GetString()).ToArray());
        Assert.True(r.GetProperty("spinDrawn").GetProperty("hasChart").GetBoolean());
        Assert.False(r.GetProperty("spinNone").GetProperty("hasChart").GetBoolean());
        Assert.Equal("No sick spinlock was reported in this window.", r.GetProperty("spinNone").GetProperty("text").GetString());
    }

    /// <summary>A bucket carries the LARGEST value of each counter (one bad page is never averaged away), 0 stays 0, and a counter no
    /// snapshot in the bucket carried is null, not 0.</summary>
    [Fact]
    public void ABucket_CarriesEachCounterPeak_ZeroStaysZero_AndAbsenceStaysNull()
    {
        var t = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc);
        var rows = new[]
        {
            Snap(t, bad: 0, latch: 4),
            Snap(t.AddMinutes(2), bad: 3, latch: 1),
            Snap(t.AddMinutes(4), bad: 0),
            Snap(t.AddMinutes(30), bad: 0),
        };

        var buckets = DarlingMcpHealthParserTools.BucketCpu(rows, 10);

        Assert.Equal(3, buckets[0].BadPagesDetected);
        Assert.Equal(4, buckets[0].LatchWarnings);
        Assert.Equal(0, buckets[1].BadPagesDetected);
        Assert.Null(buckets[1].LatchWarnings);
        Assert.Null(buckets[0].IntervalDumpRequests);
    }

    /// <summary>The sick spinlock series are the first five types in snapshot order, backoffs peak per bucket (1 when the snapshot names
    /// the type without a count), and SQL Server's "none" (no spinlock is sick) is no series.</summary>
    [Fact]
    public void SickSpinlocks_AreTheFirstFiveTypesOldestFirst_PeakPerBucket_AndNoneIsNotASeries()
    {
        var t = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc);
        var rows = new[]
        {
            Snap(t.AddMinutes(8), spin: "B", backoffs: 5),
            Snap(t.AddMinutes(1), spin: "none", backoffs: 0),
            Snap(t, spin: "A", backoffs: 10),
            Snap(t.AddMinutes(2), spin: "A", backoffs: 30),
            Snap(t.AddMinutes(3), spin: "C"),
            Snap(t.AddMinutes(11), spin: "D", backoffs: 1),
            Snap(t.AddMinutes(12), spin: "E", backoffs: 1),
            Snap(t.AddMinutes(13), spin: "F", backoffs: 1),
        };

        Assert.Equal(new[] { "A", "C", "B", "D", "E" }, DarlingMcpHealthParserTools.SickSpinlockTypes(rows));
        var first = DarlingMcpHealthParserTools.BucketCpu(rows, 10)[0];
        Assert.Equal(30, first.SickSpinlocks["A"]);
        Assert.Equal(1, first.SickSpinlocks["C"]);
        Assert.Equal(5, first.SickSpinlocks["B"]);
        Assert.False(first.SickSpinlocks.ContainsKey("none"));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live read of the chart form of get_health_parser_system_health over planted SYSTEM snapshots (#6295):
/// the counters and spinlock types ride beside the CPU buckets, the old fields are unchanged, and a server with no snapshot
/// in the window answers with no <c>entries</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class WebSystemEventsChartsLiveTests
{
    private const string ServerName = "darling-system-events-charts";
    private const string EmptyServerName = "darling-system-events-charts-empty";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly int EmptyServerId = ServerIdHelper.GetDeterministicHashCode(EmptyServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The SYSTEM fixture with its timestamp and counters replaced.</summary>
    private static string SystemXml(DateTime at, int bad, int latch, string spinType, int backoffs)
    {
        var xml = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", "sp_server_diagnostics_system.xml"));
        xml = Regex.Replace(xml, "timestamp=\"[^\"]*\"", $"timestamp=\"{at:yyyy-MM-ddTHH:mm:ss.000Z}\"", RegexOptions.None, TimeSpan.FromSeconds(5));
        xml = Regex.Replace(xml, "BadPagesDetected=\"\\d+\"", $"BadPagesDetected=\"{bad}\"", RegexOptions.None, TimeSpan.FromSeconds(5));
        xml = Regex.Replace(xml, "latchWarnings=\"\\d+\"", $"latchWarnings=\"{latch}\"", RegexOptions.None, TimeSpan.FromSeconds(5));
        xml = Regex.Replace(xml, "sickSpinlockType=\"[^\"]*\"", $"sickSpinlockType=\"{spinType}\"", RegexOptions.None, TimeSpan.FromSeconds(5));
        return Regex.Replace(xml, "spinlockBackoffs=\"\\d+\"", $"spinlockBackoffs=\"{backoffs}\"", RegexOptions.None, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task ChartRead_CarriesTheCountersAndSpinlockTypes_AndAnEmptyWindowCarriesNoEntries()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live System Events chart test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, EmptyServerId, EmptyServerName, ct);
            var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var first = new DateTime(t.AddHours(-2).Ticks / TimeSpan.TicksPerHour * TimeSpan.TicksPerHour, DateTimeKind.Utc).AddMinutes(5);   // inside one 60-minute bucket

            async Task Plant(DateTime at, string xml) =>
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO system_health_events (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7)",
                    CollectionIdGenerator.Next(), at, ServerId, ServerName, at, SystemHealthParser.SpServerDiagnosticsEvent, xml);

            /* Two snapshots one minute apart (one bucket at a 60-minute width): the peak wins. A third an hour later carries 0 counters. */
            await Plant(first, SystemXml(first, bad: 1, latch: 0, spinType: "SOS_CACHESTORE", backoffs: 40));
            await Plant(first.AddMinutes(1), SystemXml(first.AddMinutes(1), bad: 6, latch: 0, spinType: "none", backoffs: 0));
            var later = first.AddMinutes(61);
            await Plant(later, SystemXml(later, bad: 0, latch: 0, spinType: "none", backoffs: 0));

            var json = await DarlingMcpHealthParserTools.GetSystemHealthChart(postgres, ServerName, 3, 60, null, cancellationToken: ct);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            Assert.Equal(new[] { "SOS_CACHESTORE" }, root.GetProperty("sick_spinlock_types").EnumerateArray().Select(x => x.GetString()).ToArray());
            var entries = root.GetProperty("entries").EnumerateArray().ToArray();
            Assert.Equal(2, entries.Length);
            // The old fields are still there, unchanged in name.
            Assert.Equal(65, entries[0].GetProperty("system_cpu_utilization").GetDouble());
            Assert.Equal(2, entries[0].GetProperty("snapshots").GetInt32());
            Assert.Equal(6, entries[0].GetProperty("bad_pages_detected").GetInt64());
            Assert.Equal(0, entries[0].GetProperty("latch_warnings").GetInt64());
            Assert.Equal(2, entries[0].GetProperty("interval_dump_requests").GetInt64());
            Assert.Equal(1, entries[0].GetProperty("is_access_violation_occurred").GetInt64());
            Assert.Equal(3, entries[0].GetProperty("write_access_violation_count").GetInt64());
            Assert.Equal(4, entries[0].GetProperty("non_yielding_tasks_reported").GetInt64());
            var spin = entries[0].GetProperty("sick_spinlocks").EnumerateArray().Single();
            Assert.Equal("SOS_CACHESTORE", spin.GetProperty("type").GetString());
            Assert.Equal(40, spin.GetProperty("backoffs").GetInt64());
            // A collected 0 is a real 0; a bucket that names no sick spinlock lists none.
            Assert.Equal(0, entries[1].GetProperty("bad_pages_detected").GetInt64());
            Assert.Empty(entries[1].GetProperty("sick_spinlocks").EnumerateArray());

            // A server with no snapshot in the window answers with no entries, so the page draws no zero line.
            using var empty = JsonDocument.Parse(await DarlingMcpHealthParserTools.GetSystemHealthChart(postgres, EmptyServerName, 3, 60, null, cancellationToken: ct));
            Assert.False(empty.RootElement.TryGetProperty("entries", out _));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var sql = $"DELETE FROM system_health_events WHERE server_id IN ({ServerId}, {EmptyServerId}); DELETE FROM servers WHERE server_id IN ({ServerId}, {EmptyServerId});";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
