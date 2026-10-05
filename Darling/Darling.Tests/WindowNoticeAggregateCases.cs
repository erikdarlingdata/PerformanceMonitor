/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The window-floor live cases (#4966) of the three SQL Server aggregate reads that sum a raw collector table over the window:
/// <c>get_wait_stats</c>, <c>get_latch_stats</c> and <c>get_spinlock_stats</c>. Each tool's live class supplies how it is called and
/// how a row is planted; the cases are the same for all three. The reads window on <c>collection_time</c> of the raw table
/// (through its view), the column the coverage probe reads, and none has a rollup tier.
/// </summary>
internal sealed class WindowNoticeAggregateCases(
    string tool, string table, Func<NpgsqlDataSource, string, int, DateTime, int, Task<string>> call,
    Func<NpgsqlConnection, string, DateTime, int, Task> plantRow, string? truncatedKey)
{
    private string Name(string window) => tool.Replace('_', '-') + "-window-" + window;

    private Task RunAsync(string? cs, string window, Func<NpgsqlConnection, NpgsqlDataSource, DateTime, Task> body) =>
        WindowFloorLiveHarness.RunAsync(cs, [table], [Name(window)], [table], body);

    private Task SeedAsync(NpgsqlConnection c, string window, DateTime created, DateTime? runsFrom, int step, DateTime end) =>
        WindowFloorLiveHarness.SeedServerAsync(c, Name(window), created, table, runsFrom, step, end, [table], TestContext.Current.CancellationToken);

    private async Task PlantThreeAsync(NpgsqlConnection c, string window, DateTime at)
    {
        for (var i = 0; i < 3; i++)
        {
            await plantRow(c, Name(window), at, i);
        }
    }

    public Task CollectionStartingInsideTheWindow_NamesWhereCoverageStarts_AndACappedPageKeepsTheTwoFlagsApart(string? cs) =>
        RunAsync(cs, "added", async (c, ds, end) =>
        {
            var added = end.AddDays(-2);
            await SeedAsync(c, "added", added, added, 30, end);
            await PlantThreeAsync(c, "added", end.AddDays(-1));

            var root = WindowFloorLiveHarness.Parse(await call(ds, Name("added"), 168, end, 2));

            Assert.True(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(added), root.GetProperty("effective_start").GetString());
            Assert.Equal(DarlingMcpWindowNotice.Build(added, end.AddHours(-168), table).TruncationNote, root.GetProperty("truncation_note").GetString());
            Assert.False(root.TryGetProperty("effective_hours_back", out _));
            if (truncatedKey is not null)
            {
                Assert.True(root.GetProperty(truncatedKey).GetBoolean());
            }

            var names = root.EnumerateObject().Select(p => p.Name).ToList();
            Assert.Equal(["effective_start", "window_truncated", "truncation_note"], names.Skip(names.IndexOf("hours_back") + 1).Take(3));
        });

    public Task ARankCappedPage_BesideACoveredWindow_IsNotWindowTruncated(string? cs) =>
        RunAsync(cs, "capped", async (c, ds, end) =>
        {
            await SeedAsync(c, "capped", end.AddDays(-30), end.AddDays(-8), 60, end);
            await PlantThreeAsync(c, "capped", end.AddDays(-5));

            var root = WindowFloorLiveHarness.Parse(await call(ds, Name("capped"), 168, end, 2));

            Assert.False(root.GetProperty("window_truncated").GetBoolean());
            if (truncatedKey is not null)
            {
                Assert.True(root.GetProperty(truncatedKey).GetBoolean());
            }
        });

    public Task AQuietStart_IsCovered(string? cs) =>
        RunAsync(cs, "quiet", async (c, ds, end) =>
        {
            await SeedAsync(c, "quiet", end.AddDays(-30), end.AddDays(-8), 60, end);
            await PlantThreeAsync(c, "quiet", end.AddDays(-5));

            var root = WindowFloorLiveHarness.Parse(await call(ds, Name("quiet"), 168, end, 10));

            Assert.False(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
            var effective = DateTime.Parse(root.GetProperty("effective_start").GetString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);
            Assert.InRange((effective - end.AddHours(-168)).TotalSeconds, 0, 120);
        });

    public Task TheNoRowsAnswer_StaysBare(string? cs) =>
        RunAsync(cs, "bare", async (c, ds, end) =>
        {
            await SeedAsync(c, "bare", end.AddDays(-30), null, 30, end);

            var root = WindowFloorLiveHarness.Parse(await call(ds, Name("bare"), 168, end, 10));

            Assert.Equal("unavailable", root.GetProperty("status").GetString());
            Assert.False(root.TryGetProperty("effective_start", out _));
            Assert.False(root.TryGetProperty("window_truncated", out _));
            Assert.False(root.TryGetProperty("truncation_note", out _));
            Assert.False(root.TryGetProperty("hints", out _));
        });

    public Task AShortWindow_WithRows_StartsNoProbe(string? cs) =>
        RunAsync(cs, "short", async (c, ds, end) =>
        {
            await SeedAsync(c, "short", end.AddDays(-30), end.AddDays(-2), 30, end);
            await plantRow(c, Name("short"), end.AddMinutes(-20), 0);
            var calls = 0;
            DarlingMcpWindowNotice.TestOnlyProbe = () => { calls++; return Task.FromResult<DateTime?>(null); };

            var root = WindowFloorLiveHarness.Parse(await call(ds, Name("short"), 1, end, 10));

            Assert.Equal(0, calls);
            Assert.False(root.GetProperty("window_truncated").GetBoolean());
        });

    public Task AFailedProbe_CostsTheNotice_NeverTheRows(string? cs) =>
        RunAsync(cs, "probefail", async (c, ds, end) =>
        {
            await SeedAsync(c, "probefail", end.AddDays(-2), end.AddDays(-2), 30, end);
            await PlantThreeAsync(c, "probefail", end.AddDays(-1));
            DarlingMcpWindowNotice.TestOnlyProbe = () => throw new TimeoutException("the probe's deadline passed");

            var json = await call(ds, Name("probefail"), 168, end, 10);
            var root = WindowFloorLiveHarness.Parse(json);

            Assert.False(root.TryGetProperty("status", out _), json);
            Assert.False(root.TryGetProperty("effective_start", out _));
            Assert.False(root.TryGetProperty("window_truncated", out _));
            Assert.False(root.TryGetProperty("truncation_note", out _));
            Assert.Contains(root.EnumerateObject(), p => p.Value.ValueKind == JsonValueKind.Array && p.Value.GetArrayLength() == 3);
        });
}
