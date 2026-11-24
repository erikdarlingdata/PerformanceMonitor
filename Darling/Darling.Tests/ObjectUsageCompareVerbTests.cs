/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every live fact mints its own scratch database through ScratchPostgres and touches nothing shared. */

/// <summary>
/// #9349 proof C: the <c>--object-usage-compare</c> verb. The arguments, the verb's place in the allow-list and the help text, the difference text, and
/// (live) a seeded store: identical, then a stored delta changed by hand, then no chain. The verb reads only: the row counts of every table are the
/// same after each run.
/// </summary>
public sealed class ObjectUsageCompareVerbTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #9349 compare verb tests.";
    private const int ServerId = -934921;
    private const string ServerName = "compare-sql1";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void TheVerb_IsKnown_AndInTheHelp_AndTakesAServerADatabaseAndOptionallyDays()
    {
        Assert.True(DarlingCliCommands.IsKnownVerb("--object-usage-compare"));
        Assert.Contains("--object-usage-compare", DarlingCliCommands.UsageText(), StringComparison.Ordinal);

        Assert.True(DarlingCliCommands.TryParseObjectUsageCompareArgs(["s1", "Sales"], out var plain, out _));
        Assert.Equal(("s1", "Sales", 90, null), (plain!.ServerName, plain.DatabaseName, plain.Days, plain.ConfigPath));
        Assert.True(DarlingCliCommands.TryParseObjectUsageCompareArgs(["s1", "Sales", "30", "--config", "c.json"], out var full, out _));
        Assert.Equal((30, "c.json"), (full!.Days, full.ConfigPath));

        foreach (var bad in new[] { new[] { "s1" }, ["s1", "Sales", "0"], ["s1", "Sales", "x"], ["s1", "Sales", "30", "40"], ["s1", "Sales", "--bogus"], ["s1", "Sales", "--config"] })
        {
            Assert.False(DarlingCliCommands.TryParseObjectUsageCompareArgs(bad, out _, out var message), string.Join(' ', bad));
            Assert.False(string.IsNullOrEmpty(message));
        }
    }

    [Fact]
    public void Differences_AreNamedByField_AndNothingDiffersFromItself()
    {
        var day = new DateTime(2026, 10, 1, 6, 0, 0);
        var reset = new ObjectUsageReset(day.AddDays(2), ObjectUsageResetReasons.Restart, "a", "a", day.AddDays(1));
        var objects = new[]
        {
            new ObjectUsageTotals(10, "dbo", "Orders", false, 100, 5, day, day, day, day.AddDays(3), 2),
            new ObjectUsageTotals(20, "dbo", "Quiet", false, 0, 0, null, null, day, day.AddDays(3), 1),
        };
        var full = new ObjectUsageWindowResult(day, day.AddDays(3), 4, TimeSpan.FromDays(1), [reset], ["a"], objects, 0, reset.BlindTime);

        Assert.Empty(ObjectUsageCompare.Differences(full, full with { }));

        var changed = full with
        {
            SnapshotCount = 5,
            Objects = [objects[0] with { Reads = 101 }, objects[1]],
            Resets = [],
        };
        var differences = ObjectUsageCompare.Differences(full, changed);
        Assert.Contains(differences, d => d.StartsWith("SnapshotCount:", StringComparison.Ordinal));
        Assert.Contains(differences, d => d.StartsWith("Resets[0]:", StringComparison.Ordinal));
        var object10 = Assert.Single(differences, d => d.StartsWith("Object 10:", StringComparison.Ordinal));
        Assert.Contains("Reads = 100", object10, StringComparison.Ordinal);
        Assert.Contains("Reads = 101", object10, StringComparison.Ordinal);
        Assert.DoesNotContain(differences, d => d.StartsWith("Object 20:", StringComparison.Ordinal));

        var reordered = full with { Objects = [objects[1], objects[0]] };
        Assert.Equal("Objects: the same objects in a different order", Assert.Single(ObjectUsageCompare.Differences(full, reordered)));
    }

    [Fact]
    public async Task OnASeededStore_TheVerbSaysIdentical_ThenNamesAChangedDelta_ThenSaysTheChainCannotAnswer_AndWritesNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        /* History 0 carries the restart feature: its chain holds resets, deltas and runs. */
        var history = ObjectUsageHistoryGenerator.Generate(0);
        Assert.Contains("restart", history.Features);
        await ObjectUsageHistoryGenerator.PlantAsync(connection, ServerId, ServerName, [history], ct);
        Assert.True(await ObjectUsageChainBuilder.BuildPairAsync(dataSource, ServerId, history.Database, int.MaxValue, ct) > 0);
        var windowStart = history.Times[0];

        var before = await CountsAsync(connection, ct);
        var identical = new StringWriter();
        Assert.Equal(0, await DarlingCliCommands.ObjectUsageCompareAsync(dataSource, ServerId, ServerName, history.Database, windowStart, identical, ct));
        Assert.Contains("identical", identical.ToString(), StringComparison.Ordinal);
        Assert.Contains("persisted path", identical.ToString(), StringComparison.Ordinal);
        Assert.Contains("every snapshot", identical.ToString(), StringComparison.Ordinal);
        Assert.Equal(before, await CountsAsync(connection, ct));

        /* One stored delta, the first one after the baseline, is changed by hand. */
        long changedObject;
        await using (var change = new NpgsqlCommand(
            @"UPDATE collect.object_usage_deltas SET reads = reads + 7
WHERE ctid = (SELECT ctid FROM collect.object_usage_deltas WHERE server_id = $1 AND database_name = $2 AND collection_time > $3 ORDER BY collection_time, object_id, index_id LIMIT 1)
RETURNING object_id", connection))
        {
            change.Parameters.AddWithValue(ServerId);
            change.Parameters.AddWithValue(history.Database);
            change.Parameters.AddWithValue(windowStart);
            changedObject = Convert.ToInt64(await change.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
        }

        before = await CountsAsync(connection, ct);

        var differs = new StringWriter();
        Assert.Equal(DarlingCliCommands.ObjectUsageCompareDiffers, await DarlingCliCommands.ObjectUsageCompareAsync(
            dataSource, ServerId, ServerName, history.Database, windowStart, differs, ct));
        var lines = differs.ToString().Split('\n').Select(l => l.TrimEnd('\r')).ToList();
        Assert.DoesNotContain(lines, l => l.StartsWith("identical", StringComparison.Ordinal));
        var difference = Assert.Single(lines, l => l.StartsWith("  Object ", StringComparison.Ordinal));
        Assert.StartsWith($"Object {changedObject}:", difference.TrimStart(), StringComparison.Ordinal);
        Assert.Equal(before, await CountsAsync(connection, ct));

        /* No chain rows at all: nothing to compare. */
        foreach (var table in new[] { ObjectUsageChain.DeltasTable, ObjectUsageChain.RunsTable, ObjectUsageChain.SnapshotsTable })
        {
            await using var delete = new NpgsqlCommand($"DELETE FROM {table}", connection);
            await delete.ExecuteNonQueryAsync(ct);
        }

        var none = new StringWriter();
        Assert.Equal(DarlingCliCommands.ObjectUsageCompareNoChain, await DarlingCliCommands.ObjectUsageCompareAsync(
            dataSource, ServerId, ServerName, history.Database, windowStart, none, ct));
        Assert.Contains("cannot answer", none.ToString(), StringComparison.Ordinal);
    }

    private static async Task<string> CountsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            @"SELECT (SELECT count(*) FROM collect.index_object_stats) || '/' || (SELECT count(*) FROM collect.object_usage_snapshots) || '/' ||
(SELECT count(*) FROM collect.object_usage_deltas) || '/' || (SELECT count(*) FROM collect.object_usage_runs) || '/' ||
(SELECT coalesce(sum(reads), 0) FROM collect.object_usage_deltas)", connection);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }
}
