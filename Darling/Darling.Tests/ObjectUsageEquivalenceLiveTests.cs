/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fact mints its own scratch database through ScratchPostgres and touches nothing shared. */

/// <summary>
/// #9349 proof A: the persisted read equals the full fold. 200 seeded histories (30 to 120 snapshots, 5 to 60 objects of 1 to 4 indexes, the events
/// <see cref="ObjectUsageHistoryGenerator"/> lists) live in one scratch store, one database each. Each is built by backfill and again one snapshot
/// per call, and the two give identical chain tables. Then for each window start of the design's set the persisted read and the full fold give
/// record-equal answers, or the persisted read reports a fallback. Histories not tagged as fallback cases never fall back, and the tagged ones do.
/// </summary>
public sealed class ObjectUsageEquivalenceLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #9349 equivalence proof.";
    private const int Histories = 200;
    private const int ServerId = -934920;
    private const string ServerName = "equiv-sql1";
    private const string ComparesWrongSnapshot = "the window compares a snapshot the chain step did not";
    private const string RestartDiffers = "the window's restart decision differs from the chain step's";
    private const string NoSnapshot = "the window holds no snapshot";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed record Outcome(string Database, HistoryTag Tag, DateTime Start, string Kind, string? Reason, List<string> Differences, ObjectUsageWindowResult Full);

    [Fact]
    public async Task ThePersistedRead_IsRecordEqualToTheFullFold_OverGeneratedHistories_AndAChainBuiltOneSnapshotAtATimeIsTheBackfilledChain()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        var parallel = new ParallelOptions { MaxDegreeOfParallelism = Math.Clamp(Environment.ProcessorCount, 2, 8), CancellationToken = ct };
        var stats = new List<string>();

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var histories = Enumerable.Range(0, Histories).Select(ObjectUsageHistoryGenerator.Generate).ToList();
        await ObjectUsageHistoryGenerator.PlantAsync(connection, ServerId, ServerName, histories, ct);
        stats.Add($"seed: {histories.Sum(h => h.Rows.Count)} rows, {histories.Sum(h => h.Times.Count)} snapshots");

        /* The chain by backfill, then deleted and built again one snapshot per call: the same rows. */
        var backfilled = new ConcurrentDictionary<string, (string, string, string)>();
        await Parallel.ForEachAsync(histories, parallel, async (history, token) =>
        {
            await ObjectUsageChainBuilder.BuildPairAsync(dataSource, ServerId, history.Database, int.MaxValue, token);
            backfilled[history.Database] = await FingerprintAsync(dataSource, history.Database, token);
        });
        stats.Add("backfill: done");

        var chainSize = await ScalarAsync(connection, "SELECT (SELECT count(*) FROM collect.object_usage_snapshots) + (SELECT count(*) FROM collect.object_usage_deltas) + (SELECT count(*) FROM collect.object_usage_runs)", ct);
        var rawSize = await ScalarAsync(connection, "SELECT count(*) FROM collect.index_object_stats", ct);
        var deltaSize = await ScalarAsync(connection, "SELECT count(*) FROM collect.object_usage_deltas", ct);
        stats.Add($"sizes: index_object_stats {rawSize} rows, chain tables {chainSize} rows (deltas {deltaSize})");

        var mismatches = new ConcurrentBag<string>();
        await Parallel.ForEachAsync(histories, parallel, async (history, token) =>
        {
            await using (var cleanup = await dataSource.OpenConnectionAsync(token))
            {
                foreach (var table in new[] { ObjectUsageChain.DeltasTable, ObjectUsageChain.RunsTable, ObjectUsageChain.SnapshotsTable })
                {
                    await using var delete = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = $1 AND database_name = $2", cleanup);
                    delete.Parameters.AddWithValue(ServerId);
                    delete.Parameters.AddWithValue(history.Database);
                    await delete.ExecuteNonQueryAsync(token);
                }
            }

            int built;
            do
            {
                built = await ObjectUsageChainBuilder.BuildPairAsync(dataSource, ServerId, history.Database, 1, token);
            }
            while (built > 0);

            if (await FingerprintAsync(dataSource, history.Database, token) != backfilled[history.Database])
            {
                mismatches.Add(history.Database);
            }
        });
        stats.Add("one snapshot per call: done");
        Assert.True(mismatches.IsEmpty, "a chain built one snapshot per call differs from the backfilled chain for: " + string.Join(", ", mismatches.Order().Take(10)));

        var work = histories.SelectMany(h => Starts(h).Select(s => (History: h, s.Start, s.Kind))).ToList();
        var outcomes = new ConcurrentBag<Outcome>();
        await Parallel.ForEachAsync(work, parallel, async (item, token) =>
        {
            var db = item.History.Database;
            var full = await DarlingObjectUsageReader.ReadTablesFullAsync(dataSource, ServerId, db, item.Start, token);
            var (persisted, reason) = await DarlingObjectUsageReader.TryReadPersistedAsync(dataSource, ServerId, db, item.Start, token);
            var differences = persisted is null ? new List<string>() : ObjectUsageCompare.Differences(full, persisted);
            if (persisted is null)
            {
                /* The read the tool makes on a fallback is the full fold's. */
                differences = ObjectUsageCompare.Differences(full, await DarlingObjectUsageReader.ReadTablesAsync(dataSource, ServerId, db, item.Start, token));
            }

            outcomes.Add(new Outcome(db, item.History.Tag, item.Start, item.Kind, reason, differences, full));
        });
        stats.Add($"windows: {outcomes.Count} compared, {outcomes.Count(o => o.Reason is not null)} fell back");

        /* Coverage: the generated histories do reach every kind of step. Counted on the window that starts at each history's first snapshot. */
        var whole = outcomes.Where(o => o.Kind == "first").ToList();
        int Count(Func<ObjectUsageWindowResult, bool> has) => whole.Count(o => has(o.Full));
        int WithReason(ObjectUsageResetReasons reason) => Count(f => f.Resets.Any(r => (r.Reasons & reason) != ObjectUsageResetReasons.None));
        var coverage = new Dictionary<string, int>
        {
            ["restart"] = WithReason(ObjectUsageResetReasons.Restart),
            ["node changed"] = WithReason(ObjectUsageResetReasons.NodeChanged),
            ["node baseline"] = WithReason(ObjectUsageResetReasons.NodeBaseline),
            ["counter decreased"] = WithReason(ObjectUsageResetReasons.CounterDecreased),
            ["last access cleared"] = WithReason(ObjectUsageResetReasons.LastAccessCleared),
            ["lone index clear"] = Count(f => f.IndexResets > 0),
            ["dropped object"] = Count(f => f.Objects.Any(o => o.LastSnapshot < f.CoveredTo)),
            ["late object"] = Count(f => f.Objects.Any(o => o.FirstSnapshot > f.CoveredFrom)),
            ["two named nodes"] = Count(f => f.AnsweringServerNames.Count == 2),
            ["memory optimized"] = Count(f => f.Objects.Any(o => o.IsMemoryOptimized == true)),
        };
        stats.Add("coverage (histories out of " + whole.Count + "): " + string.Join(", ", coverage.Select(c => $"{c.Key} {c.Value}")));
        stats.Add("features: " + string.Join(", ", ObjectUsageHistoryGenerator.FeatureNames.Select(
            f => $"{f} {histories.Count(h => h.Features.Contains(f))}")));
        if (Environment.GetEnvironmentVariable("DARLING_EQUIV_STATS_FILE") is { Length: > 0 } statsFile)
        {
            await File.WriteAllLinesAsync(statsFile, stats, ct);
        }

        var all = string.Join(Environment.NewLine, stats);
        Assert.True(whole.Count == Histories, "every history gets its first-snapshot window. " + all);
        var wrong = outcomes.Where(o => o.Differences.Count > 0).OrderBy(o => o.Database).ThenBy(o => o.Start).Take(5).ToList();
        Assert.True(
            wrong.Count == 0,
            "the read differs from the full fold: " + string.Join(" | ", wrong.Select(o => $"{o.Database} from {o.Start:O} ({o.Kind}, reason {o.Reason ?? "none"}): {o.Differences[0]}")));

        /* A start after the newest snapshot's reach has no baseline, so the read has nothing to replay and takes the full fold (also empty). Any other
           fallback is a fault, except in the tagged histories, which must fall back in the way the tag says. */
        var untaggedFallbacks = outcomes.Where(o => o.Tag == HistoryTag.None && o.Reason is not null && o.Kind != "after-reach").ToList();
        Assert.True(
            untaggedFallbacks.Count == 0,
            "histories not tagged as fallback cases fell back: " + string.Join(" | ", untaggedFallbacks.OrderBy(o => o.Database).Take(5).Select(o => $"{o.Database} from {o.Start:O} ({o.Kind}): {o.Reason}")));
        Assert.All(outcomes.Where(o => o.Kind == "after-reach"), o => Assert.Equal(NoSnapshot, o.Reason));

        foreach (var (tag, expected) in new[] { (HistoryTag.UnnamedAfterNamed, ComparesWrongSnapshot), (HistoryTag.StartDrift, RestartDiffers) })
        {
            var tagged = outcomes.Where(o => o.Tag == tag && o.Kind != "after-reach").ToList();
            Assert.NotEmpty(tagged);
            foreach (var group in tagged.GroupBy(o => o.Database))
            {
                Assert.True(group.Any(o => o.Reason == expected), $"{group.Key} ({tag}) never fell back with '{expected}'");
                Assert.All(group.Where(o => o.Reason is not null), o => Assert.Equal(expected, o.Reason));
            }
        }

        Assert.All(outcomes.Where(o => o.Tag == HistoryTag.Empty), o => Assert.Equal(NoSnapshot, o.Reason));

        foreach (var (name, count) in coverage)
        {
            Assert.True(count >= 5, $"the generator reached '{name}' in only {count} histories. {all}");
        }
    }

    /// <summary>The design's window starts for one history, one start for each baseline it can reach (a start an hour either side of a snapshot
    /// that is a day from its neighbours has the same baseline as the snapshot itself, so the three forms rotate over the snapshots), and the
    /// edge cases: before the first snapshot, after the newest, and inside a gap longer than the baseline's reach.</summary>
    private static IEnumerable<(DateTime Start, string Kind)> Starts(GeneratedHistory history)
    {
        var times = history.Times;
        if (times.Count == 0)
        {
            yield return (new DateTime(2026, 7, 1, 0, 0, 0, DateTimeKind.Unspecified), "first");
            yield return (new DateTime(2026, 7, 1, 0, 0, 0, DateTimeKind.Unspecified).AddDays(-20), "before-first");
            yield break;
        }

        int Baseline(DateTime start)
        {
            var near = -1;
            for (var i = 0; i < times.Count; i++)
            {
                if (times[i] <= start && times[i] > start.AddDays(-ObjectUsageWindowRead.BaselineReachDays))
                {
                    near = i;
                }
            }

            return near >= 0 ? near : times.ToList().FindIndex(t => t >= start);
        }

        var seen = new HashSet<int> { Baseline(times[0]) };
        yield return (times[0], "first");
        for (var i = 0; i < times.Count; i++)
        {
            var t = times[i];
            var forms = new[] { (t, "at"), (t.AddHours(-1), "before"), (t.AddHours(1), "after") };
            for (var k = 0; k < 3; k++)
            {
                var (start, kind) = forms[(i + k) % 3];
                if (seen.Add(Baseline(start)))
                {
                    yield return (start, kind);
                    break;
                }
            }
        }

        yield return (times[0].AddDays(-2), "before-first");
        yield return (times[^1].AddDays(3), "after-newest");
        yield return (times[^1].AddDays(ObjectUsageWindowRead.BaselineReachDays + 1), "after-reach");
        if (history.GapBefore > 0)
        {
            yield return (times[history.GapBefore - 1].AddDays(ObjectUsageWindowRead.BaselineReachDays + 0.5), "in-gap");
        }
    }

    private static async Task<(string, string, string)> FingerprintAsync(NpgsqlDataSource dataSource, string database, CancellationToken ct)
    {
        async Task<string> Text(string table, string order)
        {
            await using var command = dataSource.CreateCommand(
                $"SELECT coalesce(string_agg((to_jsonb(t) - 'server_id' - 'built_at')::text, E'\\n' ORDER BY {order}), '') FROM {table} AS t WHERE server_id = $1 AND database_name = $2");
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(database);
            return (string)(await command.ExecuteScalarAsync(ct))!;
        }

        return (
            await Text(ObjectUsageChain.SnapshotsTable, "collection_time"),
            await Text(ObjectUsageChain.DeltasTable, "collection_time, object_id, index_id"),
            await Text(ObjectUsageChain.RunsTable, "object_id, index_id, first_time"));
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }
}
