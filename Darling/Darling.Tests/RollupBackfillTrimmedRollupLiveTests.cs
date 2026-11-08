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
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7981: <c>--backfill-rollups</c> on a store whose raw data has an outage gap, after a rollup's own retention
/// has dropped buckets it had already built. TimescaleDB writes no invalidation when a continuous aggregate's
/// retention drops its materialization chunks, so it still counts that range as built and a plain refresh over
/// it does nothing. The verb's slices must reach the source anyway, without falling back to a forced refresh
/// over the whole range after every plain slice has run for nothing.
///
/// <para>A real migrated store with its real continuous aggregates, run through the verb itself twice: once to
/// build full coverage, as the field store had before the gap, and once after the rollup's own retention has
/// trimmed it. The background workers are stopped, so the only retention run is the one the test makes.</para>
///
/// <para><b>#1776 own-store</b>: deliberately NOT <c>[Collection("live-postgres")]</c>. Goes through
/// <see cref="ScratchPostgres.CreateAsync"/> and never touches the shared database's tables.</para>
/// </summary>
public sealed class RollupBackfillTrimmedRollupLiveTests
{
    private const string Raw = "query_store_stats";

    /// <summary>The rollup the field report named: 10-day retention of its own, hierarchical from the 7-day
    /// interval hourly, so its source still reaches raw's oldest row after it has trimmed itself.</summary>
    private const string Trimmed = TimescaleSupport.QueryStoreStatsIntervalDailyView;

    private const int SetupTimeoutSeconds = 120;

    /// <summary>Hours back of the newest and oldest raw row before the outage gap, and of the oldest after it.
    /// The gap (15 days back to 3.5 days back) spans the trimmed rollup's 10-day horizon, as in the field.</summary>
    private const int PreGapNewestHours = 15 * 24;
    private const int PreGapOldestHours = 28 * 24;
    private const int PostGapOldestHours = 84;

    /// <summary>The verb's whole-range fallback, as its output names it. Its presence is the waste #7981 is about:
    /// every plain slice ran and moved nothing, then one forced refresh over the whole range did the work.</summary>
    private const string WholeRangeFallback = "is still short after every slice; escalating to a forced refresh over the range";

    private static readonly Regex SliceLine = new(@"^\s+\[(\d+)/(\d+)\] ", RegexOptions.Compiled | RegexOptions.Multiline);

    [Fact]
    public async Task TrimmedRollupAcrossAnOutageGap_SlicesReachTheSource_WithoutTheWholeRangeFallback()
    {
        await RunAsync(async (connection, connectionString) =>
        {
            var now = await SeedRawWithGapAsync(connection);

            var configPath = Path.Combine(Path.GetTempPath(), $"darling-7981-{Guid.NewGuid():N}.json");
            await File.WriteAllTextAsync(
                configPath,
                $$"""{"postgres":{"managed":false,"connectionString":{{System.Text.Json.JsonSerializer.Serialize(connectionString)}}},"servers":[]}""");

            try
            {
                /* The state before the field report: every rollup built over all of raw, gap included. */
                var (firstExit, firstOut, firstErr) = await RunVerbAsync(configPath);
                Assert.True(firstExit == 0, $"the first backfill must build full coverage.\nSTDOUT:\n{firstOut}\nSTDERR:\n{firstErr}");
                Assert.True(await OldestAsync(connection, Trimmed, "bucket") <= now.AddHours(-PreGapNewestHours),
                    "the first backfill must have built the trimmed rollup over the days before the gap");

                /* The rollup's own retention run, as TimescaleDB runs it once the gate arms it. It drops every
                   materialization chunk older than its horizon, which is every day before the gap, and writes no
                   invalidation for them. */
                await RunOwnRetentionAsync(connection, Trimmed, TimescaleSupport.IntervalDailyRetentionInterval);

                var target = RollupBackfill.Targets.Single(t => string.Equals(t.View, Trimmed, StringComparison.Ordinal));
                var trimmedProbe = await RollupBackfill.ProbeAsync(connection, target.View, target.Source, target.SourceTimeColumn, default);
                Assert.True(trimmedProbe.CoverageOldestUtc >= now.AddHours(-PostGapOldestHours).Date,
                    $"the rollup's own retention must trim it to the first day after the gap; oldest bucket {trimmedProbe.CoverageOldestUtc:O}, now {now:O}");
                Assert.True(trimmedProbe.SourceOldestUtc <= now.AddHours(-PreGapNewestHours),
                    $"the rollup's source {target.Source} must still reach the days before the gap; oldest bucket {trimmedProbe.SourceOldestUtc:O}");

                /* The field run: the verb plans slices from the source's oldest day up to the trimmed floor. */
                var (exit, output, error) = await RunVerbAsync(configPath);
                Assert.True(exit == 0, $"the verb reported failure.\nSTDOUT:\n{output}\nSTDERR:\n{error}");
                Assert.Contains("DONE. Every rollup now covers its own source", output, StringComparison.Ordinal);

                var section = SectionFor(output, Trimmed);
                var sliceLines = SliceLine.Matches(section);
                Assert.True(sliceLines.Count > 0, $"the verb must have run slices for {Trimmed}; its section:\n{section}");
                var plannedSlices = int.Parse(sliceLines[0].Groups[2].Value, System.Globalization.CultureInfo.InvariantCulture);
                var fallbacks = Regex.Matches(section, Regex.Escape(WholeRangeFallback)).Count;

                /* The asserts: the slices themselves moved the coverage back across the gap. Dev runs every
                   plain slice for nothing and then one forced refresh over the whole range, so its refresh count
                   is one more than its slice count and the fallback line is in the section. */
                Assert.True(fallbacks == 0,
                    $"{Trimmed} must reach its source through its slices, not through the whole-range forced fallback; its section:\n{section}");
                Assert.True(sliceLines.Count + fallbacks == plannedSlices,
                    $"{Trimmed} must run exactly its {plannedSlices} planned slices and no refresh beyond them; it ran {sliceLines.Count} slices and {fallbacks} whole-range refreshes; its section:\n{section}");

                var after = await RollupBackfill.ProbeAsync(connection, target.View, target.Source, target.SourceTimeColumn, default);
                Assert.True(after.CoverageOldestUtc is not null && after.CoverageOldestUtc <= after.SourceOldestUtc,
                    $"{Trimmed} starts at {after.CoverageOldestUtc:O}, after its source {target.Source}'s oldest bucket {after.SourceOldestUtc:O}");
                Assert.True(after.CoverageOldestUtc <= now.AddHours(-PreGapNewestHours),
                    $"{Trimmed} must again hold the days before the gap; oldest bucket {after.CoverageOldestUtc:O}");
            }
            finally
            {
                if (File.Exists(configPath))
                {
                    File.Delete(configPath);
                }
            }
        });
    }

    /// <summary>The verb's output for one rollup: from its header line to the next rollup's header, or to the
    /// first unindented line after it (the run's closing DONE or INCOMPLETE).</summary>
    private static string SectionFor(string output, string view)
    {
        var header = $"  {view}:";
        var start = output.IndexOf(header + Environment.NewLine, StringComparison.Ordinal);
        if (start < 0)
        {
            start = output.IndexOf(header + "\n", StringComparison.Ordinal);
        }

        Assert.True(start >= 0, $"the verb's output has no section for {view}:\n{output}");
        var rest = output[(start + header.Length)..];
        var next = Regex.Match(rest, @"^(?:  \S+:\s*$|\S)", RegexOptions.Multiline);
        return next.Success ? rest[..next.Index] : rest;
    }

    private static async Task<(int Exit, string Output, string Error)> RunVerbAsync(string configPath)
    {
        var output = new StringWriter();
        var error = new StringWriter();

        /* The free space is given, as --free-space gives it, so the store may live on another host. */
        var exit = await DarlingCliCommands.BackfillRollupsAsync(configPath, dryRun: false, output, error, default, freeSpaceBytes: 200L * 1024 * 1024 * 1024);
        return (exit, output.ToString(), error.ToString());
    }

    /// <summary>One raw row an hour on both sides of the gap. Returns the hour-aligned "now" the rows were placed
    /// against.</summary>
    private static async Task<DateTime> SeedRawWithGapAsync(NpgsqlConnection connection)
    {
        var now = TimescaleSupport.AlignDown(DateTime.UtcNow, TimeSpan.FromHours(1));
        await using var seed = new NpgsqlCommand($@"
INSERT INTO collect.{Raw}
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us)
SELECT
    n, $1::timestamp - (n * INTERVAL '1 hour'), 1, 'SQL01', 'GapDb', 'dbo.GapProc', '0xABCD',
    7, 9, 'Regular', 'PRIMARY',
    100000 - n, $1::timestamp - (n * INTERVAL '1 hour'), $1::timestamp - (n * INTERVAL '1 hour'),
    10, 1000, 500, 900, 400
FROM generate_series(1, $2) AS n
WHERE n <= $3 OR n >= $4", connection) { CommandTimeout = SetupTimeoutSeconds };
        seed.Parameters.AddWithValue(DateTime.SpecifyKind(now, DateTimeKind.Unspecified));
        seed.Parameters.AddWithValue(PreGapOldestHours);
        seed.Parameters.AddWithValue(PostGapOldestHours);
        seed.Parameters.AddWithValue(PreGapNewestHours);
        await seed.ExecuteNonQueryAsync();
        return now;
    }

    /// <summary>Attaches the rollup's own retention policy with the product's horizon and runs it once.</summary>
    private static async Task RunOwnRetentionAsync(NpgsqlConnection connection, string view, string dropAfter)
    {
        await ExecuteAsync(connection, $"SELECT add_retention_policy('collect.{view}', drop_after => INTERVAL '{dropAfter}', if_not_exists => true)");

        int jobId;
        await using (var read = new NpgsqlCommand($@"SELECT j.job_id FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention' AND j.hypertable_schema = 'collect' AND j.hypertable_name = '{view}'", connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            var value = await read.ExecuteScalarAsync();
            Assert.True(value is int, $"a retention policy must be attached to {view}");
            jobId = (int)value!;
        }

        await ExecuteAsync(connection, $"CALL run_job({jobId})");
    }

    private static async Task<DateTime?> OldestAsync(NpgsqlConnection connection, string relation, string column)
    {
        await using var read = new NpgsqlCommand($"SELECT min({column}) FROM collect.{relation}", connection) { CommandTimeout = SetupTimeoutSeconds };
        return await read.ExecuteScalarAsync() is DateTime oldest ? oldest : null;
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = SetupTimeoutSeconds };
        await command.ExecuteNonQueryAsync();
    }

    private static async Task RunAsync(Func<NpgsqlConnection, string, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #7981 trimmed-rollup backfill test.");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync();
            await PgMigrations.MigrateAsync(connection, default);

            var enabled = await TimescaleSupport.TryEnableAsync(connection, null, default);
            Assert.SkipWhen(!enabled, "The live #7981 trimmed-rollup backfill test needs TimescaleDB.");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, default);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, default);
            /* The store's start holds every aggregate's materialization at one-day chunks. Without it the backfill
               builds TimescaleDB's default ten-day chunks, aligned to the epoch, and the aggregate's 7-day drop then
               keeps a chunk these tests expect gone on some calendar days. */
            var narrowed = await TimescaleSupport.EnsureMaterializationChunkIntervalAsync(connection, null, default);
            Assert.True(narrowed > 0, "the fresh store's aggregates must take one-day materialization chunks, as a store's start gives them");
            await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()");

            await body(connection, scratch.ConnectionString);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
