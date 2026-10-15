/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5858: a held retention policy on a continuous aggregate is measured on the aggregate's materialization
/// hypertable. The job names the aggregate's view, while <c>timescaledb_information.chunks</c> lists the
/// aggregate's chunks under its materialization hypertable, so <see cref="TimescaleSupport.RetentionHoldReadSql"/>
/// read zero chunks and no span for every aggregate policy, and the Retention Held alert never fired for one.
///
/// <para><b>#1776 own-store</b>: deliberately NOT <c>[Collection("live-postgres")]</c>. Goes through
/// <see cref="ScratchPostgres.CreateAsync"/> and never touches the shared database's tables.</para>
/// </summary>
public sealed class RetentionHoldContinuousAggregateLiveTests
{
    private const string Source = "t5858_source";
    private const string Aggregate = "t5858_daily";
    private const int SetupTimeoutSeconds = 60;

    private static async Task<(NpgsqlConnection Connection, ScratchPostgres Scratch)> OpenAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #5858 tests.");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);
        NpgsqlConnection? connection = null;
        /* A skip (no TimescaleDB) or a failed setup throws before the caller owns the store, so the opener drops it
           here. Left alone it would survive to the process-exit drain, which the test runner waits only 10 s for. */
        try
        {
            var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { MaxAutoPrepare = 0 }.ConnectionString;
            connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            await PgMigrations.MigrateAsync(connection, default);

            var enabled = await TimescaleSupport.TryEnableAsync(connection, null, default);
            Assert.SkipWhen(!enabled, "The live #5858 tests need TimescaleDB.");

            /* The scheduler must not run the retention jobs this test creates: a run drops the chunks it counts. */
            await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()");

            return (connection, scratch);
        }
        catch
        {
            if (connection is not null)
            {
                await connection.DisposeAsync();
            }

            await scratch.DisposeAsync();
            throw;
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = SetupTimeoutSeconds };
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = SetupTimeoutSeconds };
        return Convert.ToInt64(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// A plain hypertable holding 30 days, a daily continuous aggregate over it, and a HELD (paused) retention
    /// policy on each. The aggregate's reading counts its materialization's chunks and spans them, and the
    /// evaluator fires Retention Held for it. Before #5858 the aggregate read zero chunks and a null span, so its
    /// ratio was null and the evaluator skipped it. The plain hypertable's reading is unchanged: it still counts
    /// the hypertable's own chunks.
    /// </summary>
    [Fact]
    public async Task RetentionHoldRead_HeldPolicyOnContinuousAggregate_CountsMaterializationChunks_AndFires()
    {
        var ct = TestContext.Current.CancellationToken;
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection, $"CREATE TABLE collect.{Source} (ts timestamp NOT NULL, v integer NOT NULL)");
            await ExecAsync(connection, $"SELECT create_hypertable('collect.{Source}', 'ts', chunk_time_interval => interval '1 day')");
            await ExecAsync(connection,
                $"INSERT INTO collect.{Source} (ts, v) SELECT g, 1 FROM generate_series((now() AT TIME ZONE 'UTC') - interval '30 days', " +
                "now() AT TIME ZONE 'UTC', interval '1 hour') AS g");
            await ExecAsync(connection,
                $"CREATE MATERIALIZED VIEW collect.{Aggregate} WITH (timescaledb.continuous) AS " +
                $"SELECT time_bucket('1 day', ts) AS bucket, count(*) AS n FROM collect.{Source} GROUP BY 1 WITH DATA");

            var aggregateJob = await ScalarAsync(connection, $"SELECT add_retention_policy('collect.{Aggregate}', drop_after => interval '2 days')");
            var sourceJob = await ScalarAsync(connection, $"SELECT add_retention_policy('collect.{Source}', drop_after => interval '4 days')");
            await ExecAsync(connection, $"SELECT alter_job({aggregateJob}, scheduled => false)");
            await ExecAsync(connection, $"SELECT alter_job({sourceJob}, scheduled => false)");

            /* What the catalog itself holds: the aggregate's chunks under its materialization hypertable, and the
               source's under its own name. */
            var materializationChunks = await ScalarAsync(connection,
                "SELECT count(*) FROM timescaledb_information.chunks AS ch " +
                "JOIN timescaledb_information.continuous_aggregates AS ca " +
                "  ON ch.hypertable_schema = ca.materialization_hypertable_schema AND ch.hypertable_name = ca.materialization_hypertable_name " +
                $"WHERE ca.view_schema = 'collect' AND ca.view_name = '{Aggregate}'");
            var sourceChunks = await ScalarAsync(connection,
                $"SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = '{Source}'");
            Assert.True(materializationChunks > 0, "the aggregate must have materialized chunks for this test to mean anything");
            Assert.True(sourceChunks > 0, "the source hypertable must have chunks");

            var readings = await TimescaleSupport.ReadRetentionHoldReadingsAsync(connection, null, ct);

            var aggregate = Assert.Single(readings, r => r.JobId == aggregateJob);
            Assert.Equal(Aggregate, aggregate.HypertableName);  /* still reported by its view name */
            Assert.False(aggregate.Armed);
            Assert.Equal(materializationChunks, aggregate.ChunkCount);
            Assert.NotNull(aggregate.SpanSeconds);
            Assert.True(aggregate.SpanSeconds >= 29L * 86_400, $"the aggregate holds 30 days; span read {aggregate.SpanSeconds} s");
            Assert.Equal(2L * 86_400, aggregate.HorizonSeconds);
            Assert.NotNull(aggregate.OverHorizonRatio);

            var source = Assert.Single(readings, r => r.JobId == sourceJob);
            Assert.Equal(Source, source.HypertableName);
            Assert.Equal(sourceChunks, source.ChunkCount);
            Assert.True(source.SpanSeconds >= 29L * 86_400, $"the source holds 30 days; span read {source.SpanSeconds} s");

            /* The evaluator judges the aggregate's reading: held at well past its 2-day horizon, so it fires. */
            var h = new DarlingSelfAlertTests.Harness();
            var evaluator = h.Build();
            await evaluator.ApplyRetentionHoldsAsync(new[] { aggregate }, ct);

            var fired = Assert.Single(h.Deliverer.Outcomes);
            Assert.Equal(DarlingSelfAlertEvaluator.RetentionHoldMetric, fired.MetricName);
            Assert.Equal($"retentionhold:{aggregateJob}", fired.ServerKey);
            Assert.Contains(Aggregate, fired.ShortMessage, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.RemoveRetentionPolicyAsync(Aggregate, cleanupCt);
                await batch.RemoveRetentionPolicyAsync(Source, cleanupCt);
            });
            await connection.DisposeAsync();
            await scratch.DisposeAsync();
        }
    }
}
