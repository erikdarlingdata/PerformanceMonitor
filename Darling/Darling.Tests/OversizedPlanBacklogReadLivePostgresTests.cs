/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test here mints its own scratch database through ScratchPostgres and touches nothing
   on the shared one, so serializing it against the live-postgres collection would cost suite time and buy no
   isolation. The percentile demonstration below could have run as a bare SELECT over a VALUES list on the
   shared database — it reads no table and takes no lock on one — but it gets a scratch database anyway, so
   that this class's exemption rests on a property of the whole file rather than on a judgement about one
   statement that the next test added here would silently inherit. */

/// <summary>
/// The half of #3398 that no source pin reaches: whether the three statements PARSE, and whether the verdict
/// buckets really partition every row state — including the two the field has never produced.
///
/// <para><c>expired_at</c> was zero on both production stores the day after the V121 install, so every
/// expiry-shaped branch of the read was untested by the fleet and would have stayed that way. A row carrying
/// BOTH verdict stamps cannot be produced on demand at all: a capture clears a standing expiry, so reaching
/// that state means a sighting between two sweep passes. Both are seeded here as SQL rather than driven
/// through the writer, which is the only way to reach them.</para>
///
/// <para>Split out of <see cref="OversizedPlanBacklogReadPins"/> rather than living beside its thirteen pure
/// pins, the shape <c>LivePostgresCollectionHygieneTests</c> recommends: a class that reaches the store drags
/// its whole file into a serialization decision, and thirteen text pins have no business paying for
/// one.</para>
/// </summary>
public sealed class OversizedPlanBacklogReadLivePostgresTests
{
    private const int RegisteredServerId = 4001;
    private const int UnregisteredServerId = 4002;

    /// <summary>
    /// The six row states. Written as SQL rather than through
    /// <see cref="OversizedPlanBacklog.RecordSightingsAsync"/> and the sweep, because two of them are not
    /// reachable that way — see this class's remarks.
    /// </summary>
    private const string FixtureSql = @"
INSERT INTO collect.servers (server_id, server_name, is_enabled)
VALUES (4001, 'fixture-server', TRUE);

INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at,
 plan_xml, captured_at, expired_at, last_attempt_at, attempt_count)
VALUES
    /* pending, never attempted */
    (4001, 'query_stats', '0xPENDING_NEW', '0xSQL1', 0, 400, 'fixture_db', '0xHASH1',
     1000000, '2026-09-10 08:00:00', '2026-09-13 08:00:00', NULL, NULL, NULL, NULL, 0),
    /* pending, attempted twice and still nothing learned about the handle */
    (4001, 'query_stats', '0xPENDING_TRIED', '0xSQL2', 0, 400, 'fixture_db', '0xHASH2',
     900000, '2026-09-10 08:00:00', '2026-09-13 08:00:00', NULL, NULL, NULL, '2026-09-13 10:00:00', 2),
    /* captured, content held */
    (4001, 'query_stats', '0xCAPTURED', '0xSQL3', 0, 400, 'fixture_db', '0xHASH3',
     800000, '2026-09-10 08:00:00', '2026-09-13 08:00:00', '<ShowPlanXML />', '2026-09-13 12:00:00', NULL,
     '2026-09-13 12:00:00', 1),
    /* BOTH stamps, and no content: the state the plan fallbacks cannot serve */
    (4001, 'procedure_stats', '0xCAPTURED_EMPTY', '0xSQL4', 0, 0, 'fixture_db', NULL,
     700000, '2026-09-10 08:00:00', '2026-09-13 08:00:00', NULL, '2026-09-13 09:00:00', '2026-09-13 07:00:00',
     '2026-09-13 09:00:00', 1),
    /* expired: a fetch established that the handle no longer renders a plan */
    (4001, 'procedure_stats', '0xEXPIRED', '0xSQL5', 0, 0, 'fixture_db', NULL,
     600000, '2026-09-10 08:00:00', '2026-09-13 08:00:00', NULL, NULL, '2026-09-13 11:00:00',
     '2026-09-13 11:00:00', 1),
    /* a server that is not in the registry */
    (4002, 'query_stats', '0xORPHAN', '0xSQL6', 0, 400, 'fixture_db', '0xHASH6',
     650000, '2026-09-10 08:00:00', '2026-09-13 08:00:00', NULL, NULL, NULL, NULL, 0);";

    /// <summary>
    /// The partition, the registry join, the listing's agreement with the rollup, and — the assertion a text
    /// pin cannot make — <see cref="OversizedPlanBacklog.ClaimSql"/> taking exactly the rows the read calls
    /// pending.
    /// </summary>
    [Fact]
    public async Task TheRollupPartitionsEveryRowState_AndTheClaimAgreesWithPending()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live oversized-plan backlog read test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, null, ct);
            await ExecuteAsync(migrate, FixtureSql, ct);
        }

        /* The reads resolve `servers` through the search path, exactly as the MCP host's pool does. */
        await using var postgres = NpgsqlDataSource.Create(ReadConnectionString(scratch));

        var census = await DarlingOversizedPlanBacklogReader.GetCollectorCensusAsync(postgres, null, ct);
        var servers = await DarlingOversizedPlanBacklogReader.GetPerServerRollupAsync(postgres, null, ct);

        Assert.Equal(
            new[] { OversizedPlanBacklog.ProcedureStatsCollectorName, OversizedPlanBacklog.QueryStatsCollectorName },
            census.Select(c => c.CollectorName).OrderBy(n => n, StringComparer.Ordinal).ToArray());

        Assert.Equal(2, servers.Count);

        /* THE partition, on both grains. Nothing may be counted twice and nothing may be missed. */
        foreach (var c in census)
        {
            Assert.Equal(c.TotalRows, c.PendingRows + c.CapturedRows + c.ExpiredRows);
        }

        foreach (var s in servers)
        {
            Assert.Equal(s.TotalRows, s.PendingRows + s.CapturedRows + s.ExpiredRows);
        }

        Assert.Equal(6L, servers.Sum(s => s.TotalRows));

        var registered = servers.Single(s => s.ServerId == RegisteredServerId);
        var orphaned = servers.Single(s => s.ServerId == UnregisteredServerId);

        /* A row whose server has left the registry is still a row in the table. An inner join would have
           dropped it and reported a five-row backlog. */
        Assert.Equal("fixture-server", registered.ServerName);
        Assert.Null(orphaned.ServerName);
        Assert.Equal(1L, orphaned.TotalRows);

        /* The both-stamps row lands in captured and NOT in expired: the five rows on the registered server
           are 2 pending + 2 captured (one of them also carrying an expiry) + 1 expired. */
        Assert.Equal(5L, registered.TotalRows);
        Assert.Equal(2L, registered.PendingRows);
        Assert.Equal(2L, registered.CapturedRows);
        Assert.Equal(1L, registered.ExpiredRows);

        /* rows_with_content is not captured_rows: one captured row carries no plan_xml, which is precisely
           the state get_plan_xml cannot serve and the rollup would otherwise call done. */
        Assert.Equal(1L, registered.RowsWithContent);

        /* Attempts are counted on STILL-PENDING rows only — a captured row's attempt was not a failure. */
        Assert.Equal(1L, registered.PendingRowsAttempted);
        Assert.Equal(2L, registered.PendingAttempts);
        Assert.Equal(2, registered.MaxAttemptsOnAPendingRow);

        /* The two verdict clocks, which are the whole point: they move only when the sweep decides. */
        Assert.Equal<DateTime?>(new DateTime(2026, 9, 13, 12, 0, 0), registered.LastCapturedAt);
        Assert.Equal<DateTime?>(new DateTime(2026, 9, 13, 11, 0, 0), registered.LastExpiredAt);

        /* A discrete median is one of the seeded sizes, never an interpolation between two of them. */
        Assert.Contains(registered.MedianObservedBytes, new long[] { 600_000, 700_000, 800_000, 900_000, 1_000_000 });
        Assert.Equal(600_000L, registered.MinObservedBytes);
        Assert.Equal(1_000_000L, registered.MaxObservedBytes);

        /* Every seeded size is over the cap, which is what makes these rows exist at all. */
        Assert.True(registered.MinObservedBytes > QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes);

        /* The listing's per-row verdict reproduces the rollup's counts exactly. */
        var rows = await DarlingOversizedPlanBacklogReader.GetRowsAsync(postgres, RegisteredServerId, 100, ct);
        Assert.Equal(5, rows.Count);
        Assert.Equal(
            registered.PendingRows,
            (long)rows.Count(r => r.Verdict == DarlingOversizedPlanBacklogReader.VerdictPending));
        Assert.Equal(
            registered.CapturedRows,
            (long)rows.Count(r => r.Verdict == DarlingOversizedPlanBacklogReader.VerdictCaptured));
        Assert.Equal(
            registered.ExpiredRows,
            (long)rows.Count(r => r.Verdict == DarlingOversizedPlanBacklogReader.VerdictExpired));

        /* Largest first, and the cap applies. */
        Assert.Equal(1_000_000L, rows[0].ObservedBytes);
        Assert.Equal(3, (await DarlingOversizedPlanBacklogReader.GetRowsAsync(postgres, RegisteredServerId, 3, ct)).Count);

        /* AND the claim agrees. This is the assertion the text pin cannot make: the sweep's own statement,
           run against the same fixture, takes exactly the rows the read calls pending. */
        var claimed = new List<string>();
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            await using var command = new NpgsqlCommand(OversizedPlanBacklog.ClaimSql(100), connection);
            command.Parameters.AddWithValue(RegisteredServerId);
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                claimed.Add(reader.GetString(1));
            }
        }

        Assert.Equal(
            rows.Where(r => r.Verdict == DarlingOversizedPlanBacklogReader.VerdictPending)
                .Select(r => r.PlanHandle).OrderBy(h => h, StringComparer.Ordinal).ToArray(),
            claimed.OrderBy(h => h, StringComparer.Ordinal).ToArray());

        /* Server scoping narrows BOTH grains, rather than filtering only the listing. */
        var scoped = await DarlingOversizedPlanBacklogReader.GetPerServerRollupAsync(postgres, RegisteredServerId, ct);
        Assert.Equal(RegisteredServerId, scoped.Single().ServerId);
        Assert.Equal(
            5L,
            (await DarlingOversizedPlanBacklogReader.GetCollectorCensusAsync(postgres, RegisteredServerId, ct))
                .Sum(c => c.TotalRows));
    }

    /// <summary>
    /// The trap the median's shape exists to dodge, demonstrated rather than remembered: PostgreSQL has no
    /// <c>round(double precision, integer)</c>, so the shape a reviewer would reach for to "tidy" an
    /// interpolated median fails the WHOLE statement and takes every other column with it.
    ///
    /// <para>This asserts a property of the store rather than of our code, which is the point — it is what
    /// makes <c>OversizedPlanBacklogReadPins.TheMedianIsADiscretePercentile_AndNothingRoundsADoublePrecisionValue</c>
    /// a rule with a measured reason instead of a preference, and it fails loudly if a future PostgreSQL adds
    /// the overload and the rule stops being necessary.</para>
    /// </summary>
    [Fact]
    public async Task TheContinuousPercentileWithATwoArgumentRound_IsRejectedByPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live percentile/round demonstration.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        const string Series = "FROM (VALUES (1::bigint), (2), (4)) AS t(v)";

        /* The discrete form returns a member of the series, typed as the column is. */
        await using (var ok = new NpgsqlCommand(
            "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY v) " + Series, connection))
        {
            Assert.Equal(2L, Convert.ToInt64(await ok.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));
        }

        /* The continuous form rounded to two places does not resolve, at all. */
        Exception? thrown = null;
        await using (var bad = new NpgsqlCommand(
            "SELECT round(percentile_cont(0.5) WITHIN GROUP (ORDER BY v), 2) " + Series, connection))
        {
            try
            {
                await bad.ExecuteScalarAsync(ct);
            }
            catch (Exception ex)
            {
                thrown = ex;
            }
        }

        Assert.NotNull(thrown);
        Assert.Contains("round", thrown!.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The scratch database's connection string with the store's own search path, so the reads resolve the
    /// bare <c>servers</c> name the way the MCP host's pool does.
    /// </summary>
    private static string ReadConnectionString(ScratchPostgres scratch) =>
        new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString;

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
