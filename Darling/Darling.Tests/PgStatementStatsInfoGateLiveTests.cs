/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3818 against a LIVE PostgreSQL TARGET - not the store: gated on <c>DARLING_TEST_PG_TARGET</c>, a superuser
/// connection to a PostgreSQL with <c>pg_stat_statements</c> in <c>shared_preload_libraries</c>, which the
/// live-store cluster (<c>DARLING_TEST_PG</c>, TimescaleDB preloaded) is not. Written against
/// <c>postgres:14</c> with the extension created at <c>VERSION '1.8'</c>: the extension installed, preloaded,
/// its base view readable, and no <c>pg_stat_statements_info</c> because nobody ran
/// <c>ALTER EXTENSION ... UPDATE</c> after the engine upgrade. That is one of the two states in which the
/// column is absent, and #3818 assumed it was the state of the 23 clusters it counted; #3830 read
/// <c>pg_extension_availability</c> and found the other one - no <c>pg_extension</c> row anywhere. The rig
/// proves the gate holds in this state; the gate is on the RELATION, so it holds in both.
///
/// <para>The claims, in the order the rig proved them: at 1.8 the epoch read is NULL and does not raise -
/// alone, and inside the collector's whole statement over a readable base view; after <c>UPDATE</c> it
/// fills, through the collector's own <c>BuildQuery</c> text, with the same instant a direct read of the view
/// returns; and with the extension created in a schema OFF the search_path it still fills, because the
/// schema is read from <c>pg_extension</c> rather than assumed. The first cut's static
/// <c>FROM public.pg_stat_statements_info</c> failed the first and third of these; the issue's proposed
/// <c>WHERE to_regclass(...) IS NOT NULL</c> failed the first (a FROM item is resolved at parse analysis).</para>
///
/// <para>The 1.8 whole-statement arm builds the query for major 13, deliberately: that is the query text
/// whose column set matches a 1.8 view (no <c>toplevel</c>), and the point of the arm is that the EPOCH
/// column does not raise where the base view is readable. The server major's role as a proxy for the
/// extension's version on <c>toplevel</c> is the collector comment's separate, recorded caveat.</para>
/// </summary>
public sealed class PgStatementStatsInfoGateLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG_TARGET");

    [Fact]
    public async Task TheEpochRead_IsNullBelow19_FillsAfterUpdate_AndFindsTheExtensionOutsidePublic_AgainstALivePostgresTarget()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG_TARGET to a superuser connection string for a PostgreSQL with pg_stat_statements preloaded to run the live #3818 proof.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        var major = connection.PostgreSqlVersion.Major;
        Assert.True(major >= 13, $"the proof needs an engine that ships pg_stat_statements 1.8; this is {major}");

        try
        {
            await ExecAsync(connection, "DROP EXTENSION IF EXISTS pg_stat_statements; DROP SCHEMA IF EXISTS ext CASCADE;", ct);

            /* ---- 1.8: the state of the failing clusters. ---- */
            await ExecAsync(connection, "CREATE EXTENSION pg_stat_statements VERSION '1.8'", ct);
            Assert.Equal("1.8", await ScalarAsync<string>(connection, "SELECT extversion FROM pg_extension WHERE extname = 'pg_stat_statements'", ct));
            Assert.Null(await ScalarAsync<string>(connection, "SELECT to_regclass('public.pg_stat_statements_info')::text", ct));

            /* Alone: NULL, and - the whole point - no 42P01. */
            Assert.Null(await ScalarAsync<object>(connection, "SELECT " + PgStatementStatsCollector.StatementsEpochSql, ct));

            /* Inside the collector's whole statement over a readable base view. Something has to have run so
               `calls > 0` matches, and the reads above already did. */
            var rows18 = await ReadAsync(connection, Sql(major: 13), ct);
            Assert.True(rows18.Count > 0, "the base view was readable and had rows; the statement must return them");
            Assert.Null(rows18.Epoch);

            /* ---- ALTER EXTENSION UPDATE: the one-statement remedy. ---- */
            await ExecAsync(connection, "ALTER EXTENSION pg_stat_statements UPDATE", ct);
            var direct = await ScalarAsync<DateTime>(connection, "SELECT stats_reset FROM public.pg_stat_statements_info", ct);

            var rowsCurrent = await ReadAsync(connection, Sql(major), ct);
            Assert.True(rowsCurrent.Count > 0);
            Assert.NotNull(rowsCurrent.Epoch);
            Assert.Equal(direct, rowsCurrent.Epoch!.Value);
            Assert.Equal(DateTimeKind.Utc, rowsCurrent.Epoch.Value.Kind);

            /* ---- Outside public, off the search_path: the second failure mode. ---- */
            await ExecAsync(connection, "DROP EXTENSION pg_stat_statements; CREATE SCHEMA ext; CREATE EXTENSION pg_stat_statements SCHEMA ext;", ct);
            Assert.Null(await ScalarAsync<string>(connection, "SELECT to_regclass('pg_stat_statements_info')::text", ct));
            Assert.NotNull(await ScalarAsync<object>(connection, "SELECT " + PgStatementStatsCollector.StatementsEpochSql, ct));
        }
        finally
        {
            /* Leave the rig as a fresh CREATE EXTENSION would: current version, public. */
            await ExecAsync(connection, "DROP EXTENSION IF EXISTS pg_stat_statements; DROP SCHEMA IF EXISTS ext CASCADE; CREATE EXTENSION pg_stat_statements;", ct);
        }
    }

    private static string Sql(int major)
        => PgStatementStatsCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 3818,
            ServerName = "live-target",
            CollectionTime = DateTime.UtcNow,
            Deltas = new NoOpDeltaCalculator(),
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                IsAurora = false,
                PostgresMajorVersion = major,
                PostgresVersionNum = major * 10000,
            },
        }).Text;

    /// <summary>Row count plus the epoch read at ordinal 27 off the first row - the read <c>ReadAsync</c> makes.</summary>
    private static async Task<(int Count, DateTime? Epoch)> ReadAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);

        var count = 0;
        DateTime? epoch = null;
        while (await reader.ReadAsync(ct))
        {
            if (count == 0)
            {
                epoch = reader.IsDBNull(27) ? null : reader.GetDateTime(27);
            }

            count++;
        }

        return (count, epoch);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T?> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? default : (T)value;
    }

    /// <summary>The collector's deltas are not this proof's subject; every call is a first sighting.</summary>
    private sealed class NoOpDeltaCalculator : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue, DateTime? collectionTime = null, int maxGapSeconds = 0) => 0;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue, out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return 0;
        }
    }
}
