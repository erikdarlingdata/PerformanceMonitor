/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The autovacuum collector's <c>autovacuum_disabled</c> flag against a REAL catalog (#3691 lane 36's bug, fixed
/// in the third between-waves batch). <c>pg_options_to_table</c> returns the reloption literal as the operator
/// typed it, so <c>WITH (autovacuum_enabled = off)</c> — the spelling in PostgreSQL's own documentation — stores
/// <c>off</c>, and the old <c>lower(option_value) = 'false'</c> read every such table as enabled: measured live,
/// <c>autovacuum_disabled = f</c> on all 47 stored rows of a table switched off with <c>off</c>, so
/// <c>CONFIG_PG_AUTOVACUUM_DISABLED</c> (#3761) could not fire for it. The fix reads the flag through the boolean
/// input function (<c>NOT option_value::boolean</c>), which accepts exactly the set the server accepted when it
/// validated the option.
///
/// <para>This class plants tables in the SHARED <c>DARLING_TEST_PG</c> store inside one transaction that is never
/// committed — the collector's statement reads only <c>pg_stat_user_tables</c> / <c>pg_class</c> of the current
/// database, so the throwaway store IS a valid target for it. Nothing is written past the rollback, which is why
/// there is no <c>finally</c> and no <c>LiveStoreCleanup</c>: the transaction is the cleanup. Serialized through
/// the collection because the store connection is shared with every other live class.</para>
///
/// <para>The activity-filter half is the load-bearing one. In an uncommitted transaction <c>n_dead_tup</c> and
/// <c>n_mod_since_analyze</c> are still 0 for a table just created (the statistics collector counts at commit), so
/// the ONLY predicate that can admit these rows is the filter's "kept regardless of activity" clause — the same
/// clause the bug also broke. A spelling that reaches the result set at all did so through the fixed predicate.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgAutovacuumDisabledSpellingLiveTests
{
    /// <summary>
    /// Every spelling PostgreSQL accepts for "off" reads as disabled, <c>on</c> reads as enabled and is NOT kept by
    /// the disabled clause, and a table with no reloption at all is not in the rows (no activity, not disabled).
    /// </summary>
    [Fact]
    public async System.Threading.Tasks.Task EverySpellingOfOff_ReadsAsDisabled_AndIsKeptByTheActivityFilter()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the autovacuum reloption spelling test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var tx = await connection.BeginTransactionAsync(ct);

        /* One table per spelling, each empty and never written, so activity cannot be what admits it. The
           names carry the spelling so the assertion below can read the result set by name. */
        var offSpellings = new[] { "off", "false", "0", "no", "f", "n" };
        foreach (var spelling in offSpellings)
        {
            await using var create = new NpgsqlCommand(
                $"CREATE TABLE pg_temp.av_spelling_{spelling} (id int) WITH (autovacuum_enabled = {spelling})", connection, tx);
            await create.ExecuteNonQueryAsync(ct);
        }

        await using (var on = new NpgsqlCommand("CREATE TABLE pg_temp.av_spelling_on (id int) WITH (autovacuum_enabled = on)", connection, tx))
        {
            await on.ExecuteNonQueryAsync(ct);
        }

        await using (var plain = new NpgsqlCommand("CREATE TABLE pg_temp.av_spelling_plain (id int)", connection, tx))
        {
            await plain.ExecuteNonQueryAsync(ct);
        }

        /* Temp tables are the current session's own and DO appear in pg_stat_user_tables (schemaname pg_temp_N),
           which is what lets the collector's unmodified statement see them without a committed CREATE. */
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "live-store-as-target",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 18 },
            ExcludedDatabases = Array.Empty<string>(),
            CurrentDatabaseName = connection.Database,
        };
        var sql = PgAutovacuumStatsCollector.Instance.BuildQuery(context).Text;

        var disabledByTable = new Dictionary<string, bool>(StringComparer.Ordinal);
        await using (var read = new NpgsqlCommand(sql, connection, tx))
        await using (var reader = await read.ExecuteReaderAsync(ct))
        {
            var tableAt = reader.GetOrdinal("table_name");
            var disabledAt = reader.GetOrdinal("autovacuum_disabled");
            while (await reader.ReadAsync(ct))
            {
                var table = reader.GetString(tableAt);
                if (table.StartsWith("av_spelling_", StringComparison.Ordinal))
                {
                    disabledByTable[table] = reader.GetBoolean(disabledAt);
                }
            }
        }

        foreach (var spelling in offSpellings)
        {
            Assert.True(disabledByTable.TryGetValue($"av_spelling_{spelling}", out var disabled),
                $"autovacuum_enabled = {spelling}: the table was not in the rows — the activity filter's disabled clause did not admit it");
            Assert.True(disabled, $"autovacuum_enabled = {spelling}: autovacuum_disabled read false — the spelling was not recognised as off");
        }

        /* The other direction: `on` is enabled and, with no activity, must NOT be kept by the disabled clause;
           the plain table has no reloption at all and the coalesce(..., false) leaves it out the same way. */
        Assert.False(disabledByTable.ContainsKey("av_spelling_on"), "autovacuum_enabled = on was admitted by the disabled clause");
        Assert.False(disabledByTable.ContainsKey("av_spelling_plain"), "a table with no reloption was admitted by the disabled clause");

        await tx.RollbackAsync(ct);
    }
}
