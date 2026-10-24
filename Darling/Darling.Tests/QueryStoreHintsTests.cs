/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6581 slice 1: the Query Store hints collector (<c>query_store_hints</c>, SQL Server 2022 and later). Its SQL, its version gate,
/// the marker row that makes a hint-free database a complete capture, and its V217 table.
/// </summary>
public sealed class QueryStoreHintsCollectorTests
{
    private static CollectorContext Context(int major, bool azureDb = false) => new()
    {
        CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "s",
        Target = new CollectorTargetInfo { SqlMajorVersion = major, IsAzureSqlDb = azureDb },
        Deltas = new CollectorDeltaCalculator(),
    };

    [Fact]
    public void TheCollector_SkipsBefore2022_AndRunsOn2022AndLater_UnknownAndAzure()
    {
        var instance = QueryStoreHintsCollector.Instance;

        Assert.False(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { SqlMajorVersion = 15 }));
        Assert.False(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { SqlMajorVersion = 13 }));
        Assert.True(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { SqlMajorVersion = 16 }));
        Assert.True(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { SqlMajorVersion = 17 }));
        Assert.True(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { SqlMajorVersion = 0 }));
        Assert.True(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { IsAzureSqlDb = true, SqlMajorVersion = 12 }));
        Assert.True(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { IsAzureManagedInstance = true, SqlMajorVersion = 12 }));
        Assert.False(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql }));
    }

    [Fact]
    public void ThePayload_ReadsTheHintsView_GuardsAMissingView_AndEmitsOneMarkerRow()
    {
        var body = QueryStoreHintsCollector.BuildPayloadBody();

        /* A database without the view (a Managed Instance on an older update policy) returns no result set and no error. */
        Assert.Contains("IF OBJECT_ID(N'sys.query_store_query_hints', N'V') IS NULL", body, StringComparison.Ordinal);
        /* The forced-plans eligibility gate, so a database whose Query Store is off or a readable secondary writes no capture. */
        Assert.Contains("actual_state IN (1, 2, 4)", body, StringComparison.Ordinal);
        Assert.Contains("readonly_reason & 8 = 0", body, StringComparison.Ordinal);
        Assert.Contains("UNION ALL", body, StringComparison.Ordinal);
        Assert.Contains($"TOP ({QueryStoreHintsCollector.MaxRowsPerDatabase})", body, StringComparison.Ordinal);
        Assert.DoesNotContain("JOIN", body, StringComparison.Ordinal);
        Assert.True(QueryStoreHintsCollector.Instance.CommandTimeoutSecondsOverride is > 0 and <= 300);
    }

    [Fact]
    public void TheEnumeration_AppliesTheServersDatabaseScopeAndExclusions_AndAzureDoesNotEnumerate()
    {
        var context = new CollectorContext
        {
            CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "s", Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            Deltas = new CollectorDeltaCalculator(),
            DatabaseScope = new[] { "KeepA" },
            ExcludedDatabases = new[] { "Skip" },
        };

        var query = QueryStoreHintsCollector.Instance.BuildEnumerationQuery(context)!;

        Assert.Contains("@scope_db_0", query.Text, StringComparison.Ordinal);
        Assert.Contains("@excl_db_0", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("/*EXCLUSION_FILTER*/", query.Text, StringComparison.Ordinal);
        Assert.Contains(query.Parameters, p => Equals(p.Value, "KeepA"));
        Assert.Contains(query.Parameters, p => Equals(p.Value, "Skip"));
        /* The vendor-name screen is the forced-plans one, not a copy. */
        Assert.Contains("N'rdsadmin'", query.Text, StringComparison.Ordinal);

        Assert.Null(QueryStoreHintsCollector.Instance.BuildEnumerationQuery(Context(0, azureDb: true)));
        Assert.Contains("OBJECT_ID", QueryStoreHintsCollector.Instance.BuildQuery(Context(0, azureDb: true)).Text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AReadOfAHintFreeDatabase_StoresTheMarkerRowAlone_AndAReadWithHints_StoresThem()
    {
        var table = new DataTable();
        table.Columns.Add("query_hint_id", typeof(long));
        table.Columns.Add("query_id", typeof(long));
        table.Columns.Add("query_hint_text", typeof(string));
        table.Columns.Add("last_query_hint_failure_reason", typeof(string));
        table.Columns.Add("query_hint_failure_count", typeof(long));
        table.Columns.Add("source", typeof(string));
        table.Rows.Add(DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);

        var rows = new List<QueryStoreHintsCollector.Row>();
        await QueryStoreHintsCollector.Instance.ReadItemAsync("EmptyDb", table.CreateDataReader(), rows, Context(16), TestContext.Current.CancellationToken);

        var marker = Assert.Single(rows);
        Assert.Equal("EmptyDb", marker.DbName);
        Assert.Null(marker.QueryHintId);
        Assert.Null(marker.QueryId);
        Assert.Null(marker.QueryHintText);

        table.Rows.Add(7L, 42L, "OPTION (OPTIMIZE FOR UNKNOWN)", "GENERAL_FAILURE", 3L, "USER");
        rows.Clear();
        await QueryStoreHintsCollector.Instance.ReadItemAsync("HintDb", table.CreateDataReader(), rows, Context(16), TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Null(rows[0].QueryHintId);
        var hint = rows[1];
        Assert.Equal("HintDb", hint.DbName);
        Assert.Equal(7L, hint.QueryHintId);
        Assert.Equal(42L, hint.QueryId);
        Assert.Equal("OPTION (OPTIMIZE FOR UNKNOWN)", hint.QueryHintText);
        Assert.Equal("GENERAL_FAILURE", hint.LastFailureReason);
        Assert.Equal(3L, hint.FailureCount);
        Assert.Equal("USER", hint.Source);
    }

    [Fact]
    public void TheCollector_IsRegistered_EveryFiveMinutes_ThirtyDays_AsAPlainDarlingOnlyTable()
    {
        var instance = QueryStoreHintsCollector.Instance;
        Assert.Contains(instance, CollectorCatalog.All);
        Assert.Equal(5, CollectorScheduleDefaults.All["query_store_hints"].FrequencyMinutes);
        Assert.Equal(30, CollectorScheduleDefaults.All["query_store_hints"].RetentionDays);
        Assert.True(instance.StaysPlainTable);
        Assert.True(instance.IsDarlingOnly);
        Assert.DoesNotContain(TimescaleSupport.HypertableTables, t => t.TargetTable == "query_store_hints");
    }

    [Fact]
    public void TheV217Table_IsTheGeneratorsOutputForTheCatalogEntry_AndAddsTheMarkerIndex()
    {
        var rung = PgMigrations.Scripts.Single(m => m.Version == 217).Sql.Replace("collect.", "", StringComparison.Ordinal);
        var generated = PgSchemaGenerator.CreateTable(QueryStoreHintsCollector.Instance) + "\n\n" + PgSchemaGenerator.CreateIndex(QueryStoreHintsCollector.Instance);

        static string Norm(string s) => string.Join(' ', s.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

        Assert.StartsWith(Norm(generated).TrimEnd(';'), Norm(rung), StringComparison.Ordinal);
        Assert.Contains("WHERE query_hint_id IS NULL", rung, StringComparison.Ordinal);
    }
}

/// <summary>
/// #6581 slice 1: <c>get_query_store_hints</c> over a real store. Seeded hints come back with their failure state, a cleared hint is
/// not listed, a database captured with no hint reads <c>empty</c>, no capture on a 2022 server reads <c>unavailable</c>, and a
/// server older than 2022 reads <c>not_collected</c> with the version reason.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStoreHintsReadLiveTests
{
    private const string ServerName = "darling-qs-hints";
    private const string Db = "HintDb";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<T> WithStoreAsync<T>(int sqlMajorVersion, Func<NpgsqlConnection, CancellationToken, Task> seed, Func<NpgsqlDataSource, CancellationToken, Task<T>> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store hint tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET sql_major_version = $1 WHERE server_id = $2", sqlMajorVersion, ServerId);
        await seed(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        return await body(postgres, ct);
    }

    private static Task InsertAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, long? hintId, long? queryId, string? text, string? reason, long? failures) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"INSERT INTO collect.query_store_hints
(collection_id, collection_time, server_id, server_name, database_name, query_hint_id, query_id, query_hint_text, last_query_hint_failure_reason, query_hint_failure_count, source_desc)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, Db, hintId, queryId, text, reason, failures, hintId is null ? null : "USER");

    private static Task<string> Call(NpgsqlDataSource postgres, CancellationToken ct) =>
        DarlingMcpForcedPlanTools.GetQueryStoreHints(postgres, ServerName, 24, null, DatabaseFilter.All, ct);

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    [Fact]
    public async Task SeededHints_ComeBackWithFailureState_AndAClearedHintIsNotListed()
    {
        await WithStoreAsync(16, async (connection, ct) =>
        {
            var first = DateTime.UtcNow.AddMinutes(-10);
            var second = DateTime.UtcNow.AddMinutes(-5);
            /* Earlier capture: hints 1 (query 100) and 3 (query 300). Newest: 1 (still there), 2 (new, failing); 3 was cleared. */
            await InsertAsync(connection, ct, first, null, null, null, null, null);
            await InsertAsync(connection, ct, first, 1, 100, "OPTION (OPTIMIZE FOR UNKNOWN)", null, 0);
            await InsertAsync(connection, ct, first, 3, 300, "OPTION (MAXDOP 1)", null, 0);
            await InsertAsync(connection, ct, second, null, null, null, null, null);
            await InsertAsync(connection, ct, second, 1, 100, "OPTION (OPTIMIZE FOR UNKNOWN)", null, 0);
            await InsertAsync(connection, ct, second, 2, 200, "OPTION (RECOMPILE)", "UNSUPPORTED_HINT", 4);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) VALUES ($1, $2, 100, 'SELECT 1 FROM dbo.T', $3)",
                ServerId, Db, DarlingMcpTestData.Naive(DateTime.UtcNow));
        }, async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, ct));

            Assert.Equal(2, root.GetProperty("total_hints").GetInt32());
            Assert.Equal(1, root.GetProperty("failing_hints").GetInt32());
            var hints = root.GetProperty("query_store_hints").EnumerateArray().ToList();
            Assert.Equal(2, hints.Count);

            /* Failing first. */
            Assert.Equal(2, hints[0].GetProperty("query_hint_id").GetInt64());
            Assert.Equal("FAILING", hints[0].GetProperty("state").GetString());
            Assert.Equal("UNSUPPORTED_HINT", hints[0].GetProperty("last_query_hint_failure_reason").GetString());
            Assert.Equal(4, hints[0].GetProperty("query_hint_failure_count").GetInt64());
            Assert.Equal("USER", hints[0].GetProperty("source").GetString());
            Assert.Equal(JsonValueKind.Null, hints[0].GetProperty("query_text").ValueKind);

            Assert.Equal(1, hints[1].GetProperty("query_hint_id").GetInt64());
            Assert.Equal("NO_FAILURE", hints[1].GetProperty("state").GetString());
            Assert.Equal("OPTION (OPTIMIZE FOR UNKNOWN)", hints[1].GetProperty("query_hint_text").GetString());
            Assert.Equal("SELECT 1 FROM dbo.T", hints[1].GetProperty("query_text").GetString());
            Assert.False(hints[1].GetProperty("query_text_truncated").GetBoolean());

            /* Hint 1 was in both captures, so it is first seen at the earlier one; hint 2 only at the newer. */
            var firstSeen1 = DateTime.Parse(hints[1].GetProperty("first_seen").GetString()!, null, System.Globalization.DateTimeStyles.RoundtripKind);
            var firstSeen2 = DateTime.Parse(hints[0].GetProperty("first_seen").GetString()!, null, System.Globalization.DateTimeStyles.RoundtripKind);
            Assert.True(firstSeen1 < firstSeen2);

            Assert.DoesNotContain(hints, h => h.GetProperty("query_hint_id").GetInt64() == 3);
            Assert.Equal(2, root.GetProperty("captures")[0].GetProperty("hint_count").GetInt32());
            return 0;
        });
    }

    [Fact]
    public async Task ADatabaseCapturedWithNoHint_ReadsEmpty_NotUnavailable()
    {
        await WithStoreAsync(16, (connection, ct) => InsertAsync(connection, ct, DateTime.UtcNow.AddMinutes(-2), null, null, null, null, null), async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, ct));
            Assert.Equal("empty", root.GetProperty("status").GetString());
            Assert.Equal(Db, root.GetProperty("captures")[0].GetProperty("database_name").GetString());
            return 0;
        });
    }

    [Fact]
    public async Task NoCapture_On2022_ReadsUnavailable()
    {
        await WithStoreAsync(16, (_, _) => Task.CompletedTask, async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, ct));
            Assert.Equal("unavailable", root.GetProperty("status").GetString());
            return 0;
        });
    }

    [Fact]
    public async Task NoCapture_Before2022_ReadsNotCollected_WithTheVersionReason()
    {
        await WithStoreAsync(15, (_, _) => Task.CompletedTask, async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, ct));
            Assert.Equal("not_collected", root.GetProperty("status").GetString());
            var message = root.GetProperty("message").GetString()!;
            Assert.Contains("SQL Server 2022", message, StringComparison.Ordinal);
            Assert.Contains("version", message, StringComparison.OrdinalIgnoreCase);
            return 0;
        });
    }
}
