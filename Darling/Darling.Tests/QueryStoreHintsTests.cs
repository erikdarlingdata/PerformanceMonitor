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

    private static Task InsertAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, long? hintId, long? queryId, string? text, string? reason, long? failures, string? database = null) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"INSERT INTO collect.query_store_hints
(collection_id, collection_time, server_id, server_name, database_name, query_hint_id, query_id, query_hint_text, last_query_hint_failure_reason, query_hint_failure_count, source_desc)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, database ?? Db, hintId, queryId, text, reason, failures, hintId is null ? null : "USER");

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
            Assert.False(root.GetProperty("read_capped").GetBoolean());
            Assert.Equal(2, root.GetProperty("hints_returned").GetInt32());
            return 0;
        });
    }

    /// <summary>
    /// #6876: first seen over several databases and many captures. The grouped pass must return exactly what the per-hint minimum it replaced
    /// returned (that older shape is run here over the same CTEs), a hint that was absent and came back is first seen after its last absence,
    /// and the same hint id and query id in two databases are judged apart.
    /// </summary>
    [Fact]
    public async Task FirstSeen_OverSeveralDatabases_MatchesThePerHintMinimumItReplaced()
    {
        var now = DateTime.UtcNow; var t0 = new DateTime(now.Ticks - now.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Utc).AddHours(-3); /* whole seconds: the store keeps microseconds */
        const int captures = 10;
        DateTime At(int c) => t0.AddMinutes(10 * c);
        string[] dbs = ["DbA", "DbB", "DbC"];

        /* Hint 1: every capture. Hint 2: absent in DbA captures 3 to 5 and in DbB captures 2 to 4, back after. Hint 3: new at capture 7.
           Hint 4: DbC only, captures 0 to 4, then cleared. DbC has no capture 9, so its newest capture is 8. */
        static bool Present(string db, int c, long hint) => hint switch
        {
            1 => true,
            2 => !(db == "DbA" && c is >= 3 and <= 5) && !(db == "DbB" && c is >= 2 and <= 4),
            3 => c >= 7,
            _ => db == "DbC" && c <= 4
        };

        await WithStoreAsync(16, async (connection, ct) =>
        {
            for (var c = 0; c < captures; c++)
                foreach (var db in dbs)
                {
                    if (db == "DbC" && c == captures - 1) continue;
                    await InsertAsync(connection, ct, At(c), null, null, null, null, null, db);
                    for (long hint = 1; hint <= 4; hint++)
                        if (Present(db, c, hint))
                            await InsertAsync(connection, ct, At(c), hint, hint * 100, "OPTION (MAXDOP 1)", null, 0, db);
                }
        }, async (postgres, ct) =>
        {
            var filter = DatabaseFilter.All;
            var start = t0.AddHours(-1);
            var end = DateTime.UtcNow;

            /* The shape #6876 replaced, over the same CTEs: one minimum per output row. */
            var oldSql = $@"
WITH {DarlingQueryStoreHintReader.Ctes(filter)}
SELECT h.database_name, h.query_id, h.query_hint_id,
       (SELECT MIN(x.collection_time) FROM hints AS x
         WHERE x.database_name = h.database_name AND x.query_id = h.query_id AND x.query_hint_id = h.query_hint_id
         AND (a.last_absent IS NULL OR x.collection_time > a.last_absent)) AS first_seen
FROM hints AS h
JOIN newest AS n ON n.database_name = h.database_name AND n.newest_capture = h.collection_time
LEFT JOIN absent AS a ON a.database_name = h.database_name AND a.query_id = h.query_id AND a.query_hint_id = h.query_hint_id";

            var before = new Dictionary<(string, long, long), DateTime>();
            await using (var command = postgres.CreateCommand(oldSql))
            {
                command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
                command.Parameters.AddWithValue(ServerId);
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
                command.Parameters.Add(filter.Parameter());
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint, Value = DBNull.Value });
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                    before[(reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2))] = reader.GetDateTime(3);
            }

            var read = await DarlingQueryStoreHintReader.ReadAsync(postgres, ServerId, filter, null, start, end, cancellationToken: ct);
            var after = read.Hints.ToDictionary(h => (h.DatabaseName, h.QueryId, h.HintId), h => h.FirstSeen);

            /* DbA and DbB list hints 1 to 3, DbC (newest capture 8) lists 1 to 3 as well: nine hints, hint 4 was cleared. */
            Assert.Equal(9, after.Count);
            Assert.Equal(before.OrderBy(p => p.Key).ToList(), after.OrderBy(p => p.Key).ToList());

            DateTime Naive(int c) => DateTime.SpecifyKind(At(c), DateTimeKind.Unspecified);
            Assert.Equal(Naive(0), after[("DbA", 100, 1)]);
            Assert.Equal(Naive(6), after[("DbA", 200, 2)]);
            Assert.Equal(Naive(5), after[("DbB", 200, 2)]);
            Assert.Equal(Naive(0), after[("DbC", 200, 2)]);
            Assert.Equal(Naive(7), after[("DbC", 300, 3)]);
            Assert.DoesNotContain(after.Keys, k => k.Item3 == 4);
            return 0;
        });
    }

    /// <summary>
    /// The read holds at most <see cref="DarlingQueryStoreHintReader.MaxHints"/> hints across every database. Two databases hold 2101 between
    /// them and the one FAILING hint is in the database that sorts last: the cut must not drop it, the answer must say it was cut, and every
    /// count must be the whole-set figure (or named as a count of what was returned).
    /// </summary>
    [Fact]
    public async Task AReadPastTheCap_KeepsTheFailingHint_SaysItWasCut_AndCountsTheWholeSet()
    {
        const int healthyInA = 1500;
        const int healthyInZ = 600;
        await WithStoreAsync(16, async (connection, ct) =>
        {
            var at = DateTime.UtcNow.AddMinutes(-5);
            await InsertAsync(connection, ct, at, null, null, null, null, null, "HintDbA");
            await InsertAsync(connection, ct, at, null, null, null, null, null, "HintDbZ");
            foreach (var (database, count, idBase) in new[] { ("HintDbA", healthyInA, 0L), ("HintDbZ", healthyInZ, 100000L) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO collect.query_store_hints
(collection_id, collection_time, server_id, server_name, database_name, query_hint_id, query_id, query_hint_text, query_hint_failure_count, source_desc)
SELECT -($6 + g), $1, $2, $3, $4, $6 + g, $6 + g, 'OPTION (MAXDOP 1)', 0, 'USER' FROM generate_series(1, $5::int) AS g",
                    DarlingMcpTestData.Naive(at), ServerId, ServerName, database, count, idBase);
            }
            /* The only failing hint, in the database that sorts last. */
            await InsertAsync(connection, ct, at, 999999, 999999, "OPTION (USE HINT ('NOPE'))", "UNSUPPORTED_HINT", 3, "HintDbZ");
        }, async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, ct));
            var wholeSet = healthyInA + healthyInZ + 1;

            Assert.True(root.GetProperty("read_capped").GetBoolean());
            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.Equal(DarlingQueryStoreHintReader.MaxHints, root.GetProperty("hints_returned").GetInt32());
            Assert.Equal(wholeSet, root.GetProperty("total_hints").GetInt32());
            Assert.Equal(1, root.GetProperty("failing_hints").GetInt32());
            Assert.Contains("2000", root.GetProperty("page_note").GetString(), StringComparison.Ordinal);

            /* The failing hint survives the cut and leads the list. */
            var first = root.GetProperty("query_store_hints")[0];
            Assert.Equal("FAILING", first.GetProperty("state").GetString());
            Assert.Equal("HintDbZ", first.GetProperty("database_name").GetString());
            Assert.Equal(999999, first.GetProperty("query_hint_id").GetInt64());

            /* A database's hint_count is its whole capture, not what fit in the read. */
            var counts = root.GetProperty("captures").EnumerateArray()
                .ToDictionary(c => c.GetProperty("database_name").GetString()!, c => c.GetProperty("hint_count").GetInt32());
            Assert.Equal(healthyInA, counts["HintDbA"]);
            Assert.Equal(healthyInZ + 1, counts["HintDbZ"]);
            Assert.Equal(wholeSet, counts.Values.Sum());
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
    public async Task NoCapture_WithQueryStoreOffInEveryDatabase_ReadsPrecondition_AndSaysWhatToRun()
    {
        await WithStoreAsync(16, async (connection, ct) =>
        {
            foreach (var (db, state) in new[] { ("Sales", "OFF"), ("Hr", "OFF") })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, interval_length_minutes) " +
                    "VALUES ($1, $2, $3, $4, $5, $6, 'READ_WRITE', 0, 60)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5)), ServerId, ServerName, db, state);
            }
        }, async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, ct));
            Assert.Equal("precondition", root.GetProperty("status").GetString());
            var message = root.GetProperty("message").GetString()!;
            Assert.Contains("Query Store is not enabled on", message, StringComparison.Ordinal);
            Assert.Contains("ALTER DATABASE [name] SET QUERY_STORE = ON", message, StringComparison.Ordinal);
            return 0;
        });
    }

    [Fact]
    public async Task NoCapture_WithQueryStoreInErrorInEveryDatabase_ReadsPrecondition_AndNamesTheConsistencyCheck()
    {
        await WithStoreAsync(16, async (connection, ct) =>
        {
            foreach (var (db, state) in new[] { ("Sales", "ERROR"), ("Hr", "ERROR") })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, interval_length_minutes) " +
                    "VALUES ($1, $2, $3, $4, $5, $6, 'READ_WRITE', 0, 60)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5)), ServerId, ServerName, db, state);
            }
        }, async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, ct));
            Assert.Equal("precondition", root.GetProperty("status").GetString());
            Assert.Contains("sys.sp_query_store_consistency_check", root.GetProperty("message").GetString()!, StringComparison.Ordinal);
            return 0;
        });
    }

    [Fact]
    public async Task NoCapture_WithQueryStoreWritingInOneDatabase_StaysUnavailable()
    {
        await WithStoreAsync(16, async (connection, ct) =>
        {
            foreach (var (db, state) in new[] { ("Sales", "OFF"), ("Hr", "READ_WRITE") })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, interval_length_minutes) " +
                    "VALUES ($1, $2, $3, $4, $5, $6, 'READ_WRITE', 0, 60)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5)), ServerId, ServerName, db, state);
            }
        }, async (postgres, ct) =>
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
