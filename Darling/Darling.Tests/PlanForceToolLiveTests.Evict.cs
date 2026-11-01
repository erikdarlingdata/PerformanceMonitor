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
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* The evict half of the plan force live tests (#5632, step 10): the same store and SQL Server setup (a partial class, so the private setup is
   shared). Each test makes its own scratch store and SQL Server database. */
public sealed partial class PlanForceToolLiveTests
{
    private const string EvictKind = "plan_evict";

    /// <summary>One <c>sys.dm_exec_query_stats</c> row of a cached statement, read from the test SQL Server.</summary>
    private sealed record CachedStatement(
        string PlanHandle, string SqlHandle, string QueryHash, string QueryPlanHash, int StartOffset, int EndOffset, DateTime CreationTime,
        long Generation, long Executions, DateTime LastExecution, long WorkerTime, long ElapsedTime, long LogicalReads);

    private static string MarkerQuery(string marker) => $"SELECT marker = '{marker}', n = COUNT_BIG(*) FROM dbo.t WHERE a >= 1;";

    private static async Task RunMarkerAsync(SqlScratch sql, string marker, int times, CancellationToken ct)
    {
        await using var admin = await OpenAsync(sql.Admin(sql.Database), ct);
        for (var i = 0; i < times; i++)
            await ExecAsync(admin, MarkerQuery(marker), ct);
    }

    private static async Task<CachedStatement> ReadCachedAsync(string marker, CancellationToken ct)
    {
        await using var admin = await OpenAsync(new SqlConnectionStringBuilder
        {
            DataSource = SqlHost,
            InitialCatalog = "master",
            UserID = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER"),
            Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"),
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            Pooling = false,
        }, ct);
        await using var command = new SqlCommand(@"
SELECT TOP (1) plan_handle = CONVERT(varchar(130), qs.plan_handle, 1), sql_handle = CONVERT(varchar(130), qs.sql_handle, 1),
       query_hash = CONVERT(varchar(64), qs.query_hash, 1), query_plan_hash = CONVERT(varchar(64), qs.query_plan_hash, 1),
       qs.statement_start_offset, qs.statement_end_offset, qs.creation_time, qs.plan_generation_num, qs.execution_count, qs.last_execution_time,
       qs.total_worker_time, qs.total_elapsed_time, qs.total_logical_reads
FROM sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS t
WHERE t.text LIKE @like AND t.text NOT LIKE N'%dm_exec_query_stats%'
ORDER BY qs.creation_time DESC;", admin) { CommandTimeout = 60 };
        command.Parameters.Add(new SqlParameter("@like", SqlDbType.NVarChar, 200) { Value = "%'" + marker + "'%" });
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "the marker query should be in dm_exec_query_stats");
        return new CachedStatement(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetInt32(4), reader.GetInt32(5),
            reader.GetDateTime(6), reader.GetInt64(7), reader.GetInt64(8), reader.GetDateTime(9), reader.GetInt64(10), reader.GetInt64(11), reader.GetInt64(12));
    }

    private static async Task<bool> IsCachedAsync(SqlScratch sql, string handle, CancellationToken ct)
    {
        await using var admin = await OpenAsync(sql.Admin(), ct);
        await using var command = new SqlCommand("SELECT COUNT_BIG(*) FROM sys.dm_exec_cached_plans WHERE plan_handle = CONVERT(varbinary(64), @hex, 1);", admin);
        command.Parameters.Add(new SqlParameter("@hex", SqlDbType.VarChar, 130) { Value = handle });
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) > 0;
    }

    /// <summary>Seeds one collected query_stats row for a statement, as the collector would store it (<c>creation_time</c> on the server's clock).</summary>
    private static async Task SeedStatsAsync(Ctx c, CachedStatement s, DateTime collectionTime, long executions, string? planHashOverride = null)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, creation_time, last_execution_time, execution_count,
     total_worker_time, total_elapsed_time, total_logical_reads, sql_handle, plan_handle, plan_generation_num, statement_start_offset, statement_end_offset,
     delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
VALUES ($1, $2, 7, 'srv-7', $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $8, $9, $10, 60)", c.Connection) { CommandTimeout = 60 };
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = DateTime.UtcNow.Ticks, NpgsqlDbType = NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = c.Sql.Database, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = s.QueryHash, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = planHashOverride ?? s.QueryPlanHash, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(s.CreationTime, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(s.LastExecution, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = executions, NpgsqlDbType = NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = s.WorkerTime, NpgsqlDbType = NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = s.ElapsedTime, NpgsqlDbType = NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = s.LogicalReads, NpgsqlDbType = NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = s.SqlHandle, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = s.PlanHandle, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = s.Generation, NpgsqlDbType = NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = s.StartOffset, NpgsqlDbType = NpgsqlDbType.Integer });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = s.EndOffset, NpgsqlDbType = NpgsqlDbType.Integer });
        await command.ExecuteNonQueryAsync(c.Ct);
    }

    private static Task<string> EvictAsync(Ctx c, string handle, bool dryRun, string server = "srv-7") =>
        DarlingMcpPlanForceTools.RunEvictAsync(c.Source, c.Source, c.Log, server, handle, c.Arg, dryRun, c.Ct);

    private static async Task<int> JournalActionRowsAsync(Ctx c) =>
        int.Parse(await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'action_%'", c.Ct) ?? "0", CultureInfo.InvariantCulture);

    private static async Task<DateTime> DoneTimeAsync(Ctx c) =>
        DateTime.Parse(await PgScalarAsync(c.Connection, "SELECT event_time FROM config.investigation_journal WHERE event = 'action_done'", c.Ct) ?? "", CultureInfo.InvariantCulture);

    [Fact]
    public async Task ADryRunEvict_ShowsTheStatementAndTheStoresBeforeImage_WritesNothing_AndJournalsNothing()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var marker = "ev1" + Guid.NewGuid().ToString("N")[..8];
            await RunMarkerAsync(c.Sql, marker, 3, c.Ct);
            var cached = await ReadCachedAsync(marker, c.Ct);
            await SeedStatsAsync(c, cached, DateTime.UtcNow.AddMinutes(-5), cached.Executions);

            var answer = await EvictAsync(c, cached.PlanHandle.ToLowerInvariant().Replace("0X", "0x", StringComparison.Ordinal), dryRun: true);
            var json = Parse(answer);
            Assert.Equal("dry_run", (string?)json["status"]);
            Assert.Equal($"DBCC FREEPROCCACHE ({cached.PlanHandle.ToUpperInvariant().Replace("0X", "0x", StringComparison.Ordinal)});", (string?)json["statement"]);
            var statements = json["before"]!["statements"]!.AsArray();
            var one = Assert.Single(statements);
            Assert.Equal(cached.QueryHash, (string?)one!["query_hash"]);
            Assert.Equal(cached.QueryPlanHash, (string?)one["query_plan_hash"]);
            Assert.Equal(cached.SqlHandle, (string?)one["sql_handle"]);
            Assert.Equal(cached.StartOffset, (int)one["statement_start_offset"]!);
            Assert.Equal(cached.Executions, (long)one["execution_count"]!);
            Assert.Contains("every statement in that batch", (string?)json["warnings"]![0], StringComparison.Ordinal);
            Assert.True(await IsCachedAsync(c.Sql, cached.PlanHandle, c.Ct));
            Assert.Equal(0, await JournalActionRowsAsync(c));
            var parameter = typeof(DarlingMcpPlanForceTools).GetMethod(nameof(DarlingMcpPlanForceTools.EvictCachedPlan))!.GetParameters().Single(p => p.Name == "dry_run");
            Assert.True((bool)parameter.DefaultValue!);
            AssertNoSecret(c, answer);
        });
    }

    [Fact]
    public async Task ARealEvict_RemovesTheHandle_JournalsCalledAndDone_AnswersTheJournalId_AndHasNoUndo()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var marker = "ev2" + Guid.NewGuid().ToString("N")[..8];
            await RunMarkerAsync(c.Sql, marker, 3, c.Ct);
            var cached = await ReadCachedAsync(marker, c.Ct);
            await SeedStatsAsync(c, cached, DateTime.UtcNow.AddMinutes(-5), cached.Executions);

            var answer = await EvictAsync(c, cached.PlanHandle, dryRun: false);
            var json = Parse(answer);
            Assert.Equal("done", (string?)json["status"]);
            Assert.False(await IsCachedAsync(c.Sql, cached.PlanHandle, c.Ct));
            Assert.True(json.ContainsKey("undo"), "an eviction answers undo, and it is null");
            Assert.Null(json["undo"]);

            var called = Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Equal(EvictKind, (string?)called["kind"]);
            Assert.Equal("evict_cached_plan", (string?)called["tool"]);
            Assert.Equal(cached.PlanHandle.ToUpperInvariant().Replace("0X", "0x", StringComparison.Ordinal), (string?)called["target"]!["id"]);
            Assert.Equal(7, (int)called["target"]!["server_id"]!);
            Assert.Null(called["target"]!["database"]);
            Assert.Equal(cached.PlanHandle, (string?)called["before"]!["plan_handle"]);
            Assert.Single(called["before"]!["statements"]!.AsArray());
            var done = Assert.Single(await RowsAsync(c, "action_done"));
            Assert.StartsWith("DBCC FREEPROCCACHE (0x", (string?)done["after"]!["statement"], StringComparison.Ordinal);
            Assert.NotNull(done["after"]!["message"]);
            Assert.Equal((long)json["journal_id"]!, (long)done["called_id"]!);

            var list = await new AgentChangeUndo(c.Source, 30).ReadAsync(50, cancellationToken: c.Ct);
            var change = Assert.Single(list.Changes);
            Assert.Equal(EvictKind, change.Kind);
            Assert.Equal(AgentChangeResult.Done, change.Result);
            Assert.False(change.CanTryUndo(DateTime.UtcNow), "an eviction has no undo");
            AssertNoSecret(c, answer, await JournalTextAsync(c));
        });
    }

    [Fact]
    public async Task AHandleNotInTheCache_IsRefusedPlanNotCached_OnADryRunAndARealCall_WithNothingJournaledAsDone()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            const string absent = "0x06000700AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
            foreach (var dryRun in new[] { true, false })
            {
                var json = Parse(await EvictAsync(c, absent, dryRun));
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("plan_not_cached", (string?)json["reason"]);
            }

            Assert.Equal(0, await JournalActionRowsAsync(c));
            Assert.Equal("1", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = 'plan_not_cached'", c.Ct));
        });
    }

    [Fact]
    public async Task AMalformedHandle_IsRefusedBadPlanHandle_BeforeTheGate_AndJournaled()
    {
        await RunAsync(Without.Switch, async c =>
        {
            /* The switch is off, yet the answer is the handle's own refusal: a handle that cannot do anything is refused before the gate. */
            var json = Parse(await EvictAsync(c, "0x06;DROP", dryRun: false));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("bad_plan_handle", (string?)json["reason"]);
            Assert.Equal("1", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = 'bad_plan_handle'", c.Ct));
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    [Fact]
    public async Task AnEvictByALoginWithoutAlterServerState_IsRefusedEvictPermissionDenied_AndTheHandleStaysCached()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var marker = "ev3" + Guid.NewGuid().ToString("N")[..8];
            await RunMarkerAsync(c.Sql, marker, 2, c.Ct);
            var cached = await ReadCachedAsync(marker, c.Ct);
            await using (var admin = await OpenAsync(c.Sql.Admin(), c.Ct))
                await ExecAsync(admin, $"REVOKE ALTER SERVER STATE FROM [{c.Sql.Login}];", c.Ct);

            foreach (var dryRun in new[] { true, false })
            {
                var answer = await EvictAsync(c, cached.PlanHandle, dryRun);
                var json = Parse(answer);
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("evict_permission_denied", (string?)json["reason"]);
                AssertNoSecret(c, answer);
            }

            Assert.True(await IsCachedAsync(c.Sql, cached.PlanHandle, c.Ct));
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    private static async Task AssertEvictRefusedAsync(Without without, string reason)
    {
        await RunAsync(without, async c =>
        {
            /* The plan cache is shared with the whole server, and memory pressure can trim any plan. A second, untouched control plan tells the two
               apart: the marker plan gone with the control plan still cached means the refused call evicted it (a defect); both gone means the cache was
               trimmed under the test, so it is primed again and tried once more. */
            var trimmed = true;
            for (var attempt = 0; attempt < 2 && trimmed; attempt++)
            {
                var marker = "ev4" + Guid.NewGuid().ToString("N")[..8];
                var control = "ev4c" + Guid.NewGuid().ToString("N")[..8];
                await RunMarkerAsync(c.Sql, marker, 2, c.Ct);
                await RunMarkerAsync(c.Sql, control, 2, c.Ct);
                var cached = await ReadCachedAsync(marker, c.Ct);
                var controlPlan = await ReadCachedAsync(control, c.Ct);
                foreach (var dryRun in new[] { true, false })
                {
                    var answer = await EvictAsync(c, cached.PlanHandle, dryRun);
                    var json = Parse(answer);
                    Assert.Equal("refused", (string?)json["status"]);
                    Assert.Equal(reason, (string?)json["reason"]);
                    AssertNoSecret(c, answer);
                }

                if (await IsCachedAsync(c.Sql, cached.PlanHandle, c.Ct))
                {
                    trimmed = false;
                }
                else
                {
                    Assert.False(await IsCachedAsync(c.Sql, controlPlan.PlanHandle, c.Ct),
                        "The refused call evicted the plan: the marker plan is gone while an untouched control plan is still cached.");
                }
            }

            Assert.False(trimmed, "The plan cache was trimmed under the test twice in a row (both the marker plan and the control plan were gone).");
            Assert.Equal(0, await JournalActionRowsAsync(c));
            Assert.Equal("1", await PgScalarAsync(c.Connection, $"SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = '{reason}'", c.Ct));
        });
    }

    [Fact]
    public Task WithoutTheEvictSwitch_TheCallIsRefused_AndNothingReachesTheServer() => AssertEvictRefusedAsync(Without.EvictSwitch, "server_action_off");

    [Fact]
    public Task WithoutALevelThreeRoute_TheEvictIsRefused_AndNothingReachesTheServer() => AssertEvictRefusedAsync(Without.Level, "not_level_3");

    [Fact]
    public Task WithAgentActionsOff_TheEvictIsRefused_AndNothingReachesTheServer() => AssertEvictRefusedAsync(Without.ActionsEnabled, "actions_off");

    [Fact]
    public Task WithoutTheCredentialSlot_TheEvictIsRefused_AndNothingReachesTheServer() => AssertEvictRefusedAsync(Without.CredentialSlot, "no_remediation_credential");

    [Fact]
    public async Task AnEvictOnAPostgreSqlTarget_IsRefusedWithItsOwnReason()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var json = Parse(await EvictAsync(c, "0x06000700AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", dryRun: false, server: "srv-8"));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("postgres_target", (string?)json["reason"]);
            Assert.Equal("1", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = 'postgres_target'", c.Ct));
        });
    }

    [Fact]
    public async Task AHandleDarlingNeverCollected_StillEvicts_AndTheAnswerSaysItCannotBeFollowed()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var marker = "ev5" + Guid.NewGuid().ToString("N")[..8];
            await RunMarkerAsync(c.Sql, marker, 2, c.Ct);
            var cached = await ReadCachedAsync(marker, c.Ct);

            var json = Parse(await EvictAsync(c, cached.PlanHandle, dryRun: false));
            Assert.Equal("done", (string?)json["status"]);
            Assert.False(await IsCachedAsync(c.Sql, cached.PlanHandle, c.Ct));
            Assert.Contains("has not collected this handle", (string?)json["warnings"]![0], StringComparison.Ordinal);
            Assert.Empty(json["before"]!["statements"]!.AsArray());

            var read = Parse(await DarlingMcpCompilesAfterEvictTools.RunAsync(c.Source, (long)json["journal_id"]!, c.Ct));
            Assert.Equal("empty", (string?)read["status"]);
        });
    }

    [Fact]
    public async Task TheCompileRead_FollowsTheNextCompile_FromTheServersOwnCreationTime_AndRefusesWhatIsNotAnEviction()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var marker = "ev6" + Guid.NewGuid().ToString("N")[..8];
            await RunMarkerAsync(c.Sql, marker, 3, c.Ct);
            var before = await ReadCachedAsync(marker, c.Ct);
            await SeedStatsAsync(c, before, DateTime.UtcNow.AddMinutes(-5), before.Executions);

            var evicted = Parse(await EvictAsync(c, before.PlanHandle, dryRun: false));
            Assert.Equal("done", (string?)evicted["status"]);
            var journalId = (long)evicted["journal_id"]!;
            var evictedAt = await DoneTimeAsync(c);

            /* Nothing collected after the eviction yet. */
            var none = Parse(await DarlingMcpCompilesAfterEvictTools.RunAsync(c.Source, journalId, c.Ct));
            Assert.Equal("not_collected_yet", (string?)Assert.Single(none["statements"]!.AsArray())!["state"]);

            /* The old plan, collected again after the eviction with its old creation_time: not a compile. */
            await SeedStatsAsync(c, before, evictedAt.AddSeconds(30), before.Executions + 1);
            var old = Parse(await DarlingMcpCompilesAfterEvictTools.RunAsync(c.Source, journalId, c.Ct));
            Assert.Equal("old_plan_still_seen", (string?)Assert.Single(old["statements"]!.AsArray())!["state"]);

            /* The next run compiles a new plan: read the server's own row, seed it as collected. */
            await RunMarkerAsync(c.Sql, marker, 2, c.Ct);
            var after = await ReadCachedAsync(marker, c.Ct);
            Assert.True(after.CreationTime > before.CreationTime, "a new compile has a later creation_time on the server's own clock");
            await SeedStatsAsync(c, after, evictedAt.AddMinutes(1), after.Executions);

            var read = Parse(await DarlingMcpCompilesAfterEvictTools.RunAsync(c.Source, journalId, c.Ct));
            var statement = Assert.Single(read["statements"]!.AsArray())!;
            Assert.Equal("recompiled", (string?)statement["state"]);
            Assert.Equal(before.QueryPlanHash == after.QueryPlanHash, (bool)statement["same_plan_shape"]!);
            Assert.Equal(after.PlanHandle, (string?)statement["after"]!["plan_handle"]);
            Assert.Equal(before.PlanHandle, (string?)statement["before"]!["plan_handle"]);
            Assert.Equal(after.Executions, (long)statement["after"]!["executions_since"]!);
            Assert.NotNull(statement["after"]!["avg_cpu_ms"]);
            Assert.NotNull(statement["before"]!["avg_cpu_ms"]);

            /* The probe the PR body reports: did plan_handle change after the new compile. */
            var changed = !string.Equals(before.PlanHandle, after.PlanHandle, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(changed, (bool)statement["plan_handle_changed"]!);
            /* Recorded in the PR body, not decided on: a live run saw the handle change on the new compile, but the read never relies on it. */

            /* A journal id that is not a done eviction is refused. */
            var notEvict = Parse(await DarlingMcpCompilesAfterEvictTools.RunAsync(c.Source, journalId + 1000, c.Ct));
            Assert.Equal("not_an_eviction", (string?)notEvict["reason"]);
            AssertNoSecret(c, read.ToJsonString());
        });
    }
}
