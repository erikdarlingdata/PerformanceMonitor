/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// Deploy follow-ups on a live store (#6009, D16 of #5632 Phase C): a report names a module and a latest date; when the module's
/// <c>modify_date</c>, as <c>collect.procedure_stats.module_modify_date</c> holds it, moves after the report, the sweep opens the
/// follow-up once, with the procedure-stats counts before and after in its brief. A module that has not moved opens nothing.
/// </summary>
public sealed class InvestigationDeployFollowUpLiveTests
{
    private const string Evidence = "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"head blocker 412 s\",\"basis\":\"verified\"}]";
    private const string Module = "module:Shop.dbo.usp_Orders";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private readonly DateTime _now = new(DateTime.UtcNow.AddSeconds(-5).Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, DateTimeKind.Utc);

    /// <summary>The module's modify_date before the report (a deploy weeks ago) and after it (a deploy half an hour after the report).</summary>
    private DateTime Deployed1 => _now.AddDays(-40);

    private DateTime Deployed2 => _now.AddMinutes(30);

    /// <summary>When the sweep runs, and the collection that carried the new modify_date, inside the sweep's 30-minute read.</summary>
    private DateTime SweepAt => _now.AddHours(2);

    private DateTime RecentCollection => SweepAt.AddMinutes(-15);

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    /// <summary>Reports a request on server 7 with a deploy condition on <see cref="Module"/> and a time 20 days out.</summary>
    private async Task<Guid> ReportedAsync(InvestigationRequestStore store, CancellationToken ct)
    {
        var alert = new InvestigationAlert(1, 7, "srv-7", "blocking", "Blocking", _now, new[] { "key" }, null, 500, 500, LeaseMinutes: 15, ExpiryMinutes: 240);
        var id = (await store.OpenOrJoinAsync(alert, ct)).RequestId!.Value;
        var claim = (await store.ClaimAsync(id, "agent-a", ct)).ClaimId!.Value;
        var answer = JsonDocument.Parse(await DarlingMcpInvestigationTools.SubmitAsync(store, id.ToString(), claim.ToString(), 1, "needs_person",
            "a blocking chain", Evidence, "summary", null, null, ct,
            follow_up_at: DateTime.UtcNow.AddDays(20).ToString("o", CultureInfo.InvariantCulture), follow_up_condition: Module));
        Assert.Equal("reported", answer.RootElement.GetProperty("status").GetString());
        return id;
    }

    /// <summary>One collection of the module: <paramref name="executions"/> executions at <paramref name="cpuMs"/> and
    /// <paramref name="elapsedMs"/> each, under a modify_date.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, long collectionId, DateTime at, DateTime modifyDate, int executions,
        int cpuMs, int elapsedMs, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, delta_execution_count,
     delta_worker_time, delta_elapsed_time, sample_interval_seconds, module_modify_date)
VALUES ($1, $2, 7, 'srv-7', 'Shop', 'dbo', 'usp_Orders', $3, $4, $5, 300, $6)", connection)
        {
            Parameters =
            {
                new NpgsqlParameter { Value = collectionId },
                new NpgsqlParameter { Value = DateTime.SpecifyKind(at, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp },
                new NpgsqlParameter { Value = (long)executions },
                new NpgsqlParameter { Value = (long)executions * cpuMs * 1000 },
                new NpgsqlParameter { Value = (long)executions * elapsedMs * 1000 },
                new NpgsqlParameter { Value = DateTime.SpecifyKind(modifyDate, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp },
            },
        };
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task AModuleChangeAfterTheReport_OpensTheFollowUpOnce_BeforeItsTimeComes()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var earlier = await ReportedAsync(store, ct);
            await SeedAsync(connection, 1, _now.AddHours(-1), Deployed1, 100, 10, 20, ct);
            await SeedAsync(connection, 2, RecentCollection, Deployed2, 100, 4, 8, ct);

            var later = new InvestigationRequestStore(dataSource, 30, () => SweepAt);
            Assert.Equal(1, (await later.SweepAsync(ct)).FollowUps);
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*)::text FROM config.investigation_requests WHERE follow_up_of = '{earlier}'", ct));
            Assert.Equal("deploy", await ScalarAsync(connection,
                "SELECT detail::jsonb ->> 'trigger' FROM config.investigation_journal WHERE event = 'follow_up'", ct));

            /* Once: a later pass, with the module still at its new date, opens nothing more. */
            Assert.Equal(0, (await later.SweepAsync(ct)).FollowUps);
        });
    }

    [Fact]
    public async Task AChangeOlderThanTheSweepsThirtyMinuteRead_IsNotReadByTheSweep()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var earlier = await ReportedAsync(store, ct);
            await SeedAsync(connection, 1, _now.AddHours(-1), Deployed1, 100, 10, 20, ct);

            /* The new modify_date came in a collection 90 minutes before the sweep: past the read's bound, so the sweep does not scan back to it. */
            await SeedAsync(connection, 2, SweepAt.AddMinutes(-90), Deployed2, 100, 4, 8, ct);

            var later = new InvestigationRequestStore(dataSource, 30, () => SweepAt);
            Assert.Equal(0, (await later.SweepAsync(ct)).FollowUps);
            Assert.Equal("0", await ScalarAsync(connection,
                $"SELECT count(*)::text FROM config.investigation_requests WHERE follow_up_of = '{earlier}'", ct));
        });
    }

    [Fact]
    public async Task ACollectorStampedChange_ReWakesTheFollowUpOnce_AndStopsBeingWatchedWhenItOpens()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var earlier = await ReportedAsync(store, ct);
            var watched = new[] { new PerformanceMonitor.Collectors.ProcedureStatsCollector.WatchedModule("Shop", "dbo", "usp_Orders") };

            /* The module is watched while its follow-up is pending, and by no other server. */
            Assert.Equal(watched, await ProcedureStatsModuleDates.ReadWatchedAsync(dataSource, 7, null, ct));
            Assert.Empty(await ProcedureStatsModuleDates.ReadWatchedAsync(dataSource, 8, null, ct));

            /* A collection the collector stamped: the rows are written with no date (the COPY), then the stamp puts the module's on. */
            await SeedAsync(connection, 1, RecentCollection, Deployed2, 100, 4, 8, ct);
            await ExecAsync(connection, "UPDATE collect.procedure_stats SET module_modify_date = NULL", ct);
            await SeedAsync(connection, 2, RecentCollection, Deployed2, 5, 1, 1, ct);
            await ExecAsync(connection, "UPDATE collect.procedure_stats SET module_modify_date = NULL, object_name = 'usp_Other' WHERE collection_id = 2", ct);
            var rows = new[]
            {
                default(PerformanceMonitor.Collectors.ProcedureStatsCollector.Row) with
                {
                    DatabaseName = "Shop", SchemaName = "dbo", ObjectName = "usp_Orders", ModuleModifyDate = Deployed2,
                },
                default(PerformanceMonitor.Collectors.ProcedureStatsCollector.Row) with
                {
                    DatabaseName = "Shop", SchemaName = "dbo", ObjectName = "usp_Other",
                },
            };
            await using (var transaction = await connection.BeginTransactionAsync(ct))
            {
                var stamped = await ProcedureStatsModuleDates.StampAsync(connection, transaction, 7, RecentCollection, rows, 30, null, ct);
                Assert.Equal(1, stamped);
                await transaction.CommitAsync(ct);
            }

            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*)::text FROM collect.procedure_stats WHERE module_modify_date IS NOT NULL", ct));
            Assert.Equal("usp_Orders", await ScalarAsync(connection, "SELECT object_name FROM collect.procedure_stats WHERE module_modify_date IS NOT NULL", ct));

            var later = new InvestigationRequestStore(dataSource, 30, () => SweepAt);
            Assert.Equal(1, (await later.SweepAsync(ct)).FollowUps);
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*)::text FROM config.investigation_requests WHERE follow_up_of = '{earlier}'", ct));
            Assert.Equal(0, (await later.SweepAsync(ct)).FollowUps);

            /* Once the follow-up has opened, nothing is watched: the collector goes back to sending no module query. */
            Assert.Empty(await ProcedureStatsModuleDates.ReadWatchedAsync(dataSource, 7, null, ct));
        });
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task AnUnchangedModule_OpensNothing_UntilTheTimeEndsIt()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var earlier = await ReportedAsync(store, ct);
            await SeedAsync(connection, 1, _now.AddHours(-1), Deployed1, 100, 10, 20, ct);
            await SeedAsync(connection, 2, RecentCollection, Deployed1, 100, 10, 20, ct);

            var later = new InvestigationRequestStore(dataSource, 30, () => SweepAt);
            Assert.Equal(0, (await later.SweepAsync(ct)).FollowUps);
            Assert.Equal("0", await ScalarAsync(connection,
                $"SELECT count(*)::text FROM config.investigation_requests WHERE follow_up_of = '{earlier}'", ct));

            /* The time still ends it, as a time follow-up (#6008). */
            var atTheEnd = new InvestigationRequestStore(dataSource, 30, () => DateTime.UtcNow.AddDays(21));
            Assert.Equal(1, (await atTheEnd.SweepAsync(ct)).FollowUps);
            Assert.Equal("time", await ScalarAsync(connection,
                "SELECT detail::jsonb ->> 'trigger' FROM config.investigation_journal WHERE event = 'follow_up'", ct));
        });
    }

    [Fact]
    public async Task TheReWokenBrief_ShowsTheProcedureStatsCountsBeforeAndAfterTheChange()
    {
        await RunAsync(async (store, dataSource, connection, ct) =>
        {
            var earlier = await ReportedAsync(store, ct);
            await SeedAsync(connection, 1, _now.AddHours(-2), Deployed1, 100, 10, 20, ct);
            await SeedAsync(connection, 2, _now.AddHours(-1), Deployed1, 100, 10, 20, ct);
            await SeedAsync(connection, 3, RecentCollection, Deployed2, 300, 4, 8, ct);

            var later = new InvestigationRequestStore(dataSource, 30, () => SweepAt);
            Assert.Equal(1, (await later.SweepAsync(ct)).FollowUps);
            var followUpId = Guid.Parse((await ScalarAsync(connection,
                $"SELECT request_id::text FROM config.investigation_requests WHERE follow_up_of = '{earlier}'", ct))!);

            var sources = await new PgInvestigationBriefSourceReader(dataSource, utcNow: () => SweepAt).ReadAsync(followUpId, ct);
            var section = Assert.Single(sources!.Evidence!, e => e.Section == "deploy_follow_up");
            Assert.Equal("ready", section.State);
            var content = JsonDocument.Parse(section.ContentJson!).RootElement;
            Assert.Equal("Shop.dbo.usp_Orders", content.GetProperty("module").GetString());
            var before = content.GetProperty("before");
            var after = content.GetProperty("after");
            Assert.Equal(200, before.GetProperty("executions").GetInt64());
            Assert.Equal(10.0, before.GetProperty("avg_cpu_ms").GetDouble());
            Assert.Equal(20.0, before.GetProperty("avg_duration_ms").GetDouble());
            Assert.Equal(300, after.GetProperty("executions").GetInt64());
            Assert.Equal(4.0, after.GetProperty("avg_cpu_ms").GetDouble());
            Assert.Equal(8.0, after.GetProperty("avg_duration_ms").GetDouble());
        });
    }

    private async Task RunAsync(Func<InvestigationRequestStore, NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the deploy follow-up live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var store = new InvestigationRequestStore(dataSource, 30, () => _now);
        await body(store, dataSource, connection, ct);
    }
}
