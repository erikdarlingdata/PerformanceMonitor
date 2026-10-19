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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6072: <c>get_cpu_by_database</c>, <c>get_load_by_caller</c> and <c>get_query_pileups</c> over seeded rows, each class method on a scratch database of
/// its own. Seed and read share one data source. Every time is an offset from <see cref="LiveClock.AnchoredEnd"/>, so no wall clock decides what the
/// window holds.
/// </summary>
[Collection("live-postgres")]
public sealed class WhoDroveItLiveTests
{
    private const string ServerName = "darling-who-drove-it-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string DbA = "SalesDb";
    private const string DbB = "ReportDb";
    private const string DbC = "ProcOnlyDb";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime End => LiveClock.AnchoredEnd();

    private static string AsOf => End.ToString("o", CultureInfo.InvariantCulture);

    private static async Task RunAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live who-drove-it tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using (var seed = await postgres.OpenConnectionAsync(ct))
        {
            await DarlingMcpTestData.ExecAsync(seed, ct,
                "INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date) VALUES ($1, $2, $2, TRUE, $3, $3)",
                ServerId, ServerName, DarlingMcpTestData.Naive(DateTime.UtcNow));
        }

        await body(postgres, ct);
    }

    private static async Task StatementAsync(
        NpgsqlDataSource postgres, CancellationToken ct, string database, string hash, string planHash, DateTime at, long cpuUs, long executions, string text)
    {
        await using var seed = await postgres.OpenConnectionAsync(ct);
        await DarlingMcpTestData.ExecAsync(seed, ct,
            @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                                               query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
              VALUES ($1,$2,$3,$4,$5,$6,$7,'0xH','0xP',$8,$9,$10,$11,300)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, database, hash, planHash, text, executions, cpuUs, cpuUs * 2);
    }

    private static async Task ProcedureAsync(NpgsqlDataSource postgres, CancellationToken ct, string database, string name, DateTime at, long cpuUs, long executions)
    {
        await using var seed = await postgres.OpenConnectionAsync(ct);
        await DarlingMcpTestData.ExecAsync(seed, ct,
            @"INSERT INTO collect.procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                                                   delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
              VALUES ($1,$2,$3,$4,$5,'dbo',$6,'0xS',$7,$7,$8,300)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, database, name, cpuUs, executions);
    }

    private static async Task SnapshotAsync(
        NpgsqlDataSource postgres, CancellationToken ct, DateTime at, int session, string hash, string login, string host, string program,
        long cpuMs, long elapsedMs, string? waitType = null, int blockedBy = 0, long waitMs = 0, string text = "SELECT 1", string database = DbA)
    {
        await using var seed = await postgres.OpenConnectionAsync(ct);
        await DarlingMcpTestData.ExecAsync(seed, ct,
            @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status,
                                           blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, login_name, host_name, program_name, query_hash, request_id)
              VALUES ($1,$2,$3,$4,$5,$6,'00 00:00:05.000',$7,'running',$8,$9,$10,$11,$12,$13,$14,$15,$16,0)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, session, database, text, blockedBy, waitType, waitMs, cpuMs, elapsedMs, login, host, program, hash);
    }

    private static async Task BlockingAsync(NpgsqlDataSource postgres, CancellationToken ct, DateTime at, int blocked, int blocking, long waitMs, string blockerLogin)
    {
        await using var seed = await postgres.OpenConnectionAsync(ct);
        await DarlingMcpTestData.ExecAsync(seed, ct,
            @"INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, monitor_loop, event_time, database_name, blocked_spid, blocked_ecid,
                                                  blocked_last_tran_started, blocking_spid, blocking_ecid, blocking_last_tran_started, wait_time_ms, lock_mode, blocking_status,
                                                  contentious_object, blocked_sql_text, blocking_sql_text, blocked_login_name, blocked_host_name, blocked_client_app,
                                                  blocking_login_name, blocking_host_name, blocking_client_app)
              VALUES ($1,$2,$3,$4,1,$2,$5,$6,0,$2,$7,0,$2,$8,'KEY','sleeping','dbo.Orders','SELECT blocked','UPDATE dbo.Orders SET x = 1',
                      'victim','victimhost','victimapp',$9,'blockerhost','blockerapp')",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, DbA, blocked, blocking, waitMs, blockerLogin);
    }

    /* ───────────────────────── get_cpu_by_database ───────────────────────── */

    /// <summary>
    /// SalesDb: hash H1 runs under two plans (300 ms + 300 ms), H2 100 ms, so 700 ms. ReportDb: H3, 300 ms. Server total 1,000 ms. The procedure stats hold a
    /// procedure in SalesDb at 650 ms (its statements are already inside the 700), and a procedure-only database ProcOnlyDb.
    /// </summary>
    private static async Task SeedCpuAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        var t = End.AddHours(-2);
        await StatementAsync(postgres, ct, DbA, "0xH1", "0xPA", t, 300_000, 3, "SELECT * FROM dbo.Orders WHERE Id = 1");
        await StatementAsync(postgres, ct, DbA, "0xH1", "0xPB", t.AddMinutes(5), 300_000, 2, "SELECT * FROM dbo.Orders WHERE Id = 2");
        await StatementAsync(postgres, ct, DbA, "0xH2", "0xPC", t, 100_000, 1, "SELECT 2");
        await StatementAsync(postgres, ct, DbB, "0xH3", "0xPD", t, 300_000, 4, "SELECT 3");
        await ProcedureAsync(postgres, ct, DbA, "usp_Heavy", t, 650_000, 5);
        await ProcedureAsync(postgres, ct, DbC, "usp_Lonely", t, 900_000, 1);
    }

    [Fact]
    public Task CpuByDatabase_RanksByCpu_WithTheShareOfTheServerTotal_AndTheTopStatementOfEach() => RunAsync(async (postgres, ct) =>
    {
        await SeedCpuAsync(postgres, ct);
        using var doc = JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetCpuByDatabaseCore(postgres, ServerName, 4, 20, AsOf, ct));
        var root = doc.RootElement;
        var dbs = root.GetProperty("databases").EnumerateArray().ToArray();
        Assert.Equal(new[] { DbA, DbB }, dbs.Select(d => d.GetProperty("database_name").GetString()));
        Assert.Equal(1000.0, root.GetProperty("server_cpu_ms").GetDouble(), 3);
        Assert.Equal(700.0, dbs[0].GetProperty("cpu_ms").GetDouble(), 3);
        Assert.Equal(0.7, dbs[0].GetProperty("share").GetDouble(), 4);
        Assert.Equal(0.3, dbs[1].GetProperty("share").GetDouble(), 4);
        /* The statement that ran under two plans is ONE statement: 600 ms, 6 of the database's 700 ms in share. */
        var top = dbs[0].GetProperty("top_statement");
        Assert.Equal("0xH1", top.GetProperty("query_hash").GetString());
        Assert.Equal(600.0, top.GetProperty("cpu_ms").GetDouble(), 3);
        Assert.Equal(Math.Round(600.0 / 700.0, 4), top.GetProperty("share_of_database").GetDouble(), 4);
        Assert.Equal(5, top.GetProperty("executions").GetInt64());
        Assert.Equal(2, dbs[0].GetProperty("statements").GetInt64());
        Assert.StartsWith("SELECT * FROM dbo.Orders", top.GetProperty("query_text").GetString(), StringComparison.Ordinal);
        Assert.False(root.GetProperty("truncated").GetBoolean());
    });

    [Fact]
    public Task CpuByDatabase_DoesNotCountAProcedureStatementTwice_AndRanksByStatementCpuOnly() => RunAsync(async (postgres, ct) =>
    {
        await SeedCpuAsync(postgres, ct);
        using var doc = JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetCpuByDatabaseCore(postgres, ServerName, 4, 20, AsOf, ct));
        var root = doc.RootElement;
        var dbs = root.GetProperty("databases").EnumerateArray().ToArray();
        /* The server total is the statement CPU alone: adding the procedures' 650 ms (and the other database's 900 ms) would say 2,550. */
        Assert.Equal(1000.0, root.GetProperty("server_cpu_ms").GetDouble(), 3);
        Assert.Equal(1000.0, dbs.Sum(d => d.GetProperty("cpu_ms").GetDouble()), 3);
        /* The procedure figure rides beside the statement figure, never added to it. */
        Assert.Equal(650.0, dbs[0].GetProperty("procedure_cpu_ms").GetDouble(), 3);
        Assert.Equal("dbo.usp_Heavy", dbs[0].GetProperty("top_procedure").GetProperty("name").GetString());
        Assert.Equal(700.0, dbs[0].GetProperty("cpu_ms").GetDouble(), 3);
        Assert.Equal(JsonValueKind.Null, dbs[1].GetProperty("procedure_cpu_ms").ValueKind);
        /* A database only the procedure stats know is not ranked: its work is not in the plan cache's statements. */
        Assert.DoesNotContain(dbs, d => d.GetProperty("database_name").GetString() == DbC);
        Assert.Contains("never added", root.GetProperty("overlap_note").GetString(), StringComparison.Ordinal);
        Assert.Equal("collect.query_stats", root.GetProperty("cpu_source").GetString());
    });

    [Fact]
    public Task CpuByDatabase_ShareIsOfTheServer_NotOfThePage_AndACutPageIsSaid() => RunAsync(async (postgres, ct) =>
    {
        await SeedCpuAsync(postgres, ct);
        using var doc = JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetCpuByDatabaseCore(postgres, ServerName, 4, 1, AsOf, ct));
        var root = doc.RootElement;
        Assert.True(root.GetProperty("truncated").GetBoolean());
        var only = Assert.Single(root.GetProperty("databases").EnumerateArray());
        Assert.Equal(0.7, only.GetProperty("share").GetDouble(), 4);
        Assert.Equal(1000.0, root.GetProperty("server_cpu_ms").GetDouble(), 3);
    });

    [Fact]
    public Task CpuByDatabase_AnEmptyWindow_SaysWhy_AndABadTopIsRefused() => RunAsync(async (postgres, ct) =>
    {
        await SeedCpuAsync(postgres, ct);
        using var empty = JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetCpuByDatabaseCore(postgres, ServerName, 1, 20, End.AddHours(-12).ToString("o", CultureInfo.InvariantCulture), ct));
        Assert.Equal("empty", empty.RootElement.GetProperty("status").GetString());
        Assert.Contains("No statement CPU", empty.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        using var refused = JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetCpuByDatabaseCore(postgres, ServerName, 4, DarlingMcpWhoDroveItTools.MaxTop + 1, AsOf, ct));
        Assert.Equal("invalid", refused.RootElement.GetProperty("status").GetString());
    });

    /* ───────────────────────── get_load_by_caller ───────────────────────── */

    /// <summary>
    /// (APP01, SSMS, sa): one request (session 51) seen twice, a minute apart, at 3,000 ms CPU, with hash HA (twice), 4 s then 64 s elapsed.
    /// (WEB01, api, svc): session 60 seen twice (one request, 1,000 ms, HB twice) and session 61 once (a second request, 1,000 ms, HC).
    /// (WEB02, api, svc2): one request at 1,000 ms. Total 6,000 ms CPU, 4 requests, 6 samples. Elapsed grows with the capture time, as it does for a live request.
    /// </summary>
    private static async Task SeedCallersAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        var t = End.AddMinutes(-30);
        await SnapshotAsync(postgres, ct, t, 51, "0xHA", "sa", "APP01", "SSMS", 3000, 4000, text: "SELECT secret FROM dbo.Payroll WHERE ssn = '123-45-6789'");
        await SnapshotAsync(postgres, ct, t.AddMinutes(1), 51, "0xHA", "sa", "APP01", "SSMS", 3000, 64000, text: "SELECT secret FROM dbo.Payroll WHERE ssn = '123-45-6789'");
        await SnapshotAsync(postgres, ct, t, 60, "0xHB", "svc", "WEB01", "api", 1000, 1500, text: "SELECT 1");
        await SnapshotAsync(postgres, ct, t.AddMinutes(1), 60, "0xHB", "svc", "WEB01", "api", 1000, 61500, text: "SELECT 1");
        await SnapshotAsync(postgres, ct, t.AddMinutes(2), 61, "0xHC", "svc", "WEB01", "api", 1000, 1500, text: "SELECT 3");
        await SnapshotAsync(postgres, ct, t, 70, "0xHD", "svc2", "WEB02", "api", 1000, 1500, text: "SELECT 4");
        /* A WAITFOR shell is not load. */
        await SnapshotAsync(postgres, ct, t, 99, "0xHW", "svc", "WEB01", "api", 5000, 5000, text: "WAITFOR DELAY '00:10:00'");
    }

    [Fact]
    public Task LoadByCaller_GroupsByHostProgramAndLogin_WithTheSharesAndTheTopStatements() => RunAsync(async (postgres, ct) =>
    {
        await SeedCallersAsync(postgres, ct);
        using var doc = JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetLoadByCallerCore(postgres, ServerName, 1, 20, 2, AsOf, ct));
        var root = doc.RootElement;
        var callers = root.GetProperty("callers").EnumerateArray().ToArray();
        Assert.Equal(new[] { "APP01", "WEB01", "WEB02" }, callers.Select(c => c.GetProperty("host_name").GetString()));
        Assert.Equal(6, root.GetProperty("total_samples").GetInt64());
        Assert.Equal(6000.0, root.GetProperty("total_cpu_ms").GetDouble(), 3);
        Assert.Equal(4, root.GetProperty("total_requests").GetInt64());

        var app = callers[0];
        Assert.Equal("SSMS", app.GetProperty("program_name").GetString());
        Assert.Equal("sa", app.GetProperty("login_name").GetString());
        Assert.Equal(2, app.GetProperty("samples").GetInt64());
        Assert.Equal(2, app.GetProperty("captures").GetInt64());
        Assert.Equal(1, app.GetProperty("requests").GetInt64());
        Assert.Equal(3000.0, app.GetProperty("cpu_ms").GetDouble(), 3);
        Assert.Equal(0.5, app.GetProperty("cpu_share").GetDouble(), 4);
        Assert.Equal(64000.0, app.GetProperty("elapsed_ms").GetDouble(), 3);
        Assert.Equal(Math.Round(2 / 6.0, 4), app.GetProperty("sample_share").GetDouble(), 4);
        Assert.Equal("0xHA", Assert.Single(app.GetProperty("top_statements").EnumerateArray()).GetProperty("query_hash").GetString());

        var web = callers[1];
        Assert.Equal(3, web.GetProperty("samples").GetInt64());
        Assert.Equal(2, web.GetProperty("requests").GetInt64());
        Assert.Equal(2000.0, web.GetProperty("cpu_ms").GetDouble(), 3);
        Assert.Equal(Math.Round(2000 / 6000.0, 4), web.GetProperty("cpu_share").GetDouble(), 4);
        /* Two statements asked, most CPU first: HB has two samples, HC one. The WAITFOR shell is in neither the samples nor the totals. */
        Assert.Equal(new[] { "0xHB", "0xHC" }, web.GetProperty("top_statements").EnumerateArray().Select(s => s.GetProperty("query_hash").GetString()));
        Assert.Equal(Math.Round(1000 / 6000.0, 4), callers[2].GetProperty("cpu_share").GetDouble(), 4);
    });

    [Fact]
    public Task LoadByCaller_MasksLoginsAndHostsUnderMetadataOnly_AndKeepsProgramsAndNumbers() => RunAsync(async (postgres, ct) =>
    {
        await SeedCallersAsync(postgres, ct);
        var full = await DarlingMcpWhoDroveItTools.GetLoadByCallerCore(postgres, ServerName, 1, 20, 2, AsOf, ct);
        Assert.Contains("APP01", full, StringComparison.Ordinal);
        Assert.Contains("\"sa\"", full, StringComparison.Ordinal);

        var masked = McpDataDetailMasker.MaskJson(full, McpDataDetail.MetadataOnly);
        Assert.DoesNotContain("APP01", masked, StringComparison.Ordinal);
        Assert.DoesNotContain("WEB01", masked, StringComparison.Ordinal);
        Assert.DoesNotContain("\"sa\"", masked, StringComparison.Ordinal);
        Assert.DoesNotContain("svc2", masked, StringComparison.Ordinal);
        Assert.DoesNotContain("123-45-6789", masked, StringComparison.Ordinal);
        using var doc = JsonDocument.Parse(masked);
        var callers = doc.RootElement.GetProperty("callers").EnumerateArray().ToArray();
        Assert.All(callers, c =>
        {
            Assert.StartsWith("host#", c.GetProperty("host_name").GetString(), StringComparison.Ordinal);
            Assert.StartsWith("login#", c.GetProperty("login_name").GetString(), StringComparison.Ordinal);
        });
        /* One login is one number everywhere; two logins never share one. */
        Assert.Equal(3, callers.Select(c => c.GetProperty("login_name").GetString()).Distinct().Count());
        Assert.Equal(new[] { "SSMS", "api", "api" }, callers.Select(c => c.GetProperty("program_name").GetString()));
        Assert.Equal(3000.0, callers[0].GetProperty("cpu_ms").GetDouble(), 3);
    });

    [Fact]
    public Task LoadByCaller_AnEmptyWindow_SaysWhy_AndABadStatementCountIsRefused() => RunAsync(async (postgres, ct) =>
    {
        await SeedCallersAsync(postgres, ct);
        using var empty = JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetLoadByCallerCore(postgres, ServerName, 1, 20, 2, End.AddHours(-12).ToString("o", CultureInfo.InvariantCulture), ct));
        Assert.Equal("empty", empty.RootElement.GetProperty("status").GetString());
        Assert.Contains("No active-query snapshots", empty.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        using var refused = JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetLoadByCallerCore(postgres, ServerName, 1, 20, DarlingMcpWhoDroveItTools.MaxStatements + 1, AsOf, ct));
        Assert.Equal("invalid", refused.RootElement.GetProperty("status").GetString());
    });

    /// <summary>Reads get_load_by_caller for the window and returns its parsed document.</summary>
    private static async Task<JsonDocument> LoadByCallerAsync(NpgsqlDataSource postgres, CancellationToken ct, int top = 20) =>
        JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetLoadByCallerCore(postgres, ServerName, 1, top, 2, AsOf, ct));

    /// <summary>
    /// Ten captures of one request on session 80: CPU 100, 200 ... 1000 ms, elapsed growing with the capture time. The request started 5.5 s before the
    /// first capture, so the few-millisecond jitter added to each elapsed figure stays inside one second of the start and keeps the captures together.
    /// </summary>
    private static async Task SeedOneRequestAsync(NpgsqlDataSource postgres, CancellationToken ct, DateTime t, int session, string host)
    {
        for (var i = 0; i < 10; i++)
        {
            await SnapshotAsync(postgres, ct, t.AddSeconds(10 * i), session, "0xHL", "svc", host, "api", 100 * (i + 1), 5500 + 10_000 * i + (i % 3));
        }
    }

    [Fact]
    public Task LoadByCaller_ARequestSeenInTenCaptures_CountsOnceAtItsLargestCpu() => RunAsync(async (postgres, ct) =>
    {
        await SeedOneRequestAsync(postgres, ct, End.AddMinutes(-30), 80, "LONG01");
        using var doc = await LoadByCallerAsync(postgres, ct);
        var caller = Assert.Single(doc.RootElement.GetProperty("callers").EnumerateArray());
        Assert.Equal(1, caller.GetProperty("requests").GetInt64());
        Assert.Equal(10, caller.GetProperty("samples").GetInt64());
        Assert.Equal(10, caller.GetProperty("captures").GetInt64());
        Assert.Equal(1000.0, caller.GetProperty("cpu_ms").GetDouble(), 3);
        Assert.Equal(95_500.0, caller.GetProperty("elapsed_ms").GetDouble(), 3);
        Assert.Equal(1000.0, doc.RootElement.GetProperty("total_cpu_ms").GetDouble(), 3);
        var statement = Assert.Single(caller.GetProperty("top_statements").EnumerateArray());
        Assert.Equal(1, statement.GetProperty("requests").GetInt64());
        Assert.Equal(1000.0, statement.GetProperty("cpu_ms").GetDouble(), 3);
    });

    [Fact]
    public Task LoadByCaller_OneLongRequest_RanksBelowFiveShortOnesThatUsedMoreCpu() => RunAsync(async (postgres, ct) =>
    {
        var t = End.AddMinutes(-30);
        await SeedOneRequestAsync(postgres, ct, t, 80, "LONG01");
        for (var i = 0; i < 5; i++)
        {
            await SnapshotAsync(postgres, ct, t.AddSeconds(20 * i), 90 + i, "0xHS", "svc2", "SHORT01", "api", 300, 2500);
        }

        using var doc = await LoadByCallerAsync(postgres, ct);
        var callers = doc.RootElement.GetProperty("callers").EnumerateArray().ToArray();
        Assert.Equal(new[] { "SHORT01", "LONG01" }, callers.Select(c => c.GetProperty("host_name").GetString()));
        Assert.Equal(5, callers[0].GetProperty("requests").GetInt64());
        Assert.Equal(1500.0, callers[0].GetProperty("cpu_ms").GetDouble(), 3);
        Assert.Equal(1, callers[1].GetProperty("requests").GetInt64());
        Assert.Equal(1000.0, callers[1].GetProperty("cpu_ms").GetDouble(), 3);
        Assert.Equal(Math.Round(1500 / 2500.0, 4), callers[0].GetProperty("cpu_share").GetDouble(), 4);
    });

    [Fact]
    public Task LoadByCaller_TwoRequestsOnOneSession_OneAfterTheOther_CountAsTwo() => RunAsync(async (postgres, ct) =>
    {
        var t = End.AddMinutes(-30);
        /* The first request is seen at t and finishes. A minute later the same session starts another: its elapsed time restarts, so its start differs. */
        await SnapshotAsync(postgres, ct, t, 70, "0xHA", "svc", "APP01", "api", 400, 5500);
        await SnapshotAsync(postgres, ct, t.AddMinutes(1), 70, "0xHB", "svc", "APP01", "api", 700, 5500);
        using var doc = await LoadByCallerAsync(postgres, ct);
        var caller = Assert.Single(doc.RootElement.GetProperty("callers").EnumerateArray());
        Assert.Equal(2, caller.GetProperty("requests").GetInt64());
        Assert.Equal(1100.0, caller.GetProperty("cpu_ms").GetDouble(), 3);
        Assert.Equal(11_000.0, caller.GetProperty("elapsed_ms").GetDouble(), 3);
        Assert.Equal(2, doc.RootElement.GetProperty("total_requests").GetInt64());
    });

    [Fact]
    public Task LoadByCaller_WithTopBelowTheCallerCount_EachShareIsOfTheWholeWindow() => RunAsync(async (postgres, ct) =>
    {
        var t = End.AddMinutes(-30);
        await SnapshotAsync(postgres, ct, t, 51, "0xHA", "sa", "APP01", "SSMS", 5000, 5500);
        await SnapshotAsync(postgres, ct, t, 60, "0xHB", "svc", "WEB01", "api", 3000, 5500);
        await SnapshotAsync(postgres, ct, t, 61, "0xHB", "svc", "WEB01", "api", 0, 5500);
        await SnapshotAsync(postgres, ct, t, 70, "0xHD", "svc2", "WEB02", "api", 2000, 5500);
        using var doc = await LoadByCallerAsync(postgres, ct, top: 2);
        var root = doc.RootElement;
        Assert.True(root.GetProperty("truncated").GetBoolean());
        var callers = root.GetProperty("callers").EnumerateArray().ToArray();
        Assert.Equal(new[] { "APP01", "WEB01" }, callers.Select(c => c.GetProperty("host_name").GetString()));
        /* The page holds 8,000 of the window's 10,000 ms. A share of the page would read 0.625 and 0.375. */
        Assert.Equal(10_000.0, root.GetProperty("total_cpu_ms").GetDouble(), 3);
        Assert.Equal(0.5, callers[0].GetProperty("cpu_share").GetDouble(), 4);
        Assert.Equal(0.3, callers[1].GetProperty("cpu_share").GetDouble(), 4);
        Assert.Equal(4, root.GetProperty("total_samples").GetInt64());
        Assert.Equal(0.25, callers[0].GetProperty("sample_share").GetDouble(), 4);
        Assert.Equal(0.5, callers[1].GetProperty("sample_share").GetDouble(), 4);
        Assert.Equal(0.25, callers[0].GetProperty("elapsed_share").GetDouble(), 4);
    });

    /* ───────────────────────── get_query_pileups ───────────────────────── */

    /// <summary>
    /// T1: five sessions on HP (one session twice, so six rows): three wait on LCK_M_S, two run. T2: four sessions on HP (one fewer than the default).
    /// T3: five sessions, three on HP and two on HQ. T4: six sessions on HR, with no blocking snapshot near it.
    /// Blocking: a snapshot 45 s BEFORE T1 whose head is spid 77, a snapshot 20 s AFTER T1 (the nearer) where 101 and 102 wait on 90 and 90 waits on 91 (head 91, three
    /// behind it), and a snapshot an hour from T1 whose head is spid 99.
    /// </summary>
    private static async Task SeedPileupsAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        var t1 = End.AddMinutes(-90);
        var t2 = End.AddMinutes(-80);
        var t3 = End.AddMinutes(-70);
        var t4 = End.AddMinutes(-20);
        for (var s = 1; s <= 5; s++)
        {
            await SnapshotAsync(postgres, ct, t1, 100 + s, "0xHP", "app", "H" + s, "api", 10, 20, waitType: s <= 3 ? "LCK_M_S" : null, blockedBy: s <= 3 ? 90 : 0, waitMs: s <= 3 ? 500 : 0);
        }

        await SnapshotAsync(postgres, ct, t1, 101, "0xHP", "app", "H1", "api", 10, 20, waitType: "LCK_M_S", blockedBy: 90, waitMs: 400);
        for (var s = 1; s <= 4; s++)
        {
            await SnapshotAsync(postgres, ct, t2, 200 + s, "0xHP", "app", "H" + s, "api", 10, 20);
        }

        for (var s = 1; s <= 5; s++)
        {
            await SnapshotAsync(postgres, ct, t3, 300 + s, s <= 3 ? "0xHP" : "0xHQ", "app", "H" + s, "api", 10, 20);
        }

        for (var s = 1; s <= 6; s++)
        {
            await SnapshotAsync(postgres, ct, t4, 400 + s, "0xHR", "app", "H" + s, "api", 10, 20, waitType: "PAGEIOLATCH_SH");
        }

        await BlockingAsync(postgres, ct, t1.AddSeconds(-45), 55, 77, 100, "early-blocker");
        await BlockingAsync(postgres, ct, t1.AddSeconds(20), 101, 90, 900, "head-blocker");
        await BlockingAsync(postgres, ct, t1.AddSeconds(20), 102, 90, 700, "head-blocker");
        await BlockingAsync(postgres, ct, t1.AddSeconds(20), 90, 91, 100, "middle-blocker");
        await BlockingAsync(postgres, ct, t1.AddMinutes(60), 56, 99, 100, "late-blocker");
    }

    [Fact]
    public Task Pileups_ListATimeWithFiveSessionsOnOneHash_AndNotATimeWithFour() => RunAsync(async (postgres, ct) =>
    {
        await SeedPileupsAsync(postgres, ct);
        using var doc = JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetQueryPileupsCore(postgres, ServerName, 4, 5, 10, AsOf, ct));
        var piles = doc.RootElement.GetProperty("pileups").EnumerateArray().ToArray();
        /* T4 (six sessions) first, then T1 (five distinct sessions among six rows). T2 has four and T3 has three on one hash: not listed. */
        Assert.Equal(new[] { "0xHR", "0xHP" }, piles.Select(p => p.GetProperty("query_hash").GetString()));
        Assert.Equal(new long[] { 6, 5 }, piles.Select(p => p.GetProperty("sessions").GetInt64()));
        var t1 = piles[1];
        Assert.Equal(End.AddMinutes(-90).ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture), t1.GetProperty("collection_time").GetString()![..19]);

        /* The waits add up to the sessions: three on LCK_M_S, two with none. */
        var waits = t1.GetProperty("waits").EnumerateArray().ToDictionary(w => w.GetProperty("wait_type").GetString()!, w => w.GetProperty("sessions").GetInt64());
        Assert.Equal(3, waits["LCK_M_S"]);
        Assert.Equal(2, waits["(none)"]);
        Assert.Equal(3, t1.GetProperty("blocked_sessions").GetInt64());

        /* One fewer is the line: a lower threshold lists the time with four sessions, and a higher one drops the five. */
        using var four = JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetQueryPileupsCore(postgres, ServerName, 4, 4, 10, AsOf, ct));
        Assert.Equal(3, four.RootElement.GetProperty("pileups").GetArrayLength());
        using var six = JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetQueryPileupsCore(postgres, ServerName, 4, 6, 10, AsOf, ct));
        Assert.Equal(new[] { "0xHR" }, six.RootElement.GetProperty("pileups").EnumerateArray().Select(p => p.GetProperty("query_hash").GetString()));
    });

    [Fact]
    public Task Pileups_TheHeadBlockerComesFromTheNearestBlockingSnapshot_AndSaysHowNear() => RunAsync(async (postgres, ct) =>
    {
        await SeedPileupsAsync(postgres, ct);
        using var doc = JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetQueryPileupsCore(postgres, ServerName, 4, 5, 10, AsOf, ct));
        var piles = doc.RootElement.GetProperty("pileups").EnumerateArray().ToArray();
        var t1 = piles.Single(p => p.GetProperty("query_hash").GetString() == "0xHP");
        /* The snapshot 20 s after is nearer than the one 45 s before, and the one an hour away is outside the tolerance. */
        Assert.Equal(20.0, t1.GetProperty("blocking_snapshot_offset_seconds").GetDouble(), 1);
        /* In that snapshot 101 and 102 wait on 90 and 90 waits on 91, so 91 is the head of the chain, with three sessions behind it. */
        var head = t1.GetProperty("head_blocker");
        Assert.Equal(91, head.GetProperty("spid").GetInt32());
        Assert.Equal("middle-blocker", head.GetProperty("login_name").GetString());
        Assert.Equal(3, head.GetProperty("blocked_sessions").GetInt32());
        Assert.Equal(End.AddMinutes(-90).AddSeconds(20).ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture), t1.GetProperty("blocking_snapshot_time").GetString()![..19]);

        /* A pile-up with no blocking snapshot near it names none, and says so. */
        var t4 = piles.Single(p => p.GetProperty("query_hash").GetString() == "0xHR");
        Assert.Equal(JsonValueKind.Null, t4.GetProperty("head_blocker").ValueKind);
        Assert.Contains("No DMV blocking snapshot within", t4.GetProperty("head_blocker_note").GetString(), StringComparison.Ordinal);
        Assert.Equal(60, doc.RootElement.GetProperty("blocking_tolerance_seconds").GetInt32());
    });

    [Fact]
    public Task Pileups_AnEmptyWindow_SaysWhy_AndAThresholdBelowTwoIsRefused() => RunAsync(async (postgres, ct) =>
    {
        await SeedPileupsAsync(postgres, ct);
        using var empty = JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetQueryPileupsCore(postgres, ServerName, 4, 50, 10, AsOf, ct));
        Assert.Equal("empty", empty.RootElement.GetProperty("status").GetString());
        Assert.Contains("50 or more sessions", empty.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        using var refused = JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetQueryPileupsCore(postgres, ServerName, 4, 1, 10, AsOf, ct));
        Assert.Equal("invalid", refused.RootElement.GetProperty("status").GetString());
    });

    [Fact]
    public Task Pileups_MaskTheBlockersLoginAndHost_UnderMetadataOnly() => RunAsync(async (postgres, ct) =>
    {
        await SeedPileupsAsync(postgres, ct);
        var full = await DarlingMcpWhoDroveItTools.GetQueryPileupsCore(postgres, ServerName, 4, 5, 10, AsOf, ct);
        Assert.Contains("middle-blocker", full, StringComparison.Ordinal);
        Assert.Contains("blockerhost", full, StringComparison.Ordinal);
        var masked = McpDataDetailMasker.MaskJson(full, McpDataDetail.MetadataOnly);
        Assert.DoesNotContain("middle-blocker", masked, StringComparison.Ordinal);
        Assert.DoesNotContain("blockerhost", masked, StringComparison.Ordinal);
        Assert.DoesNotContain("dbo.Orders SET", masked, StringComparison.Ordinal);
        Assert.Contains("login#", masked, StringComparison.Ordinal);
    });
}
