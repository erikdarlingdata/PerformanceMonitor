/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.DarlingWebNamedTokenTests;

namespace Darling.Tests;

/// <summary>
/// #7988: a named reader token with <c>allowedTagIds</c> sees the servers under those tags and nothing else, on every surface the
/// dashboard serves it: the fleet card list, every server read (by name, by id, by partial, by sentinel), the fleet reads, alert
/// history and details, investigations, composed panels, the sweep documents, and the read cache. The pipeline is the real one
/// (<see cref="DarlingWebNamedTokenTests.BuildServer"/>) over a scratch store seeded with one row per table family for a permitted
/// and a hidden server, so each 200 is a checked value and each refusal is tried against a row that exists.
/// </summary>
/* #1776 own-store: this class creates and drops its own scratch database. */
public sealed class ScopedWebTokenLiveTests
{
    /// <summary>The per-server reads that answer 400 to a bare <c>?server=</c> before any row is read: each refuses for the required
    /// parameter named here, so the permitted case cannot be asked for a row with the server alone. Every other per-server read answers
    /// 200 against the seeded row. A read that starts answering 400 or 404 for another reason fails the census instead of passing it.</summary>
    private static readonly IReadOnlyDictionary<string, string> AnswersWithoutARow = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        ["/api/read/get_plan_xml"] = "requires plan_handle or query_hash",
        ["/api/read/get_query_store_plan_xml"] = "requires plan_id",
        ["/api/read/get_blocking_plan_xml"] = "requires a blocked-process report id",
        ["/api/read/get_deadlock_plan_xml"] = "requires deadlock_id",
        ["/api/read/get_procedure_plan_xml"] = "requires sql_handle",
        ["/api/read/get_active_query_plan_xml"] = "requires collection_time",
        ["/api/read/get_query_repro_script"] = "requires query_hash",
        ["/api/read/get_block_chain"] = "requires event_time",
        ["/api/read/get_query_store_query_history"] = "requires database_name",
        ["/api/read/get_plan_force_precheck"] = "requires database_name",
        ["/api/read/get_server_trend"] = "requires metric",
        ["/api/read/get_perfmon_trend"] = "requires counter_name",
        ["/api/read/get_query_trend"] = "requires query_hash",
        ["/api/read/get_overview_baselines"] = "requires window_start",
        ["/api/read/get_finops"] = "requires view",
    };

    [Fact]
    public async Task ScopedReaders_SeeOnlyTheirTagSubtrees()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the scoped-token test against PostgreSQL.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        var settings = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var postgres = NpgsqlDataSource.Create(settings.ConnectionString);
        await using var connection = await postgres.OpenConnectionAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);

        await DarlingMcpTestData.RegisterServerAsync(connection, 41, "finance-server", ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, 42, "operations-server", ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, 43, "prod-only-server", ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, 44, "untagged-server", ct);

        async Task<int> AddTag(string name, int? parentId, int serverId)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                WITH tag AS (
                    INSERT INTO server_tags (name, parent_id) VALUES ($1, $2) RETURNING id
                )
                INSERT INTO server_tag_map (server_id, tag_id)
                SELECT $3, id FROM tag
                RETURNING tag_id
                """;
            command.Parameters.AddWithValue(name);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Integer, (object?)parentId ?? DBNull.Value);
            command.Parameters.AddWithValue(serverId);
            return (int)(await command.ExecuteScalarAsync(ct))!;
        }

        var financeRequestId = Guid.NewGuid();
        var operationsRequestId = Guid.NewGuid();
        var sampledAt = DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-2));
        foreach (var (id, name, cpu, waitType) in new[]
        {
            (41, "finance-server", 11, "FINANCE_WAIT"),
            (42, "operations-server", 92, "OPERATIONS_WAIT"),
        })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, """
                INSERT INTO cpu_utilization_stats
                (collection_id, collection_time, server_id, server_name, sample_time,
                 sqlserver_cpu_utilization, other_process_cpu_utilization)
                VALUES ($1, $2, $3, $4, $2, $5, 0)
                """, CollectionIdGenerator.Next(), sampledAt, id, name, cpu);
            await DarlingMcpTestData.ExecAsync(connection, ct, """
                INSERT INTO memory_stats
                (collection_id, collection_time, server_id, server_name,
                 total_physical_memory_mb, available_physical_memory_mb, total_server_memory_mb,
                 target_server_memory_mb, buffer_pool_mb, plan_cache_mb, system_memory_state, sql_memory_model)
                VALUES ($1, $2, $3, $4, 65536, 16384, 49152, 49152, 40000, 5000,
                        'Available physical memory is high', 'CONVENTIONAL')
                """, CollectionIdGenerator.Next(), sampledAt, id, name);
            await DarlingMcpTestData.ExecAsync(connection, ct, """
                INSERT INTO wait_stats
                (collection_id, collection_time, server_id, server_name, wait_type,
                 delta_wait_time_ms, delta_signal_wait_time_ms, delta_waiting_tasks)
                VALUES ($1, $2, $3, $4, $5, 1000, 100, 10)
                """, CollectionIdGenerator.Next(), sampledAt, id, name, waitType);
            await DarlingMcpTestData.ExecAsync(connection, ct, """
                INSERT INTO database_config
                (config_id, capture_time, server_id, server_name, database_name)
                VALUES ($1, $2, $3, $4, $5)
                """, CollectionIdGenerator.Next(), sampledAt, id, name, name + "_db");
            await DarlingMcpTestData.ExecAsync(connection, ct, """
                INSERT INTO memory_clerks
                (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
                VALUES ($1, $2, $3, $4, $5, 40000)
                """, CollectionIdGenerator.Next(), sampledAt, id, name, name + "_CLERK");
            await DarlingMcpTestData.ExecAsync(connection, ct, """
                INSERT INTO file_io_stats
                (collection_id, collection_time, server_id, server_name, database_name,
                 file_name, file_type, physical_name, size_mb, delta_reads, delta_writes,
                 delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms)
                VALUES ($1, $2, $3, $4, $5, $6, 'ROWS', $6, 1000, 50, 20, 409600, 102400, 250, 40)
                """, CollectionIdGenerator.Next(), sampledAt, id, name, name + "_db", name + ".mdf");
            foreach (var offset in new[] { -1, 0 })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, """
                    INSERT INTO query_stats
                    (collection_id, collection_time, server_id, server_name, database_name, query_hash,
                     query_plan_hash, sql_handle, plan_handle, query_text, delta_execution_count, delta_worker_time,
                     delta_elapsed_time, delta_logical_reads, delta_logical_writes, delta_physical_reads,
                     delta_rows, delta_spills, min_dop, max_dop, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time)
                    VALUES ($1, $2, $3, $4, $5, '0xABCDEF0123456789', '0xCAFEBABE00000001',
                            '0xSQLH', '0xPLANH', $6, 10, 10000, 20000, 5000, 0, 50, 1000, 0, 1, 4, 800, 1500, 1600, 3000)
                    """, CollectionIdGenerator.Next(), sampledAt.AddMinutes(offset), id, name, name + "_db",
                    "SELECT '" + name + "_QUERY'");
            }

            await DarlingMcpTestData.ExecAsync(connection, ct, """
                INSERT INTO blocked_process_reports
                (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
                 blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text,
                 blocked_process_report_xml, contentious_object)
                VALUES ($1, $2, $3, $4, $2, $5, 55, 60, 8000, 'X', $6, $6,
                        '<blocked-process-report><blocked-process><process spid="55"/></blocked-process></blocked-process-report>', 'dbo.ScopeTest')
                """, CollectionIdGenerator.Next(), sampledAt, id, name, name + "_db", "SELECT '" + name + "'");
            await DarlingMcpTestData.ExecAsync(connection, ct, """
                INSERT INTO deadlocks
                (deadlock_id, collection_time, server_id, server_name, database_name, deadlock_time, victim_process_id, victim_sql_text)
                VALUES ($1, $2, $3, $4, $5, $2, 'process1', $6)
                """, CollectionIdGenerator.Next(), sampledAt, id, name, name + "_db", "SELECT '" + name + "'");
            await DarlingMcpTestData.ExecAsync(connection, ct, """
                INSERT INTO config_alert_log
                (alert_time, server_id, server_name, metric_name, current_value, threshold_value, dismissed)
                VALUES ($1, $2, $3, 'High CPU', $4, 10, FALSE)
                """, sampledAt.AddSeconds(id), id, name, cpu);
            for (var dismissed = 0; dismissed < (id == 41 ? 1 : 2); dismissed++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, """
                    INSERT INTO config_alert_log
                    (alert_time, server_id, server_name, metric_name, current_value, threshold_value, dismissed)
                    VALUES ($1, $2, $3, 'High CPU', 99, 10, TRUE)
                    """, sampledAt.AddMinutes(-5).AddSeconds(dismissed), id, name);
            }

            var requestId = id == 41 ? financeRequestId : operationsRequestId;
            await DarlingMcpTestData.ExecAsync(connection, ct, """
                INSERT INTO config.investigation_requests
                (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at,
                 window_until, opened_at, expires_at, state, reported_at)
                VALUES ($1, 1, $2, $3, 'resources', 'High CPU', now(), now(),
                        now() + interval '30 minutes', now(), now() + interval '4 hours', 'reported', now())
                """, requestId, id, name);
            await DarlingMcpTestData.ExecAsync(connection, ct, """
                INSERT INTO config.investigation_reports
                (request_id, report_format, agent_id, claim_id, verdict, root_cause, evidence, server_id)
                VALUES ($1, 1, 'scope-test', $2, 'real', $3, '[{"name":"scope","value":"test"}]'::jsonb, $4)
                """, requestId, Guid.NewGuid(), name + " report", id);
        }

        var prod = await AddTag("PROD", null, 43);
        var finance = await AddTag("Finance", prod, 41);
        await AddTag("Operations", prod, 42);

        var admission = new ReadAdmission(TimeProvider.System, TimeSpan.Zero, null);
        using var server = await BuildServer(
            SetFor(ScopedEntry("finance", ReaderToken, finance), ScopedEntry("prod", ParentReaderToken, prod)),
            dataSource: postgres, readAdmission: admission);

        async Task AssertFleetAsync(string token, params int[] expectedIds)
        {
            var cookie = await SignIn(server, token);
            var (context, body) = await Send(server, HttpMethods.Get, "/api/fleet", cookie);
            Assert.Equal(StatusCodes.Status200OK, context.Response.StatusCode);
            using var result = JsonDocument.Parse(body);
            Assert.Equal(expectedIds.Length, result.RootElement.GetProperty("total_servers").GetInt32());
            var ids = result.RootElement.GetProperty("cards").EnumerateArray()
                .Select(card => card.GetProperty("server_id").GetInt32()).Order().ToArray();
            Assert.Equal(expectedIds, ids);
        }

        await AssertFleetAsync(SharedToken, 41, 42, 43, 44);
        await AssertFleetAsync(ReaderToken, 41);
        await AssertFleetAsync(ParentReaderToken, 41, 42, 43);

        var financeCookie = await SignIn(server, ReaderToken);
        // The partial matches both the permitted Finance server and hidden servers in the registry.
        foreach (var path in new[] { "/api/read/get_wait_stats", "/api/server-databases", "/api/server-data-start", "/api/catalog" })
        {
            var (response, body) = await Send(server, HttpMethods.Get, path + "?server=server", financeCookie);
            Assert.True(response.Response.StatusCode == StatusCodes.Status200OK,
                path + " answered " + response.Response.StatusCode + ": " + body);
            Assert.DoesNotContain("operations-server", body, StringComparison.Ordinal);
            if (path == "/api/server-databases")
            {
                Assert.Contains("finance-server_db", body, StringComparison.Ordinal);
            }
        }

        var (unscopedRoute, _) = await Send(server, HttpMethods.Get, "/api/ag", financeCookie);
        Assert.Equal(StatusCodes.Status403Forbidden, unscopedRoute.Response.StatusCode);

        foreach (var (name, expectedStatus) in new[]
        {
            ("finance-server", StatusCodes.Status200OK),
            ("operations-server", StatusCodes.Status403Forbidden),
        })
        {
            var (catalogResponse, catalogBody) = await Send(server, HttpMethods.Get,
                "/api/catalog?server=" + name, financeCookie);
            Assert.Equal(expectedStatus, catalogResponse.Response.StatusCode);
            Assert.DoesNotContain("operations-server", catalogBody, StringComparison.Ordinal);
        }


        var sharedCookie = await SignIn(server, SharedToken);
        var prodCookie = await SignIn(server, ParentReaderToken);
        foreach (var read in new[] { "get_cpu_utilization", "get_memory_stats", "get_wait_stats" })
        {
            var path = "/api/read/" + read;
            var (sharedResponse, sharedBody) = await Send(server, HttpMethods.Get,
                path + "?server=operations-server", sharedCookie);
            Assert.Equal(StatusCodes.Status200OK, sharedResponse.Response.StatusCode);
            Assert.Contains("operations-server", sharedBody, StringComparison.Ordinal);

            var (allowedResponse, allowedBody) = await Send(server, HttpMethods.Get,
                path + "?server=finance-server", financeCookie);
            Assert.Equal(StatusCodes.Status200OK, allowedResponse.Response.StatusCode);
            Assert.Contains("finance-server", allowedBody, StringComparison.Ordinal);
            Assert.DoesNotContain("operations-server", allowedBody, StringComparison.Ordinal);
            using var allowed = JsonDocument.Parse(allowedBody);
            if (read == "get_cpu_utilization")
            {
                Assert.Equal(11, allowed.RootElement.GetProperty("samples")[0].GetProperty("sql_server_cpu").GetInt32());
            }
            else if (read == "get_memory_stats")
            {
                Assert.Equal(65536, allowed.RootElement.GetProperty("total_physical_memory_mb").GetDecimal());
            }
            else
            {
                Assert.Contains("FINANCE_WAIT", allowedBody, StringComparison.Ordinal);
                Assert.DoesNotContain("OPERATIONS_WAIT", allowedBody, StringComparison.Ordinal);
            }

            foreach (var query in new[]
            {
                "", "?server=operations-server", "?server_name=operations-server",
                "?server_id=42", "?server=finance-server&server_id=42",
                "?server=finance-server&server_name=operations-server",
                "?server=finance-server&server=operations-server", "?server=unknown-server", "?server=operations",
            })
            {
                var (denied, deniedBody) = await Send(server, HttpMethods.Get, path + query, financeCookie);
                Assert.Equal(StatusCodes.Status403Forbidden, denied.Response.StatusCode);
                Assert.DoesNotContain("operations-server", deniedBody, StringComparison.Ordinal);
            }

            var (inherited, inheritedBody) = await Send(server, HttpMethods.Get,
                path + "?server_name=operations-server", prodCookie);
            Assert.Equal(StatusCodes.Status200OK, inherited.Response.StatusCode);
            Assert.Contains("operations-server", inheritedBody, StringComparison.Ordinal);
        }

        var detailPaths = new[]
        {
            "/api/read/get_cpu_utilization",
            "/api/read/get_memory_stats",
            "/api/read/get_wait_stats",
            "/api/read/get_server_summary",
            "/api/read/get_server_properties",
            "/api/read/get_wait_types",
            "/api/read/get_wait_trend",
            "/api/read/get_memory_clerks",
            "/api/read/get_memory_trend",
            "/api/read/get_file_io_stats",
            "/api/read/get_file_io_trend",
            "/api/read/get_tempdb_trend",
            "/api/read/get_session_stats",
            "/api/read/get_active_queries",
            "/api/read/get_waiting_tasks",
            "/api/read/get_blocking_stats",
            "/api/read/get_blocking_trend",
            "/api/read/get_deadlock_trend",
            "/api/read/get_deadlock_detail",
            "/api/read/get_blocked_process_xml",
            "/api/read/get_current_waits_trend",
            "/api/read/get_lock_wait_trend",
            "/api/read/get_collection_health",
            "/api/read/get_collection_log",
            "/api/server-databases",
            "/api/server-data-start",
        };
        foreach (var path in detailPaths)
        {
            var extra = path.EndsWith("/get_wait_trend", StringComparison.Ordinal)
                ? "&wait_type=FINANCE_WAIT"
                : "";
            var (allowed, body) = await Send(server, HttpMethods.Get,
                path + "?server=finance-server" + extra, financeCookie);
            Assert.True(allowed.Response.StatusCode == StatusCodes.Status200OK,
                path + " answered " + allowed.Response.StatusCode + ": " + body);
            Assert.DoesNotContain("operations-server", body, StringComparison.Ordinal);
            if (path.EndsWith("/get_file_io_stats", StringComparison.Ordinal))
            {
                Assert.Contains("finance-server.mdf", body, StringComparison.Ordinal);
            }
            else if (path.EndsWith("/get_memory_clerks", StringComparison.Ordinal))
            {
                Assert.Contains("finance-server_CLERK", body, StringComparison.Ordinal);
            }
            else if (path == "/api/server-databases")
            {
                Assert.Contains("finance-server_db", body, StringComparison.Ordinal);
            }

            foreach (var selector in new[] { "", "?server=operations-server", "?server_id=42",
                "?server=prod-only-server", "?server=untagged-server", "?server=*", "?server=(fleet)" })
            {
                var (denied, deniedBody) = await Send(server, HttpMethods.Get, path + selector, financeCookie);
                Assert.Equal(StatusCodes.Status403Forbidden, denied.Response.StatusCode);
                Assert.DoesNotContain("operations-server", deniedBody, StringComparison.Ordinal);
            }
        }

        var censusProblems = new List<string>();
        foreach (var path in DarlingWebEndpoints.ScopedServerReadPaths.Where(path =>
            path != "/api/read/get_alert_details" &&
            !(path.StartsWith("/api/read/", StringComparison.Ordinal) &&
              DarlingWebEndpoints.IsScopedFleetRead(path["/api/read/".Length..]))))
        {
            var extra = path.EndsWith("/get_wait_trend", StringComparison.Ordinal)
                ? "&wait_type=FINANCE_WAIT" : "";
            var (allowed, body) = await Send(server, HttpMethods.Get,
                path + "?server=finance-server" + extra, financeCookie);
            var expected = AnswersWithoutARow.TryGetValue(path, out var reason) ? StatusCodes.Status400BadRequest : StatusCodes.Status200OK;
            if (allowed.Response.StatusCode != expected)
            {
                censusProblems.Add(path + " answered " + allowed.Response.StatusCode + " (expected " + expected + (reason is null ? "" : ": " + reason) + "): " + body);
            }
            Assert.DoesNotContain("operations-server", body, StringComparison.Ordinal);
            var (denied, deniedBody) = await Send(server, HttpMethods.Get,
                path + "?server=operations-server", financeCookie);
            Assert.Equal(StatusCodes.Status403Forbidden, denied.Response.StatusCode);
            Assert.DoesNotContain("operations-server", deniedBody, StringComparison.Ordinal);
        }
        Assert.True(censusProblems.Count == 0, string.Join("\n", censusProblems));

        foreach (var read in new[] { "get_top_queries_by_cpu", "get_blocking", "get_deadlocks" })
        {
            var (response, body) = await Send(server, HttpMethods.Get,
                "/api/read/" + read + "?server=finance-server", financeCookie);
            Assert.Equal(StatusCodes.Status200OK, response.Response.StatusCode);
            Assert.Contains("finance-server", body, StringComparison.Ordinal);
            Assert.DoesNotContain("operations-server", body, StringComparison.Ordinal);
        }

        var (historyResponse, historyBody) = await Send(server, HttpMethods.Get,
            "/api/read/get_alert_history?limit=1", financeCookie);
        Assert.Equal(StatusCodes.Status200OK, historyResponse.Response.StatusCode);
        using (var history = JsonDocument.Parse(historyBody))
        {
            Assert.Equal(1, history.RootElement.GetProperty("alerts_returned").GetInt32());
            Assert.Equal(41, history.RootElement.GetProperty("alerts")[0].GetProperty("server_id").GetInt32());
            Assert.Equal(1, history.RootElement.GetProperty("dismissed_excluded_count").GetInt64());
            Assert.False(history.RootElement.GetProperty("truncated").GetBoolean());
        }
        Assert.DoesNotContain("operations-server", historyBody, StringComparison.Ordinal);

        var alertTime = Uri.EscapeDataString(DateTime.SpecifyKind(sampledAt.AddSeconds(41), DateTimeKind.Utc).ToString("o"));
        var alertPath = "/api/read/get_alert_details?server_id=41&metric_name=High%20CPU&alert_time=" + alertTime;
        var (ownAlert, _) = await Send(server, HttpMethods.Get, alertPath, financeCookie);
        Assert.Equal(StatusCodes.Status200OK, ownAlert.Response.StatusCode);
        var (otherAlert, _) = await Send(server, HttpMethods.Get, alertPath.Replace("server_id=41", "server_id=42", StringComparison.Ordinal), financeCookie);
        Assert.Equal(StatusCodes.Status403Forbidden, otherAlert.Response.StatusCode);

        foreach (var path in new[]
        {
            "/api/read/list_servers", "/api/read/get_fleet_overview?detail=cards",
            "/api/read/get_query_fleet_summary?query_hash=0xABCDEF0123456789",
            "/api/read/get_finops_inventory?view=server_inventory", "/api/read/list_query_tags", "/api/read/get_job_history",
            "/api/read/list_investigation_requests", "/api/read/get_investigation_reports"
        })
        {
            var (response, body) = await Send(server, HttpMethods.Get, path, financeCookie);
            Assert.True(response.Response.StatusCode == StatusCodes.Status200OK,
                path + " answered " + response.Response.StatusCode + ": " + body);
            Assert.DoesNotContain("operations-server", body, StringComparison.Ordinal);
            if (path == "/api/read/get_investigation_reports")
            {
                using var reports = JsonDocument.Parse(body);
                Assert.Equal(1, reports.RootElement.GetProperty("requests_returned").GetInt32());
                Assert.Equal(1, reports.RootElement.GetProperty("family_counts")[0].GetProperty("reported").GetInt32());
            }
        }

        var (outsideReport, outsideReportBody) = await Send(server, HttpMethods.Get,
            "/api/read/get_investigation_reports?request_id=" + operationsRequestId, financeCookie);
        Assert.DoesNotContain("operations-server", outsideReportBody, StringComparison.Ordinal);
        Assert.True(outsideReport.Response.StatusCode is StatusCodes.Status200OK or StatusCodes.Status404NotFound);
        Assert.Contains("not_found", outsideReportBody, StringComparison.Ordinal);

        var (ownChanges, _) = await Send(server, HttpMethods.Get,
            "/api/agent-changes?request=" + financeRequestId, financeCookie);
        Assert.Equal(StatusCodes.Status200OK, ownChanges.Response.StatusCode);
        var (otherChanges, _) = await Send(server, HttpMethods.Get,
            "/api/agent-changes?request=" + operationsRequestId, financeCookie);
        Assert.Equal(StatusCodes.Status403Forbidden, otherChanges.Response.StatusCode);

        const string panel = """{"source":"wait_stats","measure":"wait_time_ms","aggregate":"sum","timeBucket":"hour","topN":20,"groupBy":["server","wait_type"],"viz":"line","annotations":["deadlocks"]}""";
        var definition = """{"variables":[],"panels":[""" + panel + "]}";
        var viewResult = await new CustomViewStore(postgres, 30).CreateAsync(
            "Scoped dashboard", null, definition, "scope-test", cancellationToken: ct);
        var viewId = Assert.IsType<CustomViewResult.Ok>(viewResult).View!.Id;
        var (viewResponse, _) = await Send(server, HttpMethods.Get, "/api/views/" + viewId, financeCookie);
        Assert.Equal(StatusCodes.Status403Forbidden, viewResponse.Response.StatusCode);

        foreach (var restrictedPath in new[] { "/api/read/get_known_patterns?server=finance-server", "/api/read/get_alert_settings" })
        {
            var (restrictedResponse, _) = await Send(server, HttpMethods.Get, restrictedPath, financeCookie);
            Assert.Equal(StatusCodes.Status403Forbidden, restrictedResponse.Response.StatusCode);
        }

        foreach (var serverSelection in new[] { "", ""","server":["finance-server","operations-server"]""" })
        {
            var (composeResponse, composed) = await PostJson(server, "/api/compose/run",
                """{"hours":4,"panel":""" + panel + serverSelection + "}", financeCookie);
            Assert.Equal(StatusCodes.Status200OK, composeResponse.Response.StatusCode);
            Assert.Contains("FINANCE_WAIT", composed, StringComparison.Ordinal);
            Assert.DoesNotContain("OPERATIONS_WAIT", composed, StringComparison.Ordinal);
            Assert.DoesNotContain("operations-server", composed, StringComparison.Ordinal);
        }
        var (outsideCompose, outsideComposeBody) = await PostJson(server, "/api/compose/run",
            """{"hours":4,"server":"operations-server","panel":""" + panel + "}", financeCookie);
        Assert.Equal(StatusCodes.Status200OK, outsideCompose.Response.StatusCode);
        using (var empty = JsonDocument.Parse(outsideComposeBody))
        {
            Assert.Equal(0, empty.RootElement.GetProperty("rows").GetArrayLength());
        }

        await DarlingMcpTestData.ExecAsync(connection, ct,
            "UPDATE servers SET server_name = 'finance-server' WHERE server_id = 42");
        try
        {
            var (response, body) = await PostJson(server, "/api/compose/run",
                """{"hours":4,"panel":""" + panel + "}", financeCookie);
            Assert.Equal(StatusCodes.Status200OK, response.Response.StatusCode);
            Assert.Contains("FINANCE_WAIT", body, StringComparison.Ordinal);
            Assert.DoesNotContain("OPERATIONS_WAIT", body, StringComparison.Ordinal);
        }
        finally
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE servers SET server_name = 'operations-server' WHERE server_id = 42");
        }

        var sweepId = CollectionIdGenerator.Next();
        var sweptAt = DateTime.UtcNow;
        await FleetSweepStore.RecordSweepAsync(postgres,
            new FleetSweepRun(sweepId, sweptAt, sweptAt.AddHours(-1), sweptAt,
                null, false, 2, 2, true, """{"notes":["operations-server private note"]}""",
                """{"fleet":{"servers_expected":2},"private":"operations-server private report","watch":{"opened":[{"server_id":42,"condition":"operations-server"}]}}"""),
            new[]
            {
                new FleetSweepServerVerdict(41, "finance-server", "Healthy", null, null, """{"has_data":true}"""),
                new FleetSweepServerVerdict(42, "operations-server", "Critical", null, "operations-server", """{"has_data":true}""")
            },
            new[] { new FleetSweepWouldHavePagedEntry(42, "resources", """{"private":"operations-server"}""") },
            Array.Empty<FleetSweepWatchItem>(), ct);

        foreach (var path in new[] { "/api/sweeps", "/api/sweeps/latest", "/api/sweeps/" + sweepId,
            "/api/sweeps/watch-items", "/api/read/get_sweep_reports" })
        {
            var (response, body) = await Send(server, HttpMethods.Get, path, financeCookie);
            Assert.Equal(StatusCodes.Status200OK, response.Response.StatusCode);
            Assert.DoesNotContain("operations-server", body, StringComparison.Ordinal);
            if (path == "/api/sweeps/latest")
            {
                using var sweep = JsonDocument.Parse(body);
                Assert.Equal(1, sweep.RootElement.GetProperty("servers_expected").GetInt32());
                Assert.Equal(1, sweep.RootElement.GetProperty("servers_reported").GetInt32());
            }
        }

        // The held view: a scoped reader's one-server get_finops view=optimization fills no cache entry, and the same read by the
        // shared seat does; the key is the handler's own, so a filled entry is what the next unscoped reader would be served.
        var heldKey = DarlingWebNamedTokenTests.HeldFinOpsKey(admission, out _);
        const string heldPath = "/api/read/get_finops?server=finance-server&view=optimization";
        var (heldScoped, heldScopedBody) = await Send(server, HttpMethods.Get, heldPath, financeCookie);
        Assert.True(heldScoped.Response.StatusCode == StatusCodes.Status200OK, heldPath + " answered " + heldScoped.Response.StatusCode + ": " + heldScopedBody);
        Assert.DoesNotContain("operations-server", heldScopedBody, StringComparison.Ordinal);
        Assert.False(admission.Cache.TryGet(heldKey, out _));
        var (heldShared, heldSharedBody) = await Send(server, HttpMethods.Get, heldPath, sharedCookie);
        Assert.True(heldShared.Response.StatusCode == StatusCodes.Status200OK, heldPath + " answered " + heldShared.Response.StatusCode + ": " + heldSharedBody);
        Assert.True(admission.Cache.TryGet(heldKey, out _));
        var (heldScopedAgain, heldScopedAgainBody) = await Send(server, HttpMethods.Get, heldPath, financeCookie);
        Assert.Equal(StatusCodes.Status200OK, heldScopedAgain.Response.StatusCode);
        Assert.DoesNotContain("operations-server", heldScopedAgainBody, StringComparison.Ordinal);

        // An incident reader must retain the authorized ID when it uses the fingerprint resolver.
        var incident = await DarlingServerResolver.ReadWebServerAsync(41,
            () => DarlingMcpBlockingTools.GetDeadlockDetail(postgres, "operations-server", cancellationToken: ct));
        Assert.Contains("Server data is unavailable to this token.", incident, StringComparison.Ordinal);
        Assert.DoesNotContain("operations-server", incident, StringComparison.Ordinal);

        // Direct store reads carry no web grant.
        var viewerFleet = await DarlingFleetReader.GetFleetOverviewAsync(postgres,
            DateTime.UtcNow.AddHours(-1), DateTime.UtcNow, cancellationToken: ct);
        Assert.Equal(4, viewerFleet.TotalServers);

        await DarlingMcpTestData.ExecAsync(connection, ct,
            "DELETE FROM server_tag_map WHERE server_id = 41 AND tag_id = $1", finance);
        var (revoked, _) = await Send(server, HttpMethods.Get,
            "/api/read/get_cpu_utilization?server=finance-server", financeCookie);
        Assert.Equal(StatusCodes.Status403Forbidden, revoked.Response.StatusCode);
    }
}
