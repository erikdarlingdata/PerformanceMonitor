/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7735: named web dashboard tokens with a role (<c>web.network.tokens</c>), the twin of <c>mcp.tokens</c>. The live cases drive the
/// REAL <c>DarlingWebHostService.ConfigurePipeline</c> over <see cref="TestServer"/>, the way <see cref="DarlingWebHostGateLiveTests"/>
/// does: a reader token signs in and reads, is refused every write route by the one write gate, an operator token is not, the shared
/// token keeps its edit seat beside them, and a token removed from the set ends its sessions on the next request. The unit cases cover
/// the start check (names, roles, values, duplicates, the MCP token rule) and the constant-time match.
/// </summary>
// #1776 own-store: the scoped-token live test creates and drops its own scratch database.
public sealed class DarlingWebNamedTokenTests
{
    private const string ListenIp = "192.168.1.205";
    private const string AllowedCidr = "192.168.1.0/24";
    private const string SharedToken = "shared-token-value-0123456789";
    private const string ReaderToken = "reader-token-value-abcdefghijk-1";
    private const string OperatorToken = "operator-token-value-lmnopqrst-2";
    private static readonly IPAddress InCidrRemote = IPAddress.Parse("192.168.1.50");

    /// <summary>One signing key for every server a test builds, so a cookie minted by one server is valid on another and the
    /// revocation cases can present it there; production keeps a per-process random key.</summary>
    private static readonly byte[] SigningKey = Enumerable.Range(0, 32).Select(i => (byte)(i * 7 + 3)).ToArray();

    private static WebTokenEntry Entry(string? name, string? token, string? role) => new() { Name = name, Token = token, Role = role };

    private static WebNetworkConfig Network(params WebTokenEntry?[] entries) => new() { Tokens = entries.ToList()! };

    private static List<string> Check(WebNetworkConfig network, out List<DarlingWebTokenSet.Entry> usable, string shared = SharedToken, IReadOnlyList<string>? mcp = null) =>
        DarlingWebTokenSet.Check(network, shared, mcp, DarlingWebTokenSet.ResolveEntry, out usable);

    private static DarlingWebTokenSet SetFor(params WebTokenEntry[] entries) =>
        DarlingWebTokenSet.Resolve(Network(entries), SharedToken, null, null);

    private static DarlingWebTokenSet ReaderAndOperator() =>
        SetFor(Entry("readers", ReaderToken, "reader"), Entry("ops", OperatorToken, "operator"));

    /* ---------------------------------------------------------------- the start check ---------------------------------------------------------------- */

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("has space")]
    [InlineData("a/b")]
    public void Check_RefusesABlankOrInvalidName(string? name)
    {
        var problems = Check(Network(Entry(name, ReaderToken, "reader")), out var usable);

        Assert.Empty(usable);
        var problem = Assert.Single(problems);
        Assert.Contains("web.network.tokens[0] has no valid name", problem, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, problem, StringComparison.Ordinal);
    }

    [Fact]
    public void Check_RefusesANameOver64Characters()
    {
        var problems = Check(Network(Entry(new string('a', 65), ReaderToken, "reader")), out var usable);

        Assert.Empty(usable);
        Assert.Contains(problems, p => p.Contains("has no valid name", StringComparison.Ordinal));
    }

    /// <summary>A token's name becomes its <c>updated_by</c> stamp and its <c>person:</c> journal actor, so a name a built-in writer already
    /// stamps as itself (web, mcp, cli, viewer) or an mcp.tokens placeholder (invalid, unclaimed) is refused in any case, and the finding
    /// names the entry.</summary>
    [Theory]
    [InlineData("web")]
    [InlineData("MCP")]
    [InlineData("unclaimed")]
    [InlineData("Invalid")]
    [InlineData("cli")]
    [InlineData("Viewer")]
    public void Check_RefusesAReservedName_ABuiltInStampOrAnMcpPlaceholder(string name)
    {
        var problems = Check(Network(Entry(name, ReaderToken, "operator")), out var usable);

        Assert.Empty(usable);
        var problem = Assert.Single(problems);
        Assert.Contains("web.network.tokens[0] is named " + name + ", which is reserved", problem, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, problem, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("web-ops")]
    [InlineData("mcp.readers")]
    [InlineData("unclaimed2")]
    public void Check_ServesANameThatOnlyContainsAReservedWord(string name)
    {
        var problems = Check(Network(Entry(name, ReaderToken, "reader")), out var usable);

        Assert.Empty(problems);
        Assert.Equal(name, Assert.Single(usable).Name);
    }

    [Theory]
    [InlineData("reader", "reader", false)]
    [InlineData("Reader", "reader", false)]
    [InlineData("operator", "operator", true)]
    [InlineData(" OPERATOR ", "operator", true)]
    public void Check_ServesTheTwoSeats_UnderTheMcpWords(string role, string normalized, bool canEdit)
    {
        var problems = Check(Network(Entry("t", ReaderToken, role)), out var usable);

        Assert.Empty(problems);
        var entry = Assert.Single(usable);
        Assert.Equal(normalized, entry.Role);
        Assert.Equal(canEdit, entry.CanEdit);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("agent")]
    [InlineData("editor")]
    [InlineData("viewer")]
    [InlineData("admin")]
    public void Check_RefusesARoleThatIsNotAWebSeat(string? role)
    {
        var problems = Check(Network(Entry("t", ReaderToken, role)), out var usable);

        Assert.Empty(usable);
        var problem = Assert.Single(problems);
        Assert.Contains("web.network.tokens[0] (t) has no valid role", problem, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, problem, StringComparison.Ordinal);
    }

    [Fact]
    public void Check_RefusesAnEntryWithNoValue_AndOneUnderTheFloor()
    {
        var problems = Check(Network(Entry("blank", null, "reader"), Entry("short", "aaaaaaaaaaaaaaaaaaaa", "reader")), out var usable);

        Assert.Empty(usable);
        Assert.Equal(2, problems.Count);
        Assert.Contains("web.network.tokens[0] (blank)'s token could not be read", problems[0], StringComparison.Ordinal);
        Assert.Contains("web.network.tokens[1] (short)'s token", problems[1], StringComparison.Ordinal);
        Assert.DoesNotContain("aaaaaaaaaaaaaaaaaaaa", problems[1], StringComparison.Ordinal);
    }

    [Fact]
    public void Check_RefusesDuplicateNames_NeitherServed_AndKeepsTheOthers()
    {
        var problems = Check(Network(Entry("readers", ReaderToken, "reader"), Entry("readers", OperatorToken, "operator"), Entry("ops", "third-token-value-0123456789-3", "operator")), out var usable);

        Assert.Equal("ops", Assert.Single(usable).Name);
        var problem = Assert.Single(problems);
        Assert.Equal("Two web.network.tokens entries are named readers. Neither is served: each name must be unique.", problem);
    }

    [Fact]
    public void Check_RefusesDuplicateValues_NoneServed()
    {
        var problems = Check(Network(Entry("a", ReaderToken, "reader"), Entry("b", ReaderToken, "operator")), out var usable);

        Assert.Empty(usable);
        var problem = Assert.Single(problems);
        Assert.StartsWith("These web.network.tokens entries share a token value with another web token: a, b.", problem, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, problem, StringComparison.Ordinal);
    }

    [Fact]
    public void Check_RefusesAValueTheSharedTokenHas()
    {
        var problems = Check(Network(Entry("same", SharedToken, "reader")), out var usable);

        Assert.Empty(usable);
        Assert.Contains(problems, p => p.StartsWith("These web.network.tokens entries share a token value with another web token: same.", StringComparison.Ordinal));
        Assert.All(problems, p => Assert.DoesNotContain(SharedToken, p, StringComparison.Ordinal));
    }

    [Fact]
    public void Check_RefusesAValueAnMcpTokenHas()
    {
        var problems = Check(Network(Entry("agentish", ReaderToken, "reader"), Entry("ops", OperatorToken, "operator")), out var usable, mcp: new[] { "some-other-mcp-token-0123456789", ReaderToken });

        Assert.Equal("ops", Assert.Single(usable).Name);
        var problem = Assert.Single(problems);
        Assert.StartsWith("These web.network.tokens entries have the value of an MCP token: agentish.", problem, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, problem, StringComparison.Ordinal);
    }

    [Fact]
    public void Check_WithNoTokensBlock_FindsNothing()
    {
        Assert.Empty(Check(new WebNetworkConfig(), out var usable));
        Assert.Empty(usable);
        Assert.Empty(DarlingWebTokenSet.Check(null, SharedToken, null, DarlingWebTokenSet.ResolveEntry, out _));
    }

    [Fact]
    public void Match_ComparesEveryToken_AndNeverStopsEarly()
    {
        var compares = 0;
        var set = new DarlingWebTokenSet(ReaderAndOperator().Named)
        {
            Compare = (presented, expected) => { compares++; return string.Equals(presented, expected, StringComparison.Ordinal); },
        };

        Assert.Equal("readers", set.Match(ReaderToken)?.Name);
        Assert.Equal(2, compares);
        compares = 0;
        Assert.Null(set.Match("nothing-like-it"));
        Assert.Equal(2, compares);
        Assert.Null(set.Match(""));
        Assert.Null(set.Find("nobody"));
        Assert.True(set.Find("ops")!.CanEdit);
    }

    [Fact]
    public void Resolve_LogsEachProblemAtCritical_AndNeverAValue()
    {
        var logger = new CapturingTestLogger();
        var set = DarlingWebTokenSet.Resolve(Network(Entry("readers", ReaderToken, "reader"), Entry("bad", OperatorToken, "agent")), SharedToken, null, logger);

        Assert.Equal("readers", Assert.Single(set.Named).Name);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Critical));
        Assert.Contains("(bad) has no valid role", logger.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain(OperatorToken, logger.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public void UnknownKeys_NamesAMistypedKeyInsideATokenEntry()
    {
        const string json = """
            { "web": { "enabled": true, "network": { "encryptedToken": "x", "tokens": [ { "name": "readers", "token": "t", "rol": "reader" } ] } } }
            """;

        Assert.Contains("web.network.tokens[0].rol", DarlingUnknownKeys.Find(json));
    }

    [Fact]
    public async Task ValidateConfig_ReportsTheNamedWebTokenFindings_WithoutAValue()
    {
        var config = new DarlingConfig
        {
            Web = new WebConfig { Network = new WebNetworkConfig { Token = SharedToken, Tokens = new List<WebTokenEntry> { Entry("a", ReaderToken, "reader"), Entry("a", OperatorToken, "operator") } } },
        };
        var output = new StringWriter();

        await DarlingCliCommands.ReportAgentDispatchAsync(config, output, CancellationToken.None);

        var text = output.ToString();
        Assert.Contains("WARNING: Two web.network.tokens entries are named a.", text, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, text, StringComparison.Ordinal);
        Assert.DoesNotContain(OperatorToken, text, StringComparison.Ordinal);
    }

    /* ---------------------------------------------------------------- the live pipeline ---------------------------------------------------------------- */

    /// <summary>The real pipeline, as <see cref="DarlingWebHostGateLiveTests"/> builds it, in network mode with the shared token and
    /// <paramref name="webTokens"/>. The signing key is fixed so a second server reads the first one's cookie.</summary>
    private static async Task<TestServer> BuildServer(DarlingWebTokenSet? webTokens, CapturingTestLogger? hostLogger = null, NpgsqlDataSource? dataSource = null, ReadAdmission? readAdmission = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var postgres = dataSource ?? NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);
        var app = builder.Build();

        var host = new DarlingWebHostService(
            hostLogger is null ? Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingWebHostService>.Instance : new HostLogger(hostLogger),
            new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache(), readAdmission: readAdmission);

        host.ConfigurePipeline(
            app, postgres, networkMode: true,
            networkListenIp: IPAddress.Parse(ListenIp), allowedCidr: IPNetwork.Parse(AllowedCidr),
            accessToken: SharedToken, oidcClient: null,
            sessionSigningKeyForTests: SigningKey,
            webTokens: webTokens);

        await app.StartAsync();
        return app.GetTestServer();
    }

    [Fact]
    public async Task BusyScopedRead_DoesNotQueryTheStore()
    {
        const string route = "get_wait_stats";
        var ct = TestContext.Current.CancellationToken;
        var admission = new ReadAdmission(TimeProvider.System, TimeSpan.Zero, null, maxHeavyReads: 1);
        using var held = await Task.Run(() => admission.AdmitAsync(ReadSurface.Web, route, ReadWindowBand.SevenDays, ct), ct);
        Assert.True(held.MayRun);

        var entry = new WebTokenEntry { Name = "finance", Token = ReaderToken, Role = "reader", AllowedTagIds = [41] };
        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        using var server = await BuildServer(SetFor(entry), dataSource: postgres, readAdmission: admission);
        var cookie = await SignIn(server, ReaderToken);
        // Any attempt to resolve scope from the store now fails before the busy response can be returned.
        await postgres.DisposeAsync();

        var (response, _) = await Send(server, HttpMethods.Get, "/api/read/get_wait_stats?server=finance-server&hours=168", cookie);
        Assert.Equal(StatusCodes.Status429TooManyRequests, response.Response.StatusCode);
    }

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

        // Deserialize the proposed public setting so this test can run against the pre-feature baseline.
        WebTokenEntry ScopedEntry(string name, string token, int tagId) =>
            JsonSerializer.Deserialize<WebTokenEntry>(
                JsonSerializer.Serialize(new { name, token, role = "reader", allowedTagIds = new[] { tagId } }))!;

        using var server = await BuildServer(
            SetFor(ScopedEntry("finance", ReaderToken, finance), ScopedEntry("prod", OperatorToken, prod)),
            dataSource: postgres);

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
        await AssertFleetAsync(OperatorToken, 41, 42, 43);

        var financeCookie = await SignIn(server, ReaderToken);
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
        var prodCookie = await SignIn(server, OperatorToken);
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

        foreach (var path in DarlingWebEndpoints.ScopedServerReadPaths.Where(path =>
            path != "/api/read/get_alert_details" &&
            !(path.StartsWith("/api/read/", StringComparison.Ordinal) &&
              DarlingWebEndpoints.IsScopedFleetRead(path["/api/read/".Length..]))))
        {
            var extra = path.EndsWith("/get_wait_trend", StringComparison.Ordinal)
                ? "&wait_type=FINANCE_WAIT" : "";
            var (allowed, body) = await Send(server, HttpMethods.Get,
                path + "?server=finance-server" + extra, financeCookie);
            Assert.True(allowed.Response.StatusCode is StatusCodes.Status200OK or StatusCodes.Status400BadRequest or StatusCodes.Status404NotFound,
                path + " answered " + allowed.Response.StatusCode + ": " + body);
            Assert.DoesNotContain("operations-server", body, StringComparison.Ordinal);
            var (denied, deniedBody) = await Send(server, HttpMethods.Get,
                path + "?server=operations-server", financeCookie);
            Assert.Equal(StatusCodes.Status403Forbidden, denied.Response.StatusCode);
            Assert.DoesNotContain("operations-server", deniedBody, StringComparison.Ordinal);
        }

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
            "/api/read/get_finops_inventory", "/api/read/list_query_tags", "/api/read/get_job_history",
            "/api/read/list_investigation_requests", "/api/read/get_investigation_reports"
        })
        {
            var (response, body) = await Send(server, HttpMethods.Get, path, financeCookie);
            Assert.True(response.Response.StatusCode is StatusCodes.Status200OK or StatusCodes.Status400BadRequest,
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

    private sealed class HostLogger(CapturingTestLogger inner) : ILogger<DarlingWebHostService>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => inner.IsEnabled(logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => inner.Log(logLevel, eventId, state, exception, formatter);
    }

    /// <summary>Signs in through the form POST and returns the session cookie (<c>name=value</c>) the 302 set.</summary>
    private static async Task<string> SignIn(TestServer server, string token)
    {
        var ctx = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, token);
        Assert.Equal(StatusCodes.Status302Found, ctx.Response.StatusCode);
        var setCookie = Assert.Single(ctx.Response.Headers.SetCookie) ?? string.Empty;
        return setCookie[..setCookie.IndexOf(';')];
    }

    private static async Task<(HttpContext Context, string Body)> PostJson(TestServer server, string path, string body, string cookie)
    {
        using var content = new MemoryStream(Encoding.UTF8.GetBytes(body));
        var context = await server.SendAsync(request =>
        {
            request.Request.Method = HttpMethods.Post;
            request.Request.Path = path;
            request.Request.ContentType = "application/json";
            request.Request.ContentLength = content.Length;
            request.Request.Body = content;
            request.Request.Headers.Host = ListenIp;
            request.Request.Headers.Cookie = cookie;
            request.Connection.RemoteIpAddress = InCidrRemote;
        });
        return (context, await new StreamReader(context.Response.Body).ReadToEndAsync());
    }

    private static async Task<(HttpContext Context, string Body)> Send(TestServer server, string method, string path, string? cookie)
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = method;
            var queryStart = path.IndexOf('?');
            c.Request.Path = queryStart < 0 ? path : path[..queryStart];
            c.Request.QueryString = queryStart < 0 ? QueryString.Empty : new QueryString(path[queryStart..]);
            c.Request.Headers.Host = ListenIp;
            c.Connection.RemoteIpAddress = InCidrRemote;
            if (cookie is not null) c.Request.Headers["Cookie"] = cookie;
        });
        return (ctx, await new StreamReader(ctx.Response.Body).ReadToEndAsync());
    }

    [Fact]
    public async Task AReaderToken_SignsIn_AndReadsTheSession_AsAReadOnlySeat()
    {
        var hostLogger = new CapturingTestLogger();
        using var server = await BuildServer(ReaderAndOperator(), hostLogger);
        var cookie = await SignIn(server, ReaderToken);

        var (ctx, body) = await Send(server, "GET", "/api/session", cookie);

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        Assert.Contains("\"can_edit\":false", body, StringComparison.Ordinal);
        Assert.Contains("\"can_own\":false", body, StringComparison.Ordinal);
        Assert.Contains("\"signed_in_admin\":false", body, StringComparison.Ordinal);
        // The audit line names the token and its role, never the value.
        Assert.Contains("Web dashboard sign-in: token readers as reader (read-only)", hostLogger.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain(ReaderToken, hostLogger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AReaderToken_PassesTheGate_ForAPageRead()
    {
        using var server = await BuildServer(ReaderAndOperator());
        var cookie = await SignIn(server, ReaderToken);

        // A path nothing maps answers 404 once the gate lets the request through; the sign-in form would be 200 text/html.
        var (ctx, _) = await Send(server, "GET", "/some/unmapped/page", cookie);

        Assert.Equal(StatusCodes.Status404NotFound, ctx.Response.StatusCode);
    }

    /// <summary>Every write route the web host maps, by method and path. The gate refuses them by METHOD, before routing, so this list
    /// is the proof that the enumerated surface is covered, not the mechanism: a mapped route that is not here is still refused.</summary>
    public static TheoryData<string, string> WriteRoutes => new()
    {
        // Admin: servers, people, agent changes
        { "POST", "/api/servers" }, { "PATCH", "/api/servers/3" }, { "PUT", "/api/servers/3/actions/kill_session" },
        { "PUT", "/api/people/add" }, { "PUT", "/api/agent-changes/switch" }, { "PUT", "/api/agent-changes/7/undo" },
        // Alerting: rules, settings, routes, mute rules, dismiss
        { "POST", "/api/alerts" }, { "PUT", "/api/alerts/3" }, { "DELETE", "/api/alerts/3" }, { "POST", "/api/alerts/validate" }, { "POST", "/api/alerts/test" },
        { "PUT", "/api/alert-settings/daily-report" }, { "PUT", "/api/alert-settings/database-protection" },
        { "PUT", "/api/threshold-overrides" }, { "PUT", "/api/environment-profiles" },
        { "POST", "/api/notification-routes" }, { "PUT", "/api/notification-routes/2" }, { "DELETE", "/api/notification-routes/2" },
        { "POST", "/api/mute-rules" }, { "PATCH", "/api/mute-rules/abc" }, { "PUT", "/api/mute-rules/abc/enabled" }, { "DELETE", "/api/mute-rules/abc" },
        { "POST", "/api/alert-history/dismiss" },
        // Composer saves and known patterns
        { "POST", "/api/views" }, { "PUT", "/api/views/3" }, { "DELETE", "/api/views/3" },
        { "POST", "/api/known-patterns" }, { "PUT", "/api/known-patterns/3" }, { "DELETE", "/api/known-patterns/3" },
        // Tags
        { "POST", "/api/server-tags" }, { "PATCH", "/api/server-tags/3" }, { "DELETE", "/api/server-tags/3" }, { "POST", "/api/server-tags/3/servers" }, { "DELETE", "/api/server-tags/3/servers" },
        { "POST", "/api/query-tags" }, { "PATCH", "/api/query-tags/3" }, { "DELETE", "/api/query-tags/3" }, { "POST", "/api/query-tags/3/assignments" }, { "DELETE", "/api/query-tags/3/assignments" },
        { "POST", "/api/query-tag-rules" }, { "PATCH", "/api/query-tag-rules/3" }, { "DELETE", "/api/query-tag-rules/3" },
        // Actions on a monitored server
        { "POST", "/api/collectors/run" }, { "POST", "/api/analyze-now" }, { "POST", "/api/active-queries/live" },
        // Investigations
        { "PUT", "/api/investigation-notes" }, { "PUT", "/api/investigation-notes/3/approve" }, { "PUT", "/api/investigation-notes/3/reject" },
        { "PUT", "/api/investigation-requests/r1/close" }, { "PUT", "/api/investigation-practice-requests/r1" }, { "PUT", "/api/investigation-feedback/r1" },
        // The proposal presses: a token seat has no owner key, so the #7109 exception does not open for it
        { "POST", "/api/proposals/5f3a6d2e-9b1c-4e7f-8a2d-1c3b5e7f9a0b/first-press" }, { "POST", "/api/proposals/5f3a6d2e-9b1c-4e7f-8a2d-1c3b5e7f9a0b/confirm" },
        // A write route that does not exist yet is born gated
        { "POST", "/api/anything/new" }, { "PATCH", "/api/compose/run" },
    };

    [Theory]
    [MemberData(nameof(WriteRoutes))]
    public async Task AReaderToken_IsRefusedEveryWriteRoute_ByTheOneGate(string method, string path)
    {
        using var server = await BuildServer(ReaderAndOperator());
        var cookie = await SignIn(server, ReaderToken);

        var (ctx, body) = await Send(server, method, path, cookie);

        Assert.Equal(StatusCodes.Status403Forbidden, ctx.Response.StatusCode);
        Assert.Contains(DarlingWebEndpoints.ReadOnlyAccountText, body, StringComparison.Ordinal);
        // #7955: the page tells the kinds of refusal apart on this code, so the gate itself must carry it.
        Assert.Equal(DarlingWebStoreWrites.RefusalReason.ReadOnlySeat,
            System.Text.Json.JsonDocument.Parse(body).RootElement.GetProperty("reason").GetString());
    }

    [Fact]
    public async Task AReaderToken_StillRunsComposedPanels_TheOneUnsafeMethodRead()
    {
        using var server = await BuildServer(ReaderAndOperator());
        var cookie = await SignIn(server, ReaderToken);

        var (ctx, _) = await Send(server, "POST", "/api/compose/run", cookie);

        // Past the gate: whatever the handler answers an empty body, it is not the gate's 403.
        Assert.NotEqual(StatusCodes.Status403Forbidden, ctx.Response.StatusCode);
    }

    [Fact]
    public async Task AnOperatorToken_SignsInToTheEditSeat_AndIsNotRefusedAWrite()
    {
        var hostLogger = new CapturingTestLogger();
        using var server = await BuildServer(ReaderAndOperator(), hostLogger);
        var cookie = await SignIn(server, OperatorToken);

        var (session, body) = await Send(server, "GET", "/api/session", cookie);
        Assert.Equal(StatusCodes.Status200OK, session.Response.StatusCode);
        Assert.Contains("\"can_edit\":true", body, StringComparison.Ordinal);
        Assert.Contains("\"signed_in_admin\":true", body, StringComparison.Ordinal);
        Assert.Contains("\"can_own\":false", body, StringComparison.Ordinal);

        // A write to a path nothing maps: the gate lets it through (404 from routing), where the reader seat gets 403.
        var (write, _) = await Send(server, "POST", "/api/anything/new", cookie);
        Assert.Equal(StatusCodes.Status404NotFound, write.Response.StatusCode);
        Assert.Contains("Web dashboard sign-in: token ops as operator (edit)", hostLogger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheSharedToken_KeepsItsEditSeat_BesideNamedTokens()
    {
        using var server = await BuildServer(ReaderAndOperator());
        var cookie = await SignIn(server, SharedToken);

        var (session, body) = await Send(server, "GET", "/api/session", cookie);
        Assert.Contains("\"can_edit\":true", body, StringComparison.Ordinal);
        var (write, _) = await Send(server, "POST", "/api/anything/new", cookie);
        Assert.Equal(StatusCodes.Status404NotFound, write.Response.StatusCode);
    }

    [Fact]
    public async Task AWrongToken_GetsTheSignInFormAgain_AndNoCookie()
    {
        using var server = await BuildServer(ReaderAndOperator());

        var ctx = await DarlingWebHostGateLiveTests.PostSignIn(server, ListenIp, InCidrRemote, "not-any-of-them-0123456789");

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        Assert.Equal("text/html; charset=utf-8", ctx.Response.ContentType);
        Assert.Equal(0, ctx.Response.Headers.SetCookie.Count);
    }

    [Fact]
    public async Task ARemovedToken_EndsItsSession_OnTheNextRequest()
    {
        using var before = await BuildServer(ReaderAndOperator());
        var cookie = await SignIn(before, ReaderToken);
        var (still, _) = await Send(before, "GET", "/api/session", cookie);
        Assert.Equal(StatusCodes.Status200OK, still.Response.StatusCode);

        // The same signing key, the readers entry gone: the cookie is cryptographically valid and names a token that no longer exists.
        using var after = await BuildServer(SetFor(Entry("ops", OperatorToken, "operator")));

        var (api, apiBody) = await Send(after, "GET", "/api/session", cookie);
        Assert.Equal(StatusCodes.Status401Unauthorized, api.Response.StatusCode);
        Assert.Contains("session expired", apiBody, StringComparison.Ordinal);

        var (page, _) = await Send(after, "GET", "/", cookie);
        Assert.Equal(StatusCodes.Status200OK, page.Response.StatusCode);
        Assert.Equal("text/html; charset=utf-8", page.Response.ContentType);
    }

    [Fact]
    public async Task ATokenCookie_IsRefusedWhereNoNamedTokenIsServed()
    {
        using var before = await BuildServer(ReaderAndOperator());
        var cookie = await SignIn(before, OperatorToken);

        using var after = await BuildServer(webTokens: null);

        var (api, _) = await Send(after, "GET", "/api/session", cookie);
        Assert.Equal(StatusCodes.Status401Unauthorized, api.Response.StatusCode);
        var (write, _) = await Send(after, "POST", "/api/anything/new", cookie);
        Assert.Equal(StatusCodes.Status401Unauthorized, write.Response.StatusCode);
    }

    [Fact]
    public async Task ARoleChange_AppliesToAnExistingSession_BecauseTheCookieCarriesOnlyTheName()
    {
        using var before = await BuildServer(SetFor(Entry("team", ReaderToken, "reader")));
        var cookie = await SignIn(before, ReaderToken);
        var (_, readerBody) = await Send(before, "GET", "/api/session", cookie);
        Assert.Contains("\"can_edit\":false", readerBody, StringComparison.Ordinal);

        using var after = await BuildServer(SetFor(Entry("team", ReaderToken, "operator")));
        var (_, operatorBody) = await Send(after, "GET", "/api/session", cookie);
        Assert.Contains("\"can_edit\":true", operatorBody, StringComparison.Ordinal);

        using var demoted = await BuildServer(SetFor(Entry("team", ReaderToken, "reader")));
        var (write, _) = await Send(demoted, "POST", "/api/views", cookie);
        Assert.Equal(StatusCodes.Status403Forbidden, write.Response.StatusCode);
    }

    [Fact]
    public void TheSeatModel_DecodesATokenCookie_ReadOnlyWithItsName_AndRefusesAnEmptyName()
    {
        Assert.True(DarlingWebSeat.TryResolveSeat(DarlingWebSeat.EncodeTokenCookieSubject("readers"), out var seat));
        Assert.Equal("readers", seat.Subject);
        Assert.Equal("readers", seat.TokenName);
        Assert.False(seat.CanEdit);
        Assert.Null(seat.OwnerKey);

        Assert.False(DarlingWebSeat.TryResolveSeat("t1:", out _));
        // A token seat never presses a proposal: the exception is for a seat with an owner key.
        Assert.False(DarlingWebSeat.IsRequestAllowed(seat, "POST", "/api/proposals/5f3a6d2e-9b1c-4e7f-8a2d-1c3b5e7f9a0b/confirm"));
    }
}
