/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5999 on a live store: an investigation request, its brief and the long-poll's opened row carry the registry's display
/// name and instance name, read when served. A rename of the display name shows on the next read of a request opened before
/// it; the instance name is the registry's <c>server_name</c>; <c>server_name</c> keeps the name the request was opened with;
/// and a server the registry no longer holds still reads (its display name falls back to that opened name, no instance name).
/// </summary>
public sealed class InvestigationServerNamesLiveTests
{
    private const int ServerId = 7_599_001;
    private const string OpenedName = "Prod OLTP";
    private const string InstanceName = @"SQLPROD01\INST1";

    private static readonly DateTime Now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static InvestigationAlert Alert(int serverId = ServerId, string serverName = OpenedName) =>
        new(1, serverId, serverName, "blocking", "Blocking", Now.AddMinutes(-10), [serverId + ":Blocking"], null,
            20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);

    private static Task RegisterAsync(NpgsqlConnection connection, int serverId, string host, string? display, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, $4, $4)", serverId, host, display, DarlingMcpTestData.Naive(Now));

    private static async Task<JsonObject> PayloadAsync(InvestigationRequestStore store, Guid id, CancellationToken ct)
    {
        var read = await store.ReadRequestAsync(id, ct);
        Assert.NotNull(read);
        return JsonNode.Parse(System.Text.Json.JsonSerializer.Serialize(InvestigationToolCore.RequestPayload(read!.Value.Row), McpHelpers.JsonOptions))!.AsObject();
    }

    [Fact]
    public async Task ARenamedDisplayName_ShowsOnTheNextRead_OfARequestOpenedBeforeTheRename_ByEveryRead()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            await RegisterAsync(connection, ServerId, InstanceName, OpenedName, ct);
            var store = new InvestigationRequestStore(dataSource, 30, () => Now.AddMinutes(-10));
            var id = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;

            Assert.Equal(OpenedName, (string?)(await PayloadAsync(store, id, ct))["display_name"]);

            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET display_name = 'Renamed OLTP' WHERE server_id = $1", ServerId);

            var request = await PayloadAsync(store, id, ct);
            Assert.Equal("Renamed OLTP", (string?)request["display_name"]);

            var list = Assert.Single(await store.ListAsync(null, ServerId, null, 10, ct));
            Assert.Equal(("Renamed OLTP", InstanceName), (list.DisplayName, list.InstanceName));

            var reports = await store.ReadReportsAsync(null, ServerId, Now.AddDays(-1), 10, 10, ct);
            var report = Assert.Single(reports.Requests);
            Assert.Equal(("Renamed OLTP", InstanceName), (report.DisplayName, report.InstanceName));

            var opened = Assert.Single(await store.OpenedSinceAsync(Now.AddDays(-1), null, 10, ct));
            Assert.Equal(("Renamed OLTP", InstanceName), (opened.DisplayName, opened.InstanceName));

            /* The brief is composed from the registry when it is read, so a claim after the rename composes the new name. */
            var sources = await new PgInvestigationBriefSourceReader(dataSource, utcNow: () => Now).ReadAsync(id, ct);
            Assert.Equal(("Renamed OLTP", InstanceName), (sources!.Request.DisplayName, sources.Request.InstanceName));
            var brief = JsonNode.Parse(InvestigationBriefComposer.Compose(sources, DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, Now))!;
            Assert.Equal("Renamed OLTP", (string?)brief["request"]!["display_name"]);
            Assert.Equal(InstanceName, (string?)brief["request"]!["instance_name"]);
        });
    }

    [Fact]
    public async Task TheInstanceName_IsTheRegistrysServerName_AndFollowsItWhenTheRegistryChanges()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            await RegisterAsync(connection, ServerId, InstanceName, OpenedName, ct);
            var store = new InvestigationRequestStore(dataSource, 30, () => Now.AddMinutes(-10));
            var id = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;

            Assert.Equal(InstanceName, (string?)(await PayloadAsync(store, id, ct))["instance_name"]);

            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET server_name = 'SQLPROD02' WHERE server_id = $1", ServerId);
            Assert.Equal("SQLPROD02", (string?)(await PayloadAsync(store, id, ct))["instance_name"]);
        });
    }

    [Fact]
    public async Task ServerName_KeepsTheNameTheRequestWasOpenedWith_AndAServerWithoutADisplayNameOrARegistryRowStillReads()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            await RegisterAsync(connection, ServerId, InstanceName, OpenedName, ct);
            var store = new InvestigationRequestStore(dataSource, 30, () => Now.AddMinutes(-10));
            var id = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET display_name = 'Renamed OLTP' WHERE server_id = $1", ServerId);
            var renamed = await PayloadAsync(store, id, ct);
            Assert.Equal(OpenedName, (string?)renamed["server_name"]);
            Assert.Equal("Renamed OLTP", (string?)renamed["display_name"]);

            /* A registered server with no display name shows its instance name as the display name, as the fleet cards do. */
            await RegisterAsync(connection, ServerId + 1, "SQLPROD03", null, ct);
            var plainId = (await store.OpenOrJoinAsync(Alert(ServerId + 1, "SQLPROD03"), ct)).RequestId!.Value;
            var plain = await PayloadAsync(store, plainId, ct);
            Assert.Equal(("SQLPROD03", "SQLPROD03", "SQLPROD03"), ((string?)plain["server_name"], (string?)plain["display_name"], (string?)plain["instance_name"]));

            /* A request whose server is no longer in the registry reads with the name it was opened with and no instance name. */
            var goneId = (await store.OpenOrJoinAsync(Alert(ServerId + 2, "Gone Server"), ct)).RequestId!.Value;
            var gone = await PayloadAsync(store, goneId, ct);
            Assert.Equal(("Gone Server", "Gone Server"), ((string?)gone["server_name"], (string?)gone["display_name"]));
            Assert.False(gone.ContainsKey("instance_name"));
        });
    }

    private static async Task RunAsync(Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the investigation server-name live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(dataSource, connection, ct);
    }
}
