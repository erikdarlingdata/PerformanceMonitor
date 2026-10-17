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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// <c>get_investigation_evidence</c> (#6033) on a live store: a claimed agent reads the sections that froze after its claim,
/// masked as the brief masks them, and the refusals are the brief's.
/// </summary>
public sealed class InvestigationEvidenceReadLiveTests
{
    private const int ServerId = 7_603_301;
    private const string ServerName = "EvReadSrv";
    private const string Statement = "SELECT secret_col FROM dbo.Customers WHERE email = 'jane@example.com' AND id = 424242";

    private static readonly DateTime AlertAt = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-5));

    private static InvestigationAlert Alert() =>
        new(1, ServerId, ServerName, "resources", "High CPU", AlertAt, new[] { ServerId + ":High CPU" }, null, 20, 100,
            LeaseMinutes: 15, ExpiryMinutes: 240);

    private static string Content() =>
        new JsonObject
        {
            ["window_start"] = "2026-01-01T00:00:00Z",
            ["rows"] = new JsonArray(new JsonObject { ["query_hash"] = "0xAAAA", ["query_text"] = Statement }),
        }.ToJsonString();

    [Fact]
    public async Task ASectionThatFroze_AfterTheClaim_IsReturned_MaskedAsTheBriefMasksIt_AtEachLevel()
    {
        await RunAsync(async (store, ct) =>
        {
            var requestId = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            var claim = (await store.ClaimAsync(requestId, "agent-a", ct)).ClaimId!.Value;
            await store.RecordEvidenceAsync(requestId, ServerId, new[] { new InvestigationEvidenceWrite("top_statements", "ready", Content()) }, ct);

            var full = JsonNode.Parse(await DarlingMcpInvestigationTools.EvidenceAsync(
                store, McpDataDetail.Full, requestId.ToString(), null, claim.ToString(), ct))!;
            var fullSection = Assert.Single(full["sections"]!.AsArray())!;
            Assert.Equal("ok", full["status"]!.GetValue<string>());
            Assert.Equal("top_statements", fullSection["section"]!.GetValue<string>());
            Assert.Equal("ready", fullSection["status"]!.GetValue<string>());
            Assert.Contains("424242", fullSection.ToJsonString());

            foreach (var mode in new[] { McpDataDetail.MaskLiterals, McpDataDetail.MetadataOnly })
            {
                var answer = JsonNode.Parse(await DarlingMcpInvestigationTools.EvidenceAsync(
                    store, mode, requestId.ToString(), new[] { "top_statements" }, claim.ToString(), ct))!;
                var text = Assert.Single(answer["sections"]!.AsArray())!.ToJsonString();
                Assert.DoesNotContain("424242", text);
                Assert.DoesNotContain("jane@example.com", text);
                Assert.Contains("0xAAAA", text);
            }

            var filtered = JsonNode.Parse(await DarlingMcpInvestigationTools.EvidenceAsync(
                store, McpDataDetail.Full, requestId.ToString(), new[] { "waits" }, claim.ToString(), ct))!;
            Assert.Equal("empty", filtered["status"]!.GetValue<string>());
        });
    }

    [Fact]
    public async Task APendingSection_IsReturnedAsPending_WithNoContent()
    {
        await RunAsync(async (store, ct) =>
        {
            var requestId = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            var claim = (await store.ClaimAsync(requestId, "agent-a", ct)).ClaimId!.Value;
            await store.RecordEvidenceAsync(requestId, ServerId, new[]
            {
                new InvestigationEvidenceWrite("top_statements", "pending", null),
                new InvestigationEvidenceWrite("waits", "ready", Content()),
            }, ct);

            var answer = JsonNode.Parse(await DarlingMcpInvestigationTools.EvidenceAsync(
                store, McpDataDetail.Full, requestId.ToString(), null, claim.ToString(), ct))!;
            var pending = answer["sections"]!.AsArray().Single(s => s!["section"]!.GetValue<string>() == "top_statements")!;
            Assert.Equal("pending", pending["status"]!.GetValue<string>());
            Assert.Null(pending["content"]);
        });
    }

    [Fact]
    public async Task AnUnknownRequest_AndARequestAnotherCallerClaimed_AreRefused_WithTheBriefsRefusalShape()
    {
        await RunAsync(async (store, ct) =>
        {
            var requestId = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            var claim = (await store.ClaimAsync(requestId, "agent-a", ct)).ClaimId!.Value;
            await store.RecordEvidenceAsync(requestId, ServerId, new[] { new InvestigationEvidenceWrite("waits", "ready", Content()) }, ct);

            var unknown = JsonNode.Parse(await DarlingMcpInvestigationTools.EvidenceAsync(
                store, McpDataDetail.Full, Guid.NewGuid().ToString(), null, null, ct))!;
            Assert.Equal("not_found", unknown["status"]!.GetValue<string>());
            Assert.Null(unknown["sections"]);

            foreach (var other in new string?[] { null, Guid.NewGuid().ToString() })
            {
                var refused = JsonNode.Parse(await DarlingMcpInvestigationTools.EvidenceAsync(
                    store, McpDataDetail.Full, requestId.ToString(), null, other, ct))!;
                Assert.Equal("already_claimed", refused["status"]!.GetValue<string>());
                Assert.Null(refused["sections"]);
            }

            var holder = JsonNode.Parse(await DarlingMcpInvestigationTools.EvidenceAsync(
                store, McpDataDetail.Full, requestId.ToString(), null, claim.ToString(), ct))!;
            Assert.Equal("ok", holder["status"]!.GetValue<string>());
        });
    }

    [Fact]
    public async Task AnUnclaimedRequest_IsServedToAnyCaller()
    {
        await RunAsync(async (store, ct) =>
        {
            var requestId = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;
            await store.RecordEvidenceAsync(requestId, ServerId, new[] { new InvestigationEvidenceWrite("waits", "ready", Content()) }, ct);
            var answer = JsonNode.Parse(await DarlingMcpInvestigationTools.EvidenceAsync(
                store, McpDataDetail.Full, requestId.ToString(), null, null, ct))!;
            Assert.Equal("ok", answer["status"]!.GetValue<string>());
        });
    }

    private static async Task RunAsync(Func<InvestigationRequestStore, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the investigation evidence read live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(new InvestigationRequestStore(dataSource, 30), ct);
    }
}
