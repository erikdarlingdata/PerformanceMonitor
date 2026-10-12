/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5241: an alert's advice and fix script reach the web Alert History page. The stored context holds them
/// (<c>AlertDetailItem.Body</c>, a code block for the fix script) but <c>detail_text</c> is the flattened
/// headings and fields only, so the page had nothing to show. The web-only <c>get_alert_details</c> read returns one
/// alert's items by the page's row identity; it is not an MCP tool, and <c>get_alert_history</c> is untouched. The
/// typed Apply payload (<c>Remediation</c>) is never projected: the web is advise-only.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertHistoryAdviceLiveTests
{
    private const int ServerId = -524101;
    private const string ServerLabel = "advice-524101";
    private const string AdviceMetric = "Analysis: plan_regression [524101aa]";
    private const string EngineMetric = "High CPU";
    private const string FixScript = "EXEC sys.sp_query_store_force_plan @query_id = 11, @plan_id = 22;";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the alert advice live tests.");
        return connectionString!;
    }

    private static NpgsqlDataSource OpenDataSource(string connectionString) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString);

    /// <summary>An analysis alert's context as the producer builds it: Diagnosis (fields), Advice (prose), the
    /// fix script (a code block that also carries the typed Apply payload).</summary>
    private static string AdviceContextJson()
    {
        var context = new AlertContext();
        var diagnosis = new AlertDetailItem { Heading = "Diagnosis" };
        diagnosis.Fields.Add(("Story", "PLAN_REGRESSION"));
        diagnosis.Fields.Add(("Severity", "1.20"));
        context.Details.Add(diagnosis);
        context.Details.Add(new AlertDetailItem
        {
            Heading = "Plan regression",
            Body = "Investigation: the plan regressed\n\nRemediation: force the better plan",
        });
        context.Details.Add(new AlertDetailItem
        {
            Heading = "Remediation T-SQL",
            Body = FixScript,
            IsCodeBlock = true,
            Remediation = new RemediationAction("PLAN_REGRESSION", "force",
                new[] { new ForcePlanTarget("SalesDb", 11, 22) }),
        });
        return AlertContextSerializer.Serialize(context);
    }

    /// <summary>An engine alert's context: headings and fields only, so its <c>detail_text</c> already says all of it.</summary>
    private static string EngineContextJson()
    {
        var context = new AlertContext();
        var item = new AlertDetailItem { Heading = "Blocking chain" };
        item.Fields.Add(("Lead blocker", "52"));
        context.Details.Add(item);
        return AlertContextSerializer.Serialize(context);
    }

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        await DeleteAsync(connection, ct);
        await InsertAlertAsync(connection, ct, now.AddMinutes(-2), AdviceMetric, "Diagnosis\n  Story: PLAN_REGRESSION\n  Severity: 1.20", AdviceContextJson());
        await InsertAlertAsync(connection, ct, now.AddMinutes(-1), EngineMetric, "Blocking chain\n  Lead blocker: 52", EngineContextJson());
    }

    private static Task InsertAlertAsync(NpgsqlConnection connection, CancellationToken ct, DateTime alertTime, string metric, string detailText, string contextJson) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 1, 1, FALSE, 'none', NULL, FALSE, FALSE, $5, $6)",
            DarlingMcpTestData.Naive(alertTime), ServerId, ServerLabel, metric, detailText, contextJson);

    private static Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_alert_log WHERE server_id = $1", ServerId);

    private static async Task<string> WebReadAsync(NpgsqlDataSource postgres, string read, string query)
    {
        var context = new DefaultHttpContext();
        context.Request.QueryString = new QueryString(query);
        return await DarlingWebEndpoints.BuildReadDispatch()[read](context, postgres, null!);
    }

    private static string DetailsQuery(string metric, DateTime alertTime, int serverId = ServerId) =>
        "?server_id=" + serverId + "&metric_name=" + Uri.EscapeDataString(metric)
        + "&alert_time=" + Uri.EscapeDataString(alertTime.ToString("o"));

    [Fact]
    public async Task GetAlertDetails_ReturnsTheAdviceAndTheFixScriptForTheKeyedRow_AndNeverTheApplyPayload()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            await SeedAsync(connection, now, ct);
            await using var postgres = OpenDataSource(cs);

            var json = await WebReadAsync(postgres, "get_alert_details", DetailsQuery(AdviceMetric, now.AddMinutes(-2)));
            using var doc = JsonDocument.Parse(json);
            Assert.Equal(new[] { "details" }, doc.RootElement.EnumerateObject().Select(p => p.Name).ToArray());
            var details = doc.RootElement.GetProperty("details").EnumerateArray().ToList();
            Assert.Equal(new[] { "Diagnosis", "Plan regression", "Remediation T-SQL" },
                details.Select(d => d.GetProperty("heading").GetString()).ToArray());

            var diagnosis = details[0];
            Assert.Equal("Story", diagnosis.GetProperty("fields")[0].GetProperty("label").GetString());
            Assert.Equal("PLAN_REGRESSION", diagnosis.GetProperty("fields")[0].GetProperty("value").GetString());
            Assert.Equal(JsonValueKind.Null, diagnosis.GetProperty("body").ValueKind);
            Assert.False(diagnosis.GetProperty("is_code_block").GetBoolean());

            Assert.Contains("force the better plan", details[1].GetProperty("body").GetString(), StringComparison.Ordinal);
            Assert.False(details[1].GetProperty("is_code_block").GetBoolean());

            Assert.Equal(FixScript, details[2].GetProperty("body").GetString());
            Assert.True(details[2].GetProperty("is_code_block").GetBoolean());

            /* The Apply payload is advise-only on the web: not under any name, anywhere in the answer. */
            Assert.DoesNotContain("emediation\"", json, StringComparison.Ordinal);
            Assert.DoesNotContain("PLAN_REGRESSION\",\"Action", json, StringComparison.Ordinal);
            Assert.DoesNotContain("SalesDb", json, StringComparison.Ordinal);
            Assert.DoesNotContain("FactKey", json, StringComparison.Ordinal);
            Assert.All(details, d => Assert.Equal(
                new[] { "heading", "fields", "body", "is_code_block" },
                d.EnumerateObject().Select(p => p.Name).ToArray()));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }

    [Fact]
    public async Task GetAlertDetails_AnswersEmptyDetails_ForAnEngineAlert_AnUnknownKey_AndAMalformedKey()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            await SeedAsync(connection, now, ct);
            await using var postgres = OpenDataSource(cs);

            const string empty = "{\"details\":[]}";
            /* An engine alert's context holds no prose: its detail_text already says everything, so it gets none. */
            Assert.Equal(empty, await WebReadAsync(postgres, "get_alert_details", DetailsQuery(EngineMetric, now.AddMinutes(-1))));
            /* Unknown metric, unknown time, unknown server: no match is an empty answer, not an error. */
            Assert.Equal(empty, await WebReadAsync(postgres, "get_alert_details", DetailsQuery("Analysis: nothing [00000000]", now.AddMinutes(-2))));
            Assert.Equal(empty, await WebReadAsync(postgres, "get_alert_details", DetailsQuery(AdviceMetric, now.AddMinutes(-3))));
            Assert.Equal(empty, await WebReadAsync(postgres, "get_alert_details", DetailsQuery(AdviceMetric, now.AddMinutes(-2), ServerId - 1)));
            /* The stamp exactly as get_alert_history spells it (the store's naive UTC, seven fraction digits, no zone). */
            var naive = DateTime.SpecifyKind(now.AddMinutes(-2), DateTimeKind.Unspecified);
            Assert.Contains("force the better plan", await WebReadAsync(postgres, "get_alert_details", DetailsQuery(AdviceMetric, naive)), StringComparison.Ordinal);
            /* A key that does not parse is the same empty answer. */
            Assert.Equal(empty, await WebReadAsync(postgres, "get_alert_details", "?server_id=" + ServerId + "&metric_name=x&alert_time=not-a-time"));
            Assert.Equal(empty, await WebReadAsync(postgres, "get_alert_details", "?server_id=abc&metric_name=x&alert_time=" + Uri.EscapeDataString(now.ToString("o"))));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }

    [Fact]
    public async Task GetAlertDetails_RefusesAMissingParameter()
    {
        var cs = RequireLivePostgres();
        await using var postgres = OpenDataSource(cs);
        foreach (var query in new[]
        {
            "?metric_name=x&alert_time=2026-01-01T00:00:00Z",
            "?server_id=1&alert_time=2026-01-01T00:00:00Z",
            "?server_id=1&metric_name=x",
        })
        {
            var json = await WebReadAsync(postgres, "get_alert_details", query);
            using var doc = JsonDocument.Parse(json);
            Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        }
    }

    [Fact]
    public async Task GetAlertHistory_IsUnchanged_TheWebRowAndTheMcpToolAnswerTheSameBytes_AndCarryNoDetails()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow), ct);
            await using var postgres = OpenDataSource(cs);

            var tool = await DarlingMcpAlertTools.GetAlertHistory(postgres, null, 1, 50, cancellationToken: ct);
            var web = await WebReadAsync(postgres, "get_alert_history", "?hours_back=1&limit=50");
            /* The old flag is gone: sending it changes nothing. */
            var withOldFlag = await WebReadAsync(postgres, "get_alert_history", "?hours_back=1&limit=50&include_details=true");

            Assert.Equal(tool, web);
            Assert.Equal(tool, withOldFlag);
            Assert.DoesNotContain("\"details\"", tool, StringComparison.Ordinal);
            Assert.DoesNotContain("force the better plan", tool, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }

    [Fact]
    public void GetAlertDetails_IsWebOnly_NotAnMcpTool_AndTheHistoryReadHasNoDetailsFlag()
    {
        /* No [McpServerTool] anywhere names it, and its implementation carries no tool attribute, so tools/list never sees it. */
        var toolNames = typeof(DarlingMcpAlertTools).Assembly.GetTypes()
            .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name)
            .Where(n => n is not null)
            .ToHashSet(StringComparer.Ordinal);
        Assert.DoesNotContain("get_alert_details", toolNames);
        Assert.Contains("get_alert_details", DarlingWebEndpoints.WebOnlyReadNames);

        var read = typeof(DarlingMcpAlertTools).GetMethod("GetAlertDetails", BindingFlags.NonPublic | BindingFlags.Static)!;
        Assert.Null(read.GetCustomAttribute<McpServerToolAttribute>());

        var history = typeof(DarlingMcpAlertTools).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == "get_alert_history");
        Assert.DoesNotContain(history.GetParameters(), p => p.Name!.Contains("detail", StringComparison.OrdinalIgnoreCase) && p.Name != "detail_text");
        Assert.DoesNotContain("include_details", history.GetCustomAttribute<DescriptionAttribute>()!.Description, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingWebEndpoints.CatalogDescriptors["get_alert_history"].Params, p => p.Name == "include_details");
        Assert.DoesNotContain("include_details", DarlingWebEndpoints.CatalogDescriptors["get_alert_history"].Description, StringComparison.Ordinal);

        /* Same role as get_alert_history (the catalog carries no role: both are plain /api/read reads of the viewer role),
           and its own descriptor, with the page's row identity as required parameters. */
        var descriptor = DarlingWebEndpoints.CatalogDescriptors["get_alert_details"];
        Assert.Equal(DarlingWebEndpoints.CatalogDescriptors["get_alert_history"].Category, descriptor.Category);
        Assert.Equal(new[] { "server_id", "metric_name", "alert_time" }, descriptor.Params.Select(p => p.Name).ToArray());
        Assert.All(descriptor.Params, p => Assert.True(p.Required));
    }

    [Fact]
    public void TheProjection_NeverReadsTheRemediationPayload()
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs"));
        var start = source.IndexOf("internal static JsonArray? ProjectDetails(", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = source.IndexOf("[McpServerTool(Name = ", start, StringComparison.Ordinal);
        var projection = source.Substring(start, end - start);
        Assert.Contains(".Body", projection, StringComparison.Ordinal);
        Assert.DoesNotContain("Remediation", projection, StringComparison.Ordinal);
    }
}
