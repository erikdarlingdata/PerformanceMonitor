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
/// headings and fields only, so the page had nothing to show. The web mirror's <c>include_details</c> returns the
/// items; the MCP tool does not take the flag, so its answer is what it was. The typed Apply payload
/// (<c>Remediation</c>) is never projected: the web is advise-only.
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

    private static async Task<string> WebReadAsync(NpgsqlDataSource postgres, string query, int limit = 50)
    {
        var context = new DefaultHttpContext();
        context.Request.QueryString = new QueryString("?hours_back=1&limit=" + limit + query);
        return await DarlingWebEndpoints.BuildReadDispatch()["get_alert_history"](context, postgres, null!);
    }

    private static JsonElement Row(string json, string metric)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("alerts").EnumerateArray()
            .Single(a => a.GetProperty("metric_name").GetString() == metric).Clone();
    }

    [Fact]
    public async Task WithIncludeDetails_TheRowCarriesTheAdviceAndTheFixScript_AndNeverTheApplyPayload()
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

            var json = await WebReadAsync(postgres, "&include_details=true");
            var advice = Row(json, AdviceMetric);
            var details = advice.GetProperty("details").EnumerateArray().ToList();
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

            /* An engine alert's context holds no prose: its detail_text already says everything, so it gets none. */
            Assert.False(Row(json, EngineMetric).TryGetProperty("details", out _));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }

    [Fact]
    public async Task WithoutIncludeDetails_TheWebRowIsByteIdenticalToTheMcpToolsAnswer_AndCarriesNoDetails()
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
            var web = await WebReadAsync(postgres, "");
            var explicitOff = await WebReadAsync(postgres, "&include_details=false");

            Assert.Equal(tool, web);
            Assert.Equal(tool, explicitOff);
            Assert.DoesNotContain("\"details\"", tool, StringComparison.Ordinal);

            /* The MCP tool answers the same whether or not the flag exists: it cannot be asked for details. */
            var withFlag = await WebReadAsync(postgres, "&include_details=true");
            Assert.NotEqual(tool, withFlag);
            Assert.Equal(tool, (await DarlingMcpAlertTools.GetAlertHistory(postgres, null, 1, 50, cancellationToken: ct)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }

    [Fact]
    public async Task OnlyTheNewestRowsUpToTheCapCarryDetails_SoAThousandRowPageStaysBounded()
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
            await DeleteAsync(connection, ct);
            var total = DarlingMcpAlertTools.MaxDetailRows + 5;
            var context = AdviceContextJson();
            for (var i = 0; i < total; i++)
                await InsertAlertAsync(connection, ct, now.AddSeconds(-i), AdviceMetric + i, "Diagnosis\n  Story: x\n  Severity: 1.20", context);
            await using var postgres = OpenDataSource(cs);

            var json = await WebReadAsync(postgres, "&include_details=true", total);
            using var doc = JsonDocument.Parse(json);
            var rows = doc.RootElement.GetProperty("alerts").EnumerateArray()
                .Where(a => a.GetProperty("server_id").GetInt32() == ServerId).ToList();
            Assert.Equal(total, rows.Count);
            Assert.Equal(DarlingMcpAlertTools.MaxDetailRows, rows.Count(a => a.TryGetProperty("details", out _)));
            Assert.All(rows.Take(DarlingMcpAlertTools.MaxDetailRows), a => Assert.True(a.TryGetProperty("details", out _)));
            Assert.All(rows.Skip(DarlingMcpAlertTools.MaxDetailRows), a => Assert.False(a.TryGetProperty("details", out _)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }

    [Fact]
    public void TheMcpTool_CannotBeAskedForDetails_AndTheWebCatalogAdvertisesTheFlag()
    {
        var method = typeof(DarlingMcpAlertTools).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == "get_alert_history");
        Assert.DoesNotContain(method.GetParameters(), p => p.Name!.Contains("detail", StringComparison.OrdinalIgnoreCase) && p.Name != "detail_text");
        Assert.DoesNotContain("include_details", method.GetCustomAttribute<DescriptionAttribute>()!.Description, StringComparison.Ordinal);

        /* The projection lives on an un-attributed internal method, so tools/list never sees it. */
        var read = typeof(DarlingMcpAlertTools).GetMethod("GetAlertHistoryRead", BindingFlags.NonPublic | BindingFlags.Static)!;
        Assert.Null(read.GetCustomAttribute<McpServerToolAttribute>());

        var param = DarlingWebEndpoints.CatalogDescriptors["get_alert_history"].Params.Single(p => p.Name == "include_details");
        Assert.Equal("bool", param.Type);
        Assert.False(param.Required);
        Assert.Equal(false, param.Default);

        var source = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs"));
        var dispatchLine = source.Split('\n').Single(l =>
            l.Contains("DarlingMcpAlertTools.GetAlertHistoryRead(", StringComparison.Ordinal));
        Assert.Contains("includeDetails: QueryBool(c,", dispatchLine, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProjection_NeverReadsTheRemediationPayload()
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs"));
        var start = source.IndexOf("internal static object WithDetails(", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = source.IndexOf("[McpServerTool(Name = ", start, StringComparison.Ordinal);
        var projection = source.Substring(start, end - start);
        Assert.Contains(".Body", projection, StringComparison.Ordinal);
        Assert.DoesNotContain("Remediation", projection, StringComparison.Ordinal);
    }
}
