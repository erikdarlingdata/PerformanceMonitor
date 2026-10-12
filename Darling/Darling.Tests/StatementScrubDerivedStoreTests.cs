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
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, #5320 (lane R10, Layer 0b): the derived stores judge their own text where they write it. A finding or an
/// alert is built from rows a collector stored, and a row stored before the collection-time filter existed can still
/// hold a statement, so <c>story_text</c>, <c>drill_down_json</c>, <c>detail_text</c> and <c>context_json</c> go
/// through <see cref="DerivedStoreScrub"/> at the INSERT. Clean input comes back as the same instance, so a clean row
/// is stored byte for byte as before.
/// </summary>
public sealed class StatementScrubDerivedStoreTests
{
    private static string Fresh(string text) => new(text.AsSpan());

    private static void AssertNoSecret(string? value)
    {
        Assert.NotNull(value);
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, value, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Text_ReturnsTheSameInstanceOnCleanInput()
    {
        var clean = Fresh("Waits rose on " + StatementScrubCanary.PlainStatement);
        Assert.Same(clean, DerivedStoreScrub.Text(clean));
        Assert.Equal(string.Empty, DerivedStoreScrub.Text(string.Empty));
        Assert.Null(DerivedStoreScrub.Text(null));
    }

    [Fact]
    public void Text_WithholdsANamedStatement()
    {
        var result = DerivedStoreScrub.Text(Fresh("Blocked on " + StatementScrubCanary.CanaryStatement));
        Assert.Equal(SensitiveStatements.PlaceholderText, result);
    }

    [Fact]
    public void Json_ReturnsTheSameInstanceOnCleanInput()
    {
        var clean = Fresh("{\"rows\":[{\"query_text\":\"" + StatementScrubCanary.PlainStatement + "\",\"n\":3}]}");
        Assert.Same(clean, DerivedStoreScrub.Json(clean));
        Assert.Null(DerivedStoreScrub.Json(null));
    }

    [Fact]
    public void Json_ReplacesOnlyTheNamedValueAndKeepsTheRest()
    {
        var json = JsonSerializer.Serialize(new
        {
            rows = new[]
            {
                new { query_text = StatementScrubCanary.CanaryStatement, n = 1 },
                new { query_text = StatementScrubCanary.PlainStatement, n = 2 },
            },
        });

        var result = DerivedStoreScrub.Json(json);

        AssertNoSecret(result);
        Assert.Contains(SensitiveStatements.PlaceholderText, result, StringComparison.Ordinal);
        Assert.Contains("canary_plain_ssf", result, StringComparison.Ordinal);
        using var parsed = JsonDocument.Parse(result!);
        Assert.Equal(2, parsed.RootElement.GetProperty("rows").GetArrayLength());
    }

    [Fact]
    public void Json_StoresNullForADocumentTheFilterCannotRead()
    {
        Assert.Null(DerivedStoreScrub.Json("{ this is not json"));
    }

    /// <summary>The Darling writers call the helper; reverting either call fails this on the old shape.</summary>
    [Fact]
    public void TheDarlingWritersCallTheHelper()
    {
        var finding = File.ReadAllText(RepoPath("Darling/PerformanceMonitor.Darling.Analysis/PgFindingStore.cs"));
        Assert.Contains("DerivedStoreScrub.Text(finding.StoryText)", finding, StringComparison.Ordinal);
        Assert.Contains("DerivedStoreScrub.Json(DrillDownSerializer.Serialize(finding.DrillDown))", finding, StringComparison.Ordinal);

        var alert = File.ReadAllText(RepoPath("Darling/PerformanceMonitor.Darling.Service/PgAlertHistoryStore.cs"));
        Assert.Contains("DerivedStoreScrub.Text(record.DetailText)", alert, StringComparison.Ordinal);
        Assert.Contains("DerivedStoreScrub.Json(record.ContextJson)", alert, StringComparison.Ordinal);
    }

    /// <summary>
    /// The managed-role <c>config.record_custom_alert_resolution</c> function (R10 verdict: it cannot hold a
    /// statement). Its only caller is <c>CustomAlertEvaluator.WriteTeardownResolutionAsync</c>, which builds
    /// <c>detail_text</c> from the server name, the operator-typed rule name (newline-stripped and capped) and one of
    /// three fixed reasons. Nothing there reads a collected row. If that message ever starts to carry collected text,
    /// this pin fails and the write has to go through <see cref="DerivedStoreScrub.Text"/>.
    /// </summary>
    [Fact]
    public void TheCustomAlertResolutionRowCarriesNoCollectedText()
    {
        var evaluator = File.ReadAllText(RepoPath("Darling/PerformanceMonitor.Darling.Service/CustomAlertEvaluator.cs"));
        Assert.Contains("$\"{serverName}: {safeName} resolved because {reason}\"", evaluator, StringComparison.Ordinal);
        Assert.Contains("RecordCustomAlertResolutionAsync(serverId, serverName, title, message)", evaluator, StringComparison.Ordinal);

        var callers = Directory.EnumerateFiles(RepoPath("Darling"), "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !f.Contains($"{Path.DirectorySeparatorChar}Darling.Tests{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => File.ReadAllText(f).Contains(".RecordCustomAlertResolutionAsync(", StringComparison.Ordinal))
            .Select(Path.GetFileName)
            .ToList();
        Assert.Equal(new[] { "CustomAlertEvaluator.cs" }, callers);
    }

    private static string RepoPath(string relative, [CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", relative));
}

/// <summary>
/// The live half: a finding and an alert built from a raw planted source row store the marker in the real tables.
/// </summary>
[Collection("live-postgres")]
public sealed class StatementScrubDerivedStoreLiveTests
{
    private const int FindingServer = -532010;
    private const int AlertServer = -532011;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task RunLiveAsync(Func<NpgsqlDataSource, NpgsqlConnection, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live derived-store scrub test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(Ct);
        await PgMigrations.MigrateAsync(connection, Ct);
        await DeleteTestRowsAsync(connection, Ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await body(postgres, connection);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM analysis_findings WHERE server_id = {FindingServer}; DELETE FROM config_alert_log WHERE server_id = {AlertServer};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static AnalysisFinding Finding(string hash, string story, Dictionary<string, object>? drillDown)
    {
        return new AnalysisFinding
        {
            ServerId = FindingServer,
            ServerName = "derived-scrub-finding",
            Category = "queries",
            StoryPath = "SCRUB_A -> SCRUB_B",
            StoryPathHash = hash,
            IncidentId = "inc-" + hash,
            Severity = 1.2,
            Confidence = StoryConfidence.Compute(1, 5, 2),
            FactCount = 2,
            RootFactKey = "SCRUB_A",
            RootFactValue = 1.0,
            AnalysisTime = DateTime.UtcNow,
            StoryText = story,
            DrillDown = drillDown,
        };
    }

    private static Dictionary<string, object> DrillDown(string statement) => new()
    {
        ["top_queries"] = new[]
        {
            new Dictionary<string, object> { ["query_text"] = statement, ["executions"] = 7 },
        },
    };

    private static async Task<(string Story, string? Drill)> ReadFindingAsync(NpgsqlConnection connection, string hash)
    {
        using var command = new NpgsqlCommand(
            "SELECT story_text, drill_down_json FROM analysis_findings WHERE server_id = $1 AND story_path_hash = $2", connection);
        command.Parameters.AddWithValue(FindingServer);
        command.Parameters.AddWithValue(hash);
        await using var reader = await command.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));
        return (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1));
    }

    private static AnalysisContext Context()
    {
        var end = DateTime.UtcNow;
        return new AnalysisContext
        {
            ServerId = FindingServer,
            ServerName = "derived-scrub-finding",
            TimeRangeStart = end.AddHours(-4),
            TimeRangeEnd = end,
            ServerUtcOffset = TimeSpan.Zero,
        };
    }

    [Fact]
    public async Task AFindingBuiltFromARawPlantedRowStoresTheMarker()
    {
        await RunLiveAsync(async (postgres, connection) =>
        {
            var store = new PgFindingStore(postgres);
            var raw = Finding("rawcanary01", "Blocked on " + StatementScrubCanary.CanaryStatement,
                DrillDown(StatementScrubCanary.CanaryStatement));
            var plainDrill = DrillDown(StatementScrubCanary.PlainStatement);
            var plain = Finding("plaincanary01", "Waits rose on " + StatementScrubCanary.PlainStatement, plainDrill);

            await store.InsertFindingsAsync(new List<AnalysisFinding> { raw, plain }, Context());

            var (story, drill) = await ReadFindingAsync(connection, "rawcanary01");
            Assert.Equal(SensitiveStatements.PlaceholderText, story);
            Assert.NotNull(drill);
            foreach (var needle in StatementScrubCanary.SecretNeedles)
            {
                Assert.DoesNotContain(needle, drill, StringComparison.Ordinal);
            }
            Assert.Contains(SensitiveStatements.PlaceholderText, drill, StringComparison.Ordinal);
            using (JsonDocument.Parse(drill!)) { }

            /* The clean finding is stored byte for byte as the serializer wrote it. */
            var (plainStory, plainDrillStored) = await ReadFindingAsync(connection, "plaincanary01");
            Assert.Equal("Waits rose on " + StatementScrubCanary.PlainStatement, plainStory);
            Assert.Equal(DrillDownSerializer.Serialize(plainDrill), plainDrillStored);
        });
    }

    [Fact]
    public async Task AnAlertBuiltFromARawPlantedRowStoresTheMarker()
    {
        await RunLiveAsync(async (postgres, connection) =>
        {
            var store = new PgAlertHistoryStore(postgres);
            var contextJson = JsonSerializer.Serialize(new { dedup = "abc123", query_text = StatementScrubCanary.CanaryStatement });
            var plainContext = JsonSerializer.Serialize(new { dedup = "abc124", query_text = StatementScrubCanary.PlainStatement });

            await store.RecordAlertAsync(new AlertHistoryRecord(
                AlertServer.ToString(System.Globalization.CultureInfo.InvariantCulture), "derived-scrub-alert", "Scrub Raw",
                "1", "1", 1, 1, AlertDelivery.NoChannelApplies(), false,
                "Long query: " + StatementScrubCanary.CanaryStatement, contextJson));
            await store.RecordAlertAsync(new AlertHistoryRecord(
                AlertServer.ToString(System.Globalization.CultureInfo.InvariantCulture), "derived-scrub-alert", "Scrub Plain",
                "1", "1", 1, 1, AlertDelivery.NoChannelApplies(), false,
                "Long query: " + StatementScrubCanary.PlainStatement, plainContext));

            using var read = new NpgsqlCommand(
                "SELECT metric_name, detail_text, context_json FROM config_alert_log WHERE server_id = $1", connection);
            read.Parameters.AddWithValue(AlertServer);
            var rows = new Dictionary<string, (string? Detail, string? Context)>();
            await using (var reader = await read.ExecuteReaderAsync(Ct))
            {
                while (await reader.ReadAsync(Ct))
                {
                    rows[reader.GetString(0)] = (reader.IsDBNull(1) ? null : reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2));
                }
            }

            Assert.Equal(2, rows.Count);
            var raw = rows["Scrub Raw"];
            Assert.Equal(SensitiveStatements.PlaceholderText, raw.Detail);
            foreach (var needle in StatementScrubCanary.SecretNeedles)
            {
                Assert.DoesNotContain(needle, raw.Context, StringComparison.Ordinal);
            }
            Assert.Contains(SensitiveStatements.PlaceholderText, raw.Context, StringComparison.Ordinal);
            Assert.Contains("abc123", raw.Context, StringComparison.Ordinal);

            var plain = rows["Scrub Plain"];
            Assert.Equal("Long query: " + StatementScrubCanary.PlainStatement, plain.Detail);
            Assert.Equal(plainContext, plain.Context);
        });
    }
}
