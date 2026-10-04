/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres and never touches the shared
   store's tables, so it cannot race the live collection. */

/// <summary>
/// The collection caveats on the web Collection Health tabs (#4843): <c>get_collection_health</c> carries a
/// <c>collection_caveats</c> array from <c>collect.analysis_collection_caveats</c>, the SQL Server and PostgreSQL tabs draw it
/// as a grid of the desktop viewer's four columns, and a server with none shows no grid. No new tool carries it.
/// </summary>
public sealed class CollectionCaveatsWebTests
{
    private static JsonElement RunHarness()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-collection-caveats-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page scripts cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the collection-caveats harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the collection-caveats harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("postgres")]
    public void AServerWithNoCaveats_ShowsNoGrid_AndOneWithCaveatsShowsTheFourColumns(string engine)
    {
        var e = RunHarness().GetProperty(engine);

        var empty = e.GetProperty("empty");
        Assert.True(empty.GetProperty("hidden").GetBoolean(), "a payload without collection_caveats must leave the panel hidden");
        Assert.Empty(empty.GetProperty("rows").EnumerateArray());

        var rows = e.GetProperty("rows");
        Assert.False(rows.GetProperty("hidden").GetBoolean());
        Assert.Equal(new[] { "Family", "Reason", "Since", "Last seen" }, rows.GetProperty("heads").EnumerateArray().Select(h => h.GetString()).ToArray());
        var cells = rows.GetProperty("rows").EnumerateArray().Select(r => r.EnumerateArray().Select(c => c.GetString()).ToArray()).ToArray();
        Assert.Equal(2, cells.Length);
        Assert.Equal(new[] { "plans", "timeout" }, cells[0].Take(2).ToArray());
        Assert.Equal(new[] { "waits", "missing_schema" }, cells[1].Take(2).ToArray());
    }

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("postgres")]
    public void TheGridRidesTheHealthRead_WithItsParams_AndNoSecondRead(string engine)
    {
        var e = RunHarness().GetProperty(engine);
        foreach (var arm in new[] { "empty", "rows" })
        {
            var a = e.GetProperty(arm);
            Assert.Equal(new[] { "get_collection_health?server=srv-a" }, a.GetProperty("healthReads").EnumerateArray().Select(r => r.GetString()).ToArray());
            Assert.False(a.GetProperty("anyCaveatTool").GetBoolean());
        }
    }

    [Fact]
    public void BothTabsShareOnePanel_ReadingTheHealthPayloadsArray_AndNoCaveatToolExists()
    {
        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        Assert.Contains("rowsKey: \"collection_caveats\"", tabs, StringComparison.Ordinal);
        Assert.Contains("hideWhenNoRows: true", tabs, StringComparison.Ordinal);
        Assert.Equal(2, tabs.Split("        COLLECTION_CAVEATS_PANEL,", StringSplitOptions.None).Length - 1);
        Assert.DoesNotContain("get_collection_caveats", tabs, StringComparison.Ordinal);
        Assert.DoesNotContain("get_collection_caveats", ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"), StringComparison.Ordinal);
    }

    private static DarlingCollectionCaveatReader.Caveat Row(string family) =>
        new(family, "timeout", new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc));

    [Fact]
    public void ACleanServersPayloadGainsNoKey_AndCaveatsAreAttachedAsAnArray()
    {
        var cleanJson = JsonSerializer.Serialize(new { server = "a" });
        Assert.Equal(cleanJson, DarlingCollectionCaveatReader.AttachToJson(cleanJson, Array.Empty<DarlingCollectionCaveatReader.Caveat>()));

        var with = DarlingCollectionCaveatReader.AttachToJson(cleanJson, new[] { Row("plans") });
        using var doc = JsonDocument.Parse(with);
        var entry = Assert.Single(doc.RootElement.GetProperty("collection_caveats").EnumerateArray());
        Assert.Equal("plans", entry.GetProperty("family").GetString());
        Assert.Equal("timeout", entry.GetProperty("reason").GetString());
        Assert.StartsWith("2026-01-01T00:00:00", entry.GetProperty("first_seen_utc").GetString(), StringComparison.Ordinal);
        Assert.StartsWith("2026-01-02T00:00:00", entry.GetProperty("last_seen_utc").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task AMissingTableAnswersNoCaveats_AndAnUnreadableOneAnswersNoneWithOneWarning()
    {
        var warnings = new List<string>();
        var missing = await DarlingCollectionCaveatReader.ReadAsync(
            () => throw new PostgresException("relation does not exist", "ERROR", "ERROR", "42P01"), warnings.Add);
        Assert.Empty(missing);
        Assert.Empty(warnings);

        var denied = await DarlingCollectionCaveatReader.ReadAsync(
            () => throw new PostgresException("permission denied", "ERROR", "ERROR", "42501"), warnings.Add);
        Assert.Empty(denied);
        Assert.Single(warnings);

        await Assert.ThrowsAsync<PostgresException>(() => DarlingCollectionCaveatReader.ReadAsync(
            () => throw new PostgresException("boom", "ERROR", "ERROR", "XX000"), warnings.Add));
    }

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<JsonDocument> HealthAsync(NpgsqlDataSource postgres, string server, CancellationToken ct)
    {
        var json = await DarlingMcpDataTools.GetCollectionHealth(postgres, server, cancellationToken: ct);
        var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("collectors", out _), json.Length > 400 ? json[..400] : json);
        return doc;
    }

    [Fact]
    public async Task TheHealthReadCarriesTheStoresCaveats_OmitsThemWhenNone_AndSurvivesAStoreWithoutTheTable()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the collection-caveat health pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            const int serverId = 480_141;
            const string server = "caveat-health-a";
            var now = DateTime.UtcNow;
            await using (var register = new NpgsqlCommand(
                """
                INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
                VALUES ($1, $2, $2, TRUE, 15, $3, $3)
                """, connection))
            {
                register.Parameters.AddWithValue(serverId);
                register.Parameters.AddWithValue(server);
                register.Parameters.AddWithValue(DateTime.SpecifyKind(now, DateTimeKind.Unspecified));
                await register.ExecuteNonQueryAsync(ct);
            }

            await using (var log = new NpgsqlCommand(
                """
                INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected)
                VALUES ($1, $2, $3, $4, 'wait_stats', 'SUCCESS', 120, 10)
                """, connection))
            {
                log.Parameters.AddWithValue(CollectionIdGenerator.Next());
                log.Parameters.AddWithValue(DateTime.SpecifyKind(now.AddMinutes(-1), DateTimeKind.Unspecified));
                log.Parameters.AddWithValue(serverId);
                log.Parameters.AddWithValue(server);
                await log.ExecuteNonQueryAsync(ct);
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            // No rows: the key is absent, not null and not empty.
            using (var clean = await HealthAsync(postgres, server, ct))
            {
                Assert.False(clean.RootElement.TryGetProperty("collection_caveats", out _));
            }

            // Two rows through the store's own writer, read back through the real tool.
            await CollectionCaveatStore.ApplyPassAsync(
                postgres, serverId,
                new[] { new CollectionCaveatStore.UnreadFamily("waits", "missing_schema"), new CollectionCaveatStore.UnreadFamily("plans", "timeout") },
                now.AddHours(-2), null, ct);
            await CollectionCaveatStore.ApplyPassAsync(
                postgres, serverId,
                new[] { new CollectionCaveatStore.UnreadFamily("waits", "missing_schema"), new CollectionCaveatStore.UnreadFamily("plans", "timeout") },
                now, null, ct);
            using (var with = await HealthAsync(postgres, server, ct))
            {
                var entries = with.RootElement.GetProperty("collection_caveats").EnumerateArray().ToArray();
                Assert.Equal(new[] { "plans", "waits" }, entries.Select(x => x.GetProperty("family").GetString()).ToArray());
                Assert.Equal(new[] { "timeout", "missing_schema" }, entries.Select(x => x.GetProperty("reason").GetString()).ToArray());
                var first = DateTime.Parse(entries[0].GetProperty("first_seen_utc").GetString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);
                var last = DateTime.Parse(entries[0].GetProperty("last_seen_utc").GetString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);
                Assert.True(last - first > TimeSpan.FromHours(1.9), "Since keeps the first sighting and Last seen takes the newest");
            }

            // Another server's rows are not this server's.
            using (var other = await HealthAsync(postgres, server, ct))
            {
                Assert.Equal(2, other.RootElement.GetProperty("collection_caveats").GetArrayLength());
            }

            // A store below V141 has no table: no caveats, and the health read still answers.
            await using (var drop = new NpgsqlCommand("DROP TABLE collect.analysis_collection_caveats", connection))
            {
                await drop.ExecuteNonQueryAsync(ct);
            }

            using (var old = await HealthAsync(postgres, server, ct))
            {
                Assert.False(old.RootElement.TryGetProperty("collection_caveats", out _));
                Assert.True(old.RootElement.GetProperty("collectors").GetArrayLength() > 0);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
