/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// An Azure SQL Database master keeps its server-wide rows in the Blocking and Deadlocks lists while its counts skip
/// the databases monitored as their own servers, so those two reads carry one explanatory note, through the same
/// resolver the fleet card uses. No note for a master with no such sibling, a non-Azure server, or a host without the
/// registry; the rows are the same either way. Gated on DARLING_TEST_PG.
/// </summary>
/* #1776 own-store: every row is planted under dedicated server ids and deleted in cleanup. */
[Collection("live-postgres")]
public sealed class SeparatelyMonitoredListNoteTests
{
    private const string Base = "darling-list-note-master-scope";
    private const string AzureHost = "listnote.database.windows.net";
    private const string LoneHost = "listnotelone.database.windows.net";
    private static readonly int MasterId = ServerIdHelper.GetDeterministicHashCode(Base);
    private static readonly int GpId = MasterId + 1;
    private static readonly int PlainId = MasterId + 2;
    private static readonly int LoneId = MasterId + 3;
    private static readonly int[] AllIds = { MasterId, GpId, PlainId, LoneId };

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Graph(string db) =>
        $"<deadlock><process-list><process id=\"p0\" currentdbname=\"{db}\" /></process-list></deadlock>";

    [Fact]
    public void TheSharedSentence_IsExactlyTheAssignedText() =>
        Assert.Equal(
            "Events from databases monitored as their own servers are listed here and counted under those servers.",
            AzureMasterScope.SeparatelyMonitoredListNote);

    [Fact]
    public void TheWebPanels_DeclareTheNoteKey_AndTheHostsPassTheRegistry()
    {
        var js = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        var web = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        foreach (var read in new[] { "get_blocking", "get_deadlocks" })
        {
            var at = js.IndexOf("\"" + read + "\",", StringComparison.Ordinal);
            Assert.True(at > 0, read);
            var end = js.IndexOf("),", at, StringComparison.Ordinal);
            Assert.Contains("\"separately_monitored_note\"", js.Substring(at, end - at), StringComparison.Ordinal);
        }
        Assert.Contains("WebSqlTextPreviewLength, registryState, c.RequestAborted)", web, StringComparison.Ordinal);
        Assert.Contains("as_of: AsOf(c), registryState: registryState, cancellationToken: c.RequestAborted)", web, StringComparison.Ordinal);
    }

    [Fact]
    public void TheViewerNoteDecision_ShowsTheSharedSentenceOnlyForANonEmptyList()
    {
        Assert.Null(PerformanceMonitor.Darling.Viewer.ViewerDataService.SeparatelyMonitoredListNoteFor(Array.Empty<string>()));
        Assert.Equal(AzureMasterScope.SeparatelyMonitoredListNote,
            PerformanceMonitor.Darling.Viewer.ViewerDataService.SeparatelyMonitoredListNoteFor(new[] { "GP" }));

        var xaml = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml");
        var code = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.Blocking.cs");
        Assert.Contains("x:Name=\"BlockingSeparatelyMonitoredNote\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"DeadlockSeparatelyMonitoredNote\"", xaml, StringComparison.Ordinal);
        Assert.Contains("BlockingSeparatelyMonitoredNote.Visibility = note is null", code, StringComparison.Ordinal);
        Assert.Contains("DeadlockSeparatelyMonitoredNote.Visibility = note is null", code, StringComparison.Ordinal);
        Assert.Contains("SeparatelyMonitoredListNoteFor(await _dataService.GetSeparatelyMonitoredAsync(", code, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BlockingAndDeadlocks_CarryTheNote_OnlyForAMasterWithSiblings()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            async Task Server(int id, string name, int edition)
            {
                await Exec(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, sql_engine_edition, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, $3, now()::timestamp, now()::timestamp)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_engine_edition = $3", ct, id, name, edition);
                await Exec(connection, "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, engine_edition) VALUES ($1,$2,$3,$4,$5)",
                    ct, CollectionIdGenerator.Next(), DateTime.UtcNow.AddMinutes(-30), id, name, edition);
            }
            await Server(MasterId, Base + "-master", 5);
            await Server(GpId, Base + "-gp", 5);
            await Server(PlainId, Base + "-plain", 3);
            await Server(LoneId, Base + "-lone", 5);

            var at = DateTime.UtcNow.AddMinutes(-20);
            var seq = 0;
            async Task Event(int id, string name, string db)
            {
                await Exec(connection, "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1,$2,$3,$4,$2,12000,60,$5,'suspended',$6)",
                    ct, CollectionIdGenerator.Next(), at.AddSeconds(seq), id, name, 70 + seq++, db);
                await Exec(connection, "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1,$2,$3,$4,$2,$5,$6)",
                    ct, CollectionIdGenerator.Next(), at.AddSeconds(seq++), id, name, Graph(db), db);
            }
            await Event(MasterId, Base + "-master", "GP");
            await Event(MasterId, Base + "-master", "master");
            await Event(GpId, Base + "-gp", "GP");
            await Event(PlainId, Base + "-plain", "x");
            await Event(LoneId, Base + "-lone", "master");

            var state = new MonitoredServerRegistryState();
            state.Publish(new List<MonitoredServer>
            {
                new() { Name = "m", Host = AzureHost, Database = "master", StoredServerId = MasterId },
                new() { Name = "g", Host = AzureHost, Database = "GP", StoredServerId = GpId },
                new() { Name = "p", Host = "plain.example.test", Database = "master", StoredServerId = PlainId },
                new() { Name = "l", Host = LoneHost, Database = "master", StoredServerId = LoneId }
            });

            foreach (var (name, expectNote) in new[] { (Base + "-master", true), (Base + "-gp", false), (Base + "-plain", false), (Base + "-lone", false) })
            {
                var blocking = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlocking(postgres, name, registryState: state, cancellationToken: ct)).RootElement;
                var deadlocks = JsonDocument.Parse(await DarlingMcpBlockingTools.GetDeadlocks(postgres, name, registryState: state, cancellationToken: ct)).RootElement;
                foreach (var (root, rows) in new[] { (blocking, "events"), (deadlocks, "deadlocks") })
                {
                    Assert.Equal(name == Base + "-master" ? 2 : 1, root.GetProperty(rows).GetArrayLength());
                    if (expectNote)
                    {
                        Assert.Equal(AzureMasterScope.SeparatelyMonitoredListNote, root.GetProperty("separately_monitored_note").GetString());
                        Assert.Equal("GP", Assert.Single(root.GetProperty("separately_monitored_databases").EnumerateArray()).GetString());
                    }
                    else
                    {
                        Assert.Equal(JsonValueKind.Null, root.GetProperty("separately_monitored_note").ValueKind);
                    }
                }
            }

            /* A host without the registry (the tests, the dashboard app) says nothing, and the master's rows are unchanged. */
            var bare = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlocking(postgres, Base + "-master", cancellationToken: ct)).RootElement;
            Assert.Equal(JsonValueKind.Null, bare.GetProperty("separately_monitored_note").ValueKind);
            Assert.Equal(2, bare.GetProperty("events").GetArrayLength());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task Exec(NpgsqlConnection c, string sql, System.Threading.CancellationToken ct, params object[] p)
    {
        using var cmd = new NpgsqlCommand(sql, c);
        foreach (var v in p) cmd.Parameters.AddWithValue(v);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var ids = string.Join(", ", AllIds);
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id IN ({ids}); " +
            $"DELETE FROM deadlocks WHERE server_id IN ({ids}); " +
            $"DELETE FROM server_properties WHERE server_id IN ({ids}); " +
            $"DELETE FROM servers WHERE server_id IN ({ids});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
