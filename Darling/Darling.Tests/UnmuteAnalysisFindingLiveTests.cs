/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6383: <c>unmute_analysis_finding</c> undoes <c>mute_analysis_finding</c> over a live store. Gated on DARLING_TEST_PG.
/// The tool deletes through the viewer's own store call (<c>PgFindingStore.UnmuteStoryAsync</c>), so these tests
/// prove the pattern is reported again (the mute filter no longer holds the hash), that the scope match is exact, and
/// that the answer carries the previous mute and the call that puts it back.
/// </summary>
[Collection("live-postgres")]
public sealed class UnmuteAnalysisFindingLiveTests
{
    private const string ServerName = "darling-unmute-e2e";
    private const string ServerDisplay = "Unmute E2E";
    private const string ScopedHash = "n6383-scoped-hash";
    private const string GlobalHash = "n6383-global-hash";

    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public async Task Unmute_AfterMute_RestoresThePattern_ReportsPreviousAndUndo_AndHoldsTheScopeExactly()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live unmute test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            using (var register = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET server_name = EXCLUDED.server_name, display_name = EXCLUDED.display_name, is_enabled = TRUE;", connection))
            {
                register.Parameters.AddWithValue(ServerId);
                register.Parameters.AddWithValue(ServerName);
                register.Parameters.AddWithValue(ServerDisplay);
                register.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
                await register.ExecuteNonQueryAsync(ct);
            }

            var store = new PgFindingStore(postgres);
            var service = new DarlingAnalysisService(postgres);

            /* A per-server mute: in force, then removed by the same scope, with what it held and the call that restores it. */
            await DarlingMcpTools.MuteAnalysisFinding(service, postgres, ScopedHash, ServerName, "n6383 scoped reason");
            Assert.Contains(ScopedHash, await store.GetMutedStoryHashesAsync(ServerId, ct));

            var unmuted = JsonDocument.Parse(await DarlingMcpTools.UnmuteAnalysisFinding(service, postgres, ScopedHash, ServerName)).RootElement;
            Assert.Equal("unmuted", unmuted.GetProperty("status").GetString());
            Assert.Equal(1, unmuted.GetProperty("removed").GetInt32());
            Assert.Equal(ServerName, unmuted.GetProperty("server").GetString());
            var previous = unmuted.GetProperty("previous").EnumerateArray().Single();
            Assert.Equal("n6383 scoped reason", previous.GetProperty("reason").GetString());
            var undo = unmuted.GetProperty("undo");
            Assert.Equal("mute_analysis_finding", undo.GetProperty("tool").GetString());
            Assert.Equal(ScopedHash, undo.GetProperty("arguments").GetProperty("story_path_hash").GetString());
            Assert.Equal(ServerName, undo.GetProperty("arguments").GetProperty("server_name").GetString());
            Assert.Equal("n6383 scoped reason", undo.GetProperty("arguments").GetProperty("reason").GetString());

            /* The pattern is reported again: the mute filter no longer holds the hash. */
            Assert.DoesNotContain(ScopedHash, await store.GetMutedStoryHashesAsync(ServerId, ct));

            /* A second unmute changes nothing and says so. */
            var again = JsonDocument.Parse(await DarlingMcpTools.UnmuteAnalysisFinding(service, postgres, ScopedHash, ServerName)).RootElement;
            Assert.Equal("not_muted", again.GetProperty("status").GetString());
            Assert.Equal(0, again.GetProperty("removed").GetInt32());

            /* The undo it gave puts the mute back. */
            var restored = JsonDocument.Parse(await DarlingMcpTools.MuteAnalysisFinding(
                service, postgres, ScopedHash, ServerName, "n6383 scoped reason")).RootElement;
            Assert.True(restored.GetProperty("registered").GetBoolean());
            Assert.Contains(ScopedHash, await store.GetMutedStoryHashesAsync(ServerId, ct));

            /* Scope is exact: an all-servers mute is not removed by naming a server, and a per-server mute is not
               removed by omitting the name. */
            await DarlingMcpTools.MuteAnalysisFinding(service, postgres, GlobalHash, server_name: null, "n6383 global reason");
            var wrongScope = JsonDocument.Parse(await DarlingMcpTools.UnmuteAnalysisFinding(service, postgres, GlobalHash, ServerName)).RootElement;
            Assert.Equal("not_muted", wrongScope.GetProperty("status").GetString());
            Assert.Contains(GlobalHash, await store.GetMutedStoryHashesAsync(ServerId, ct));

            var wrongScopeAll = JsonDocument.Parse(await DarlingMcpTools.UnmuteAnalysisFinding(service, postgres, ScopedHash)).RootElement;
            Assert.Equal("not_muted", wrongScopeAll.GetProperty("status").GetString());
            Assert.Contains(ScopedHash, await store.GetMutedStoryHashesAsync(ServerId, ct));

            var global = JsonDocument.Parse(await DarlingMcpTools.UnmuteAnalysisFinding(service, postgres, GlobalHash)).RootElement;
            Assert.Equal("unmuted", global.GetProperty("status").GetString());
            Assert.Equal("(all servers)", global.GetProperty("server").GetString());
            Assert.False(global.GetProperty("undo").GetProperty("arguments").TryGetProperty("server_name", out _));
            Assert.DoesNotContain(GlobalHash, await store.GetMutedStoryHashesAsync(ServerId, ct));

            /* A name that matches no server refuses before anything is deleted. */
            var missing = JsonDocument.Parse(await DarlingMcpTools.UnmuteAnalysisFinding(service, postgres, ScopedHash, "darling-unmute-no-such-server")).RootElement;
            Assert.Equal("not_found", missing.GetProperty("status").GetString());
            Assert.Contains(ScopedHash, await store.GetMutedStoryHashesAsync(ServerId, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM servers WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId} OR story_path_hash IN ('{ScopedHash}', '{GlobalHash}');", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
