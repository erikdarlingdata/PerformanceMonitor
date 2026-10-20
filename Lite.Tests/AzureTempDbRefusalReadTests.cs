/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6177 through the Lite tool: <c>get_tempdb_trend</c> on a store whose newest <c>tempdb_stats</c> run was refused (the
/// contained database user on Azure SQL Database) and that holds no tempdb rows answers <c>precondition</c> with the
/// contained-user sentence. With tempdb rows in the window it still answers with data.
/// </summary>
public sealed class AzureTempDbRefusalReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string Refusal = "VIEW DATABASE PERFORMANCE STATE permission denied in database 'tempdb'.";

    private readonly SharedDuckDbFixture _fixture;
    private readonly string _configDir;
    private readonly ServerManager _servers;
    private readonly int _serverId;

    public AzureTempDbRefusalReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
        _configDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_configDir);
        _servers = new ServerManager(_configDir);
        _servers.AddServer(new ServerConnection { ServerName = "azure-6177.example.net", DisplayName = "azure-6177" });
        var (resolved, error) = ServerResolver.ResolveOrError(_servers, null);
        Assert.Null(error);
        _serverId = resolved.ServerId;
    }

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { }
    }

    [Fact]
    public async Task ARefusedTempDbCollector_WithNoTempDbRows_AnswersPreconditionWithTheContainedUserSentence()
    {
        var stored = Refusal + AzureDmvPermissionHint.For(262, isAzureSqlDb: true, Refusal);
        await InsertAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message)
VALUES (61770, $1, 'azure-6177', 'tempdb_stats', $2, 5, 'PERMISSIONS', $3)", _serverId, DateTime.UtcNow.AddMinutes(-1), stored);

        var answer = JsonDocument.Parse(await McpTempDbTools.GetTempDbTrend(new LocalDataService(_fixture.DuckDb), _servers)).RootElement;

        Assert.Equal("precondition", answer.GetProperty("status").GetString());
        var message = answer.GetProperty("message").GetString()!;
        Assert.Contains("tempdb_stats", message, StringComparison.Ordinal);
        Assert.Contains("contained database user", message, StringComparison.Ordinal);
        Assert.Contains("##MS_ServerStateReader##", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ATempDbWindowWithRows_StillAnswersWithData()
    {
        await InsertAsync(@"
INSERT INTO tempdb_stats
    (collection_id, collection_time, server_id, server_name,
     user_object_reserved_mb, internal_object_reserved_mb, version_store_reserved_mb,
     total_reserved_mb, unallocated_mb, total_sessions_using_tempdb, top_session_id, top_session_tempdb_mb)
VALUES (-61771, $1, $2, 'azure-6177', 10, 5, 5, 20, 10, 3, 55, 7)", DateTime.UtcNow.AddMinutes(-5), _serverId);

        var json = await McpTempDbTools.GetTempDbTrend(new LocalDataService(_fixture.DuckDb), _servers);

        Assert.DoesNotContain("\"precondition\"", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"unavailable\"", json, StringComparison.Ordinal);
        Assert.True(JsonDocument.Parse(json).RootElement.GetProperty("trend").GetArrayLength() > 0);
    }

    private async Task InsertAsync(string sql, params object[] values)
    {
        using var connection = _fixture.DuckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var value in values)
        {
            command.Parameters.Add(new DuckDBParameter { Value = value });
        }

        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
