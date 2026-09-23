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
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Availability Group MCP slice (#991) — get_ag_health, the cross-server AG topology. Ungated: the tool
/// surface is EXACTLY the one name (static, on a [McpServerToolType] class, returning Task&lt;string&gt;), its
/// param is optional (a fleet-wide read must work with no arguments), and the advertised tools/list schema is
/// Gemini-clean.
/// </summary>
public sealed class DarlingMcpAgToolsSurfaceTests
{
    private static readonly string[] AgToolSurface =
    {
        "get_ag_health",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpAgTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheOneAgTool()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(AgToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpAgTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(toolMethods, m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    private static (string Name, bool Optional)[] McpParams(string toolName)
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == toolName);
        return method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => (p.Name!, p.HasDefaultValue))
            .ToArray();
    }

    [Theory]
    [InlineData("get_ag_health", "server_name")]
    public void ParamContract_MatchesContract(string toolName, string expectedCsv)
    {
        Assert.Equal(expectedCsv.Split(','), McpParams(toolName).Select(p => p.Name).ToArray());
    }

    [Fact]
    public void ParamContract_ServerNameIsOptional_SoTheFleetWideReadNeedsNoArguments()
    {
        /* The whole point of this tool is the cross-server view; a required server_name would make the default
           call impossible and would also trip the no-required-params schema rule below. */
        foreach (var tool in AgToolSurface)
            Assert.All(McpParams(tool), p => Assert.True(p.Optional, $"{tool}.{p.Name} must be optional"));
    }

    /* ---------------- advertised MCP schema ---------------- */

    private static System.Collections.Generic.List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpAgTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_NoRequiredParams()
    {
        var tools = BuildToolSchemas();
        Assert.Single(tools);
        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
        foreach (var t in tools)
            Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(t.InputSchema));
    }

    /* ---------------- host + instructions wiring ---------------- */

    [Fact]
    public void Instructions_DocumentTheAgTool()
    {
        /* The instructions are the model's only map of this server; a tool the prose never names is one an agent
           will not reach for. Pinned because nothing else fails when the table row is forgotten. */
        Assert.Contains("get_ag_health", DarlingMcpInstructions.Text, StringComparison.Ordinal);
        Assert.Contains("Availability Group", DarlingMcpInstructions.Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3653 A15/A16: the description says the two things the reader's own doc comment warns about, because a
    /// caller reads the description and never the reader. <see cref="DarlingAgReader"/>'s summary carries two
    /// bolded traps — <b>Latest collection per server</b> (the collectors write NO row for a server with no AGs,
    /// so a dropped AG lingers as its last non-empty snapshot and the group's <c>collection_time</c> is the only
    /// tell) and <b>What the badge deliberately does NOT band</b> (every severity restates a DMV state or health
    /// string; lag and queue depth are raw magnitudes, so a badly lagging asynchronous secondary carries a healthy
    /// severity) — and the description used to say neither. #3696 put both sentences on it, plus the trap inside
    /// the second (the DMV reports 0 lag while data movement is suspended, so lag is read with
    /// <c>is_suspended</c>). Nothing pinned them, and a description that loses a sentence fails nothing: the
    /// reader's comment goes on being right in a file no agent opens.
    ///
    /// <para>Anchored on the reader's own paragraph headers first, so the pin fails with "re-anchor" if the
    /// reader stops documenting a trap rather than passing against prose that no longer exists; then on the
    /// description and the instructions paragraph — the two texts an agent reads before calling — for each
    /// trap's mechanism, consequence and the field that tells it apart. Phrase matching rather than a
    /// paraphrase test, because these are the exact words a reader would search for.</para>
    /// </summary>
    [Fact]
    public void Description_SurfacesTheReadersTwoDocumentedTraps()
    {
        var reader = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingAgReader.cs");
        Assert.Contains("<b>Latest collection per server.</b>", reader, StringComparison.Ordinal);
        Assert.Contains("<b>What the badge deliberately does NOT band: lag and queue depth.</b>", reader, StringComparison.Ordinal);

        var description = ToolMethods()
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == "get_ag_health")
            .GetCustomAttribute<DescriptionAttribute>()!.Description;

        /* Trap one, the stale snapshot: mechanism, consequence, tell. */
        Assert.Contains("NO row for a server with no AGs", description, StringComparison.Ordinal);
        Assert.Contains("last non-empty snapshot", description, StringComparison.Ordinal);
        Assert.Contains("collection_time", description, StringComparison.Ordinal);

        /* Trap two, the un-banded magnitudes: the negative on the word a caller would look for, the fields to
           read instead, and the DMV's 0-while-suspended inside it. */
        Assert.Contains("lag and queue depth are NOT banded", description, StringComparison.Ordinal);
        Assert.Contains("secondary_lag_seconds", description, StringComparison.Ordinal);
        Assert.Contains("is_suspended", description, StringComparison.Ordinal);
        Assert.Contains("0 lag while data movement is suspended", description, StringComparison.Ordinal);

        /* #3898 Phase 2 (D5): the instructions' per-tool AG paragraph is gone — the tool's own description is
           now the ONLY surface, so the perspective trap (LOCAL-replica-only columns) moved onto it here rather
           than staying duplicated. All four traps checked in one place. */
        Assert.Contains("lag and queue depth are NOT banded", description, StringComparison.Ordinal);
        Assert.Contains("the DMV reports 0 lag while data movement is suspended", description, StringComparison.Ordinal);
        Assert.Contains("keeps returning its last non-empty snapshot", description, StringComparison.Ordinal);
        Assert.Contains("populated only for the LOCAL replica", description, StringComparison.Ordinal);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip for get_ag_health. Plants a two-replica AG with one healthy database and
/// one suspended one, then asserts the tool groups them under the reporting server, bands the group Critical off
/// the suspended database, and derives the drain estimate — the whole read path over a real store.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpAgToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-ag-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AgHealth_ReadsPlantedTopology_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live AG-health test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var when = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);
            var collectionId = CollectionIdGenerator.Next();

            foreach (var (replica, role, health) in new[]
            {
                ("AGNODE1", "PRIMARY", "HEALTHY"),
                ("AGNODE2", "SECONDARY", "HEALTHY"),
            })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, operational_state_desc, connected_state_desc, recovery_health_desc, synchronization_health_desc, availability_mode_desc, failover_mode_desc, endpoint_url)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
                    collectionId, DarlingMcpTestData.Naive(when), ServerId, ServerName, "AG_E2E", replica, role,
                    role == "PRIMARY" ? "ONLINE" : null, "CONNECTED", "ONLINE", health, "SYNCHRONOUS_COMMIT", "AUTOMATIC", $"TCP://{replica}:5022");
            }

            /* One healthy database with a real backlog (so the drain estimate is exercised) and one suspended. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO ag_database_replica_states (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name, is_local, synchronization_state_desc, last_hardened_lsn, last_commit_lsn, log_send_queue_size, redo_queue_size, log_send_rate, redo_rate, is_suspended, suspend_reason_desc, availability_mode_desc, secondary_lag_seconds)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)",
                collectionId, DarlingMcpTestData.Naive(when), ServerId, ServerName, "AG_E2E", "SalesDb", "AGNODE2", false,
                "SYNCHRONIZED", "0x0001", "0x0002", 6000L, 0L, 100L, 0L, false, null, "SYNCHRONOUS_COMMIT", 3L);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO ag_database_replica_states (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name, is_local, synchronization_state_desc, last_hardened_lsn, last_commit_lsn, log_send_queue_size, redo_queue_size, log_send_rate, redo_rate, is_suspended, suspend_reason_desc, availability_mode_desc, secondary_lag_seconds)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)",
                collectionId, DarlingMcpTestData.Naive(when), ServerId, ServerName, "AG_E2E", "StoppedDb", "AGNODE2", false,
                "NOT SYNCHRONIZING", "0x0003", "0x0004", 9000L, 0L, 0L, 0L, true, "SUSPEND_FROM_USER", "SYNCHRONOUS_COMMIT", 0L);

            var json = await DarlingMcpAgTools.GetAgHealth(postgres, ServerName);
            Assert.False(McpHelpers.IsErrorEnvelope(json), $"tool returned an error: {json}");

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.Equal(1, root.GetProperty("availability_group_count").GetInt32());
            Assert.Equal(1, root.GetProperty("reporting_server_count").GetInt32());

            var group = root.GetProperty("availability_groups")[0];
            Assert.Equal(ServerName, group.GetProperty("server_name").GetString());
            Assert.Equal("AG_E2E", group.GetProperty("ag_name").GetString());
            Assert.Equal("AGNODE1", group.GetProperty("primary_replica").GetString());
            Assert.Equal(2, group.GetProperty("replicas").GetArrayLength());
            Assert.Equal(2, group.GetProperty("databases").GetArrayLength());

            /* The suspended database drives the group Critical even though both replicas report HEALTHY — the
               reason this surface exists. */
            Assert.Equal("Critical", group.GetProperty("severity").GetString());

            var databases = group.GetProperty("databases").EnumerateArray().ToList();
            var sales = databases.Single(d => d.GetProperty("database_name").GetString() == "SalesDb");
            var stopped = databases.Single(d => d.GetProperty("database_name").GetString() == "StoppedDb");

            /* 6000 KB at 100 KB/s = 1.0 minute. */
            Assert.Equal(1.0, sales.GetProperty("est_send_drain_minutes").GetDouble());
            Assert.Equal("Healthy", sales.GetProperty("synchronization_state_severity").GetString());

            /* A stalled backlog has no finite drain, and the suspended row bands Critical. */
            Assert.Equal(JsonValueKind.Null, stopped.GetProperty("est_send_drain_minutes").ValueKind);
            Assert.Equal("Critical", stopped.GetProperty("synchronization_state_severity").GetString());
            Assert.Equal("SUSPEND_FROM_USER", stopped.GetProperty("suspend_reason").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task AgHealth_FleetWideReadNeedsNoServerFilter_AgainstDevPostgres()
    {
        /* The UNFILTERED branch ($1 bound to NULL) is the one GET /api/ag takes on every page load and nav probe,
           so it needs its own live pin — the filtered path above would not catch a broken IS NULL guard. */
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live AG-health test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var when = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, operational_state_desc, connected_state_desc, recovery_health_desc, synchronization_health_desc, availability_mode_desc, failover_mode_desc, endpoint_url)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), ServerId, ServerName, "AG_FLEET", "AGNODE1", "PRIMARY",
                "ONLINE", "CONNECTED", "ONLINE", "HEALTHY", "SYNCHRONOUS_COMMIT", "AUTOMATIC", "TCP://AGNODE1:5022");

            /* Straight through the reader, exactly as the /api/ag endpoint calls it — no server filter. */
            var result = await DarlingAgReader.GetAgHealthAsync(postgres, null, DateTime.UtcNow, ct);

            var group = Assert.Single(result.AvailabilityGroups, g => g.ServerName == ServerName);
            Assert.Equal("AG_FLEET", group.AgName);
            Assert.Equal("AGNODE1", group.PrimaryReplica);
            Assert.Equal(HealthSeverity.Healthy, group.Severity);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task AgHealth_ReturnsOnlyTheNewestSnapshot_AgainstDevPostgres()
    {
        /* THE question this SQL exists to answer. Two collection instants for one server, with a DIFFERENT replica
           set in each: only the newer instant's rows may come back. A join that forgot the MAX(collection_time)
           predicate returns both and this fails; a DISTINCT ON would return one row and this fails too. */
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live AG-health test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var newest = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);
            var older = newest.AddHours(-1);

            /* Older sweep: three replicas, and the AG still reports NOT_HEALTHY. */
            foreach (var replica in new[] { "OLD1", "OLD2", "OLD3" })
            {
                await InsertReplicaAsync(connection, ct, older, "AG_SNAP", replica, "SECONDARY", "NOT_HEALTHY");
            }

            /* Newest sweep: one replica, healthy. */
            await InsertReplicaAsync(connection, ct, newest, "AG_SNAP", "NEW1", "PRIMARY", "HEALTHY");

            var result = await DarlingAgReader.GetAgHealthAsync(postgres, null, DateTime.UtcNow, ct);

            var group = Assert.Single(result.AvailabilityGroups, g => g.ServerName == ServerName);
            Assert.Equal(newest, group.CollectionTime);
            Assert.Equal("NEW1", Assert.Single(group.Replicas).ReplicaServerName);
            Assert.Equal(HealthSeverity.Healthy, group.Severity);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task AgHealth_ServerFilterExcludesOtherServers_AgainstDevPostgres()
    {
        /* The filter has to EXCLUDE, not merely not-crash: a one-server fixture passes even with the predicate
           deleted, so this plants a second server and asserts it is absent from the filtered read and present in
           the unfiltered one. */
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live AG-health test.");

        const string OtherName = "darling-mcp-ag-e2e-other";
        var otherId = ServerIdHelper.GetDeterministicHashCode(OtherName);

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await DeleteServerRowsAsync(connection, otherId, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, otherId, OtherName, ct);
            var when = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);

            await InsertReplicaAsync(connection, ct, when, "AG_MINE", "MINE1", "PRIMARY", "HEALTHY");
            await InsertReplicaAsync(connection, ct, when, "AG_THEIRS", "THEIRS1", "PRIMARY", "HEALTHY", otherId, OtherName);

            var filtered = await DarlingAgReader.GetAgHealthAsync(postgres, ServerId, DateTime.UtcNow, ct);
            Assert.Equal("AG_MINE", Assert.Single(filtered.AvailabilityGroups).AgName);

            var unfiltered = await DarlingAgReader.GetAgHealthAsync(postgres, null, DateTime.UtcNow, ct);
            var names = unfiltered.AvailabilityGroups.Select(g => g.AgName).ToList();
            Assert.Contains("AG_MINE", names);
            Assert.Contains("AG_THEIRS", names);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
                await DeleteServerRowsAsync(cleanup, otherId, cleanupCt);
            });
        }
    }

    [Fact]
    public async Task AgHealth_SkipsServersDisabledInTheRegistry_AgainstDevPostgres()
    {
        /* A server disabled in the control plane leaves /api/fleet and the sidebar immediately; its AG cards must
           go with it rather than linger until retention expires the collected rows. */
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live AG-health test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var when = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);
            await InsertReplicaAsync(connection, ct, when, "AG_DISABLED", "NODE1", "PRIMARY", "HEALTHY");

            var before = await DarlingAgReader.GetAgHealthAsync(postgres, null, DateTime.UtcNow, ct);
            Assert.Contains("AG_DISABLED", before.AvailabilityGroups.Select(g => g.AgName));

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE servers SET is_enabled = FALSE WHERE server_id = $1", ServerId);

            var after = await DarlingAgReader.GetAgHealthAsync(postgres, null, DateTime.UtcNow, ct);
            Assert.DoesNotContain("AG_DISABLED", after.AvailabilityGroups.Select(g => g.AgName));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static Task InsertReplicaAsync(
        NpgsqlConnection connection,
        System.Threading.CancellationToken ct,
        DateTime when,
        string agName,
        string replicaName,
        string role,
        string syncHealth,
        int? serverId = null,
        string? serverName = null) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, operational_state_desc, connected_state_desc, recovery_health_desc, synchronization_health_desc, availability_mode_desc, failover_mode_desc, endpoint_url)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), serverId ?? ServerId, serverName ?? ServerName,
            agName, replicaName, role, role == "PRIMARY" ? "ONLINE" : null, "CONNECTED", "ONLINE", syncHealth,
            "SYNCHRONOUS_COMMIT", "AUTOMATIC", $"TCP://{replicaName}:5022");

    private static async Task DeleteServerRowsAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        var sql = string.Join(" ", new[] { "ag_replica_states", "ag_database_replica_states" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {serverId};"))
            + $" DELETE FROM servers WHERE server_id = {serverId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task AgHealth_EmptyStoreAnswersTheEmptyEnvelope_NotAnError()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live AG-health test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var json = await DarlingMcpAgTools.GetAgHealth(postgres, ServerName);

            Assert.False(McpHelpers.IsErrorEnvelope(json), $"tool returned an error: {json}");
            using var doc = JsonDocument.Parse(json);
            Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var sql = string.Join(" ", new[] { "ag_replica_states", "ag_database_replica_states" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"))
            + $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
