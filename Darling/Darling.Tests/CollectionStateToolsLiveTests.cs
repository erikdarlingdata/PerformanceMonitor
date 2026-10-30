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
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using Tools = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpCollectionStateTools;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another
   one. The role it connects as is created for the fact and dropped in its cleanup. */

/// <summary>
/// The collection-state tools (#6230 part A) against a store, connected as the <c>mcp</c> role with the grants
/// <see cref="DarlingManagedRoles.BuildProvisioningSql"/> hands it, so removing a grant a tool needs makes these fail with 42501.
/// The service's own config read (<see cref="StoreConfigProvider.LoadViewAsync"/>) is the witness that the next pass sees the change.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class CollectionStateToolsLiveTests
{
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    private sealed record Rig(ScratchPostgres Scratch, NpgsqlDataSource Owner, NpgsqlDataSource Mcp, string Role) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await Mcp.DisposeAsync();
            await Owner.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    private static List<string> McpStatements(string roleName, string database)
    {
        var provisioning = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);
        var statements = new List<string>();
        var uncommented = string.Join('\n', provisioning.Split('\n').Where(l => !l.TrimStart().StartsWith("--", StringComparison.Ordinal)));
        foreach (var raw in uncommented.Split(';'))
        {
            var statement = raw.Trim();
            var isGrant = statement.StartsWith("GRANT ", StringComparison.Ordinal);
            if (!isGrant && !statement.StartsWith("REVOKE ", StringComparison.Ordinal))
            {
                continue;
            }

            var marker = isGrant ? " TO " : " FROM ";
            var at = statement.LastIndexOf(marker, StringComparison.Ordinal);
            if (at < 0 || ViewerGrantReplay.CannotReplayOnScratchStore(statement))
            {
                continue;
            }

            if (!statement[(at + marker.Length)..].Split(',').Select(t => t.Trim()).Contains("mcp", StringComparer.Ordinal))
            {
                continue;
            }

            statements.Add(ViewerGrantReplay.OnDatabase(statement[..at], database) + marker + roleName);
        }

        return statements;
    }

    private static async Task<Rig> OpenAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collection-state tests (each mints its own scratch database and role).");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        var owner = NpgsqlDataSource.Create(ownerString);
        var role = "coll_state_" + Guid.NewGuid().ToString("N")[..8];
        await ExecAsync(owner, $"CREATE ROLE {role} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
        foreach (var statement in McpStatements(role, scratch.DatabaseName))
        {
            await ExecAsync(owner, statement, ct);
        }

        /* The queue function pause and resume go through, as the product builds it (the scratch store holds the migrations only), with the EXECUTE the batch grants mcp. */
        await ExecAsync(owner, DarlingManagedRoles.BuildRequestMcpCommandFunctionSql("config"), ct);
        await ExecAsync(owner, $"GRANT EXECUTE ON FUNCTION config.request_mcp_command(text, integer, integer, text) TO {role}", ct);
        await ExecAsync(owner, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
        var mcp = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(ownerString) { Username = role, Password = RolePassword }.ConnectionString);
        return new Rig(scratch, owner, mcp, role);
    }

    private static async Task DropRoleAsync(Rig rig, bool bodySucceeded) =>
        await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () =>
            ExecAsync(rig.Owner, $"DROP OWNED BY {rig.Role}; DROP ROLE IF EXISTS {rig.Role};", CancellationToken.None));

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return (T)Convert.ChangeType((await command.ExecuteScalarAsync(ct))!, typeof(T));
    }

    private static Task SeedServerAsync(NpgsqlDataSource owner, int id, string name, string excludedSql, CancellationToken ct) =>
        ExecAsync(owner, $@"INSERT INTO config_monitored_servers (server_id, name, host, auth, excluded_databases, is_enabled)
            VALUES ({id}, '{name}', '{name}.example.test', 'integrated', {excludedSql}, TRUE)", ct);

    private static JsonNode Parse(string answer) => JsonNode.Parse(answer)!;

    private static string Status(JsonNode answer) => answer["status"]!.GetValue<string>();

    private static string[] Strings(JsonNode? array) => array!.AsArray().Select(n => n!.GetValue<string>()).ToArray();

    /// <summary>What the service's next config read would hand the worker: the pause flag, the enabled servers and their exclusions.</summary>
    private static async Task<StoreConfigView> ServiceViewAsync(Rig rig, CancellationToken ct) =>
        (await new StoreConfigProvider(rig.Owner).LoadViewAsync(new DarlingConfig(), ct))!;

    private static string[] EnabledNames(StoreConfigView view) => view.EnabledServers.Select(s => s.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();

    /// <summary>An undo of a pause that has not run yet is a real undo: the flag still reads "running", but the pause is queued, so the
    /// resume is queued behind it and the service (which runs them in command_id order) ends up running.</summary>
    [Fact]
    public async Task ResumeCollection_WhileThePauseIsStillQueued_QueuesTheResume_AndTheServerEndsUpRunning()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            var paused = Parse(await Tools.PauseCollection(rig.Mcp, cancellationToken: ct));
            var pauseId = paused["command_id"]!.GetValue<long>();
            Assert.False((await ServiceViewAsync(rig, ct)).Paused);

            var resumed = Parse(await Tools.ResumeCollection(rig.Mcp, cancellationToken: ct));
            Assert.Equal("queued", Status(resumed));
            var resumeId = resumed["command_id"]!.GetValue<long>();
            Assert.True(resumeId > pauseId);
            Assert.Equal(pauseId, resumed["runs_after"]!["command_id"]!.GetValue<long>());
            Assert.Contains(pauseId.ToString(System.Globalization.CultureInfo.InvariantCulture), resumed["check"]!.GetValue<string>(), StringComparison.Ordinal);

            /* A resume now would change nothing more; a pause is again the one that changes the end state. */
            Assert.Equal("unchanged", Status(Parse(await Tools.ResumeCollection(rig.Mcp, cancellationToken: ct))));

            foreach (var (id, type) in new[] { (pauseId, "pause"), (resumeId, "resume") })
            {
                await DarlingCommandExecutor.ExecuteStoreWriteAsync(
                    rig.Owner, DarlingCommandExecutor.ResolvePlan(new ClaimedCommand(id, type, null, null, "mcp")), ct);
            }

            Assert.True(DarlingWorker.ShouldRunCollection((await ServiceViewAsync(rig, ct)).Paused));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task PauseCollection_StopsTheNextPass_AndItsOwnUndoStartsItAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            Assert.True(DarlingWorker.ShouldRunCollection((await ServiceViewAsync(rig, ct)).Paused));

            var paused = Parse(await Tools.PauseCollection(rig.Mcp, cancellationToken: ct));
            Assert.Equal("queued", Status(paused));
            Assert.False(paused["previous"]!["paused"]!.GetValue<bool>());
            Assert.Equal("resume_collection", paused["undo"]!["tool"]!.GetValue<string>());
            var pauseId = paused["command_id"]!.GetValue<long>();
            Assert.Equal("pause/mcp/pending", await ScalarAsync<string>(rig.Owner,
                $"SELECT command_type || '/' || requested_by || '/' || status FROM config_command WHERE command_id = {pauseId}", ct));
            Assert.False((await ServiceViewAsync(rig, ct)).Paused);

            /* The service runs the queued command with the plan the command plane resolves for it. */
            await DarlingCommandExecutor.ExecuteStoreWriteAsync(
                rig.Owner, DarlingCommandExecutor.ResolvePlan(new ClaimedCommand(pauseId, "pause", null, null, "mcp")), ct);
            var view = await ServiceViewAsync(rig, ct);
            Assert.True(view.Paused);
            Assert.False(DarlingWorker.ShouldRunCollection(view.Paused));
            Assert.True(Parse(await Tools.GetCollectionState(rig.Mcp, ct))["paused"]!.GetValue<bool>());

            /* A second pause says so and queues nothing. */
            Assert.Equal("unchanged", Status(Parse(await Tools.PauseCollection(rig.Mcp, cancellationToken: ct))));
            Assert.Equal(1L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM config_command", ct));

            /* The undo the answer named: resume. */
            var resumed = Parse(await Tools.ResumeCollection(rig.Mcp, cancellationToken: ct));
            Assert.Equal("queued", Status(resumed));
            Assert.Equal("pause_collection", resumed["undo"]!["tool"]!.GetValue<string>());
            var resumeId = resumed["command_id"]!.GetValue<long>();
            await DarlingCommandExecutor.ExecuteStoreWriteAsync(
                rig.Owner, DarlingCommandExecutor.ResolvePlan(new ClaimedCommand(resumeId, "resume", null, null, "mcp")), ct);
            Assert.True(DarlingWorker.ShouldRunCollection((await ServiceViewAsync(rig, ct)).Paused));
            Assert.False(Parse(await Tools.GetCollectionState(rig.Mcp, ct))["paused"]!.GetValue<bool>());
            Assert.Equal("unchanged", Status(Parse(await Tools.ResumeCollection(rig.Mcp, cancellationToken: ct))));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task McpRole_CannotSetTheViewerOnlyServiceFlags_ThePausedFlagIncluded()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            /* #6393 gave mcp UPDATE on five flags (capture_plans among them) for update_service_flags; mcp_enabled stays Viewer-only. */
            var denied = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(rig.Mcp, "UPDATE config.config_service SET mcp_enabled = NOT mcp_enabled WHERE id = 1", ct));
            Assert.Equal("42501", denied.SqlState);
            var pausedDenied = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(rig.Mcp, "UPDATE config.config_service SET paused = TRUE WHERE id = 1", ct));
            Assert.Equal("42501", pausedDenied.SqlState);
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task DisableServer_SkipsItOnTheNextPass_AndEnableServerBringsItBack()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 6201, "orders-primary", "ARRAY[]::text[]", ct);
            await SeedServerAsync(rig.Owner, 6202, "billing-primary", "ARRAY[]::text[]", ct);
            Assert.Equal(new[] { "billing-primary", "orders-primary" }, EnabledNames(await ServiceViewAsync(rig, ct)));

            var disabled = Parse(await Tools.DisableServer(rig.Mcp, "orders-primary", cancellationToken: ct));
            Assert.Equal("changed", Status(disabled));
            Assert.True(disabled["previous"]!["enabled"]!.GetValue<bool>());
            Assert.False(disabled["current"]!["enabled"]!.GetValue<bool>());
            Assert.Equal("enable_server", disabled["undo"]!["tool"]!.GetValue<string>());
            Assert.Equal("orders-primary.example.test", disabled["undo"]!["arguments"]!["server_name"]!.GetValue<string>());
            Assert.Equal("orders-primary.example.test", disabled["storage_name"]!.GetValue<string>());
            Assert.Equal(new[] { "billing-primary" }, EnabledNames(await ServiceViewAsync(rig, ct)));

            var state = Parse(await Tools.GetCollectionState(rig.Mcp, ct));
            var row = state["servers"]!.AsArray().Single(s => s!["server"]!.GetValue<string>() == "orders-primary")!;
            Assert.False(row["enabled"]!.GetValue<bool>());

            /* A disabled server is still found, by a part of its name, so it can be enabled again. Disabling twice changes nothing. */
            Assert.Equal("unchanged", Status(Parse(await Tools.DisableServer(rig.Mcp, "orders", cancellationToken: ct))));
            var enabled = Parse(await Tools.EnableServer(rig.Mcp, "orders", cancellationToken: ct));
            Assert.Equal("changed", Status(enabled));
            Assert.Equal("disable_server", enabled["undo"]!["tool"]!.GetValue<string>());
            Assert.Equal(new[] { "billing-primary", "orders-primary" }, EnabledNames(await ServiceViewAsync(rig, ct)));
            Assert.Equal("unchanged", Status(Parse(await Tools.EnableServer(rig.Mcp, "orders-primary", cancellationToken: ct))));

            /* Words for a bad argument, and nothing written. */
            var versionBefore = await ScalarAsync<long>(rig.Owner, "SELECT config_version FROM config_service", ct);
            var missing = Parse(await Tools.DisableServer(rig.Mcp, "no-such-server", cancellationToken: ct));
            Assert.Equal("refused", Status(missing));
            Assert.Equal("not_found", missing["reason"]!.GetValue<string>());
            Assert.Contains("orders-primary", missing["message"]!.GetValue<string>(), StringComparison.Ordinal);
            var ambiguous = Parse(await Tools.DisableServer(rig.Mcp, "primary", cancellationToken: ct));
            Assert.Equal("ambiguous", ambiguous["reason"]!.GetValue<string>());
            Assert.Equal("refused", Status(Parse(await Tools.DisableServer(rig.Mcp, "  ", cancellationToken: ct))));
            Assert.Equal(versionBefore, await ScalarAsync<long>(rig.Owner, "SELECT config_version FROM config_service", ct));
            Assert.Equal(2, (await ServiceViewAsync(rig, ct)).EnabledServers.Count);
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task ServerNames_ResolveLikeTheServerAdminTools_AndAnUndoHintNamesExactlyOneServer()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            /* Server 6231's NAME is the same text as server 6232's HOST; two more share one name. */
            await ExecAsync(rig.Owner, @"INSERT INTO config_monitored_servers (server_id, name, host, auth, is_enabled) VALUES
                (6231, 'web01', 'edge-a.example.test', 'integrated', TRUE),
                (6232, 'web02', 'web01', 'integrated', TRUE),
                (6233, 'twin', 'twin-a.example.test', 'integrated', TRUE),
                (6234, 'twin', 'twin-b.example.test', 'integrated', TRUE)", ct);

            /* A storage name from list_servers resolves, and the answer's undo hint resolves back to the same one server. */
            var byStorage = Parse(await Tools.DisableServer(rig.Mcp, "edge-a.example.test", cancellationToken: ct));
            Assert.Equal("changed", Status(byStorage));
            Assert.Equal("web01", byStorage["server"]!.GetValue<string>());
            var hint = byStorage["undo"]!["arguments"]!["server_name"]!.GetValue<string>();
            Assert.Equal("changed", Status(Parse(await Tools.EnableServer(rig.Mcp, hint, cancellationToken: ct))));

            /* A name that is one server's storage name and another server's display name picks neither: both are listed,
               nothing changes, and the exact single match (the other display name) still resolves. */
            var typed = Parse(await Tools.DisableServer(rig.Mcp, "web01", cancellationToken: ct));
            Assert.Equal("refused", Status(typed));
            Assert.Equal("ambiguous", typed["reason"]!.GetValue<string>());
            var typedMessage = typed["message"]!.GetValue<string>();
            Assert.Contains("web01", typedMessage, StringComparison.Ordinal);
            Assert.Contains("edge-a.example.test", typedMessage, StringComparison.Ordinal);
            Assert.Equal(4L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM config_monitored_servers WHERE is_enabled", ct));
            var byDisplay = Parse(await Tools.DisableServer(rig.Mcp, "web02", cancellationToken: ct));
            Assert.Equal("changed", Status(byDisplay));
            Assert.Equal("web01", byDisplay["storage_name"]!.GetValue<string>());
            Assert.Equal("ambiguous", Parse(await Tools.EnableServer(rig.Mcp, byDisplay["undo"]!["arguments"]!["server_name"]!.GetValue<string>(), cancellationToken: ct))["reason"]!.GetValue<string>());
            Assert.Equal("changed", Status(Parse(await Tools.EnableServer(rig.Mcp, "web02", cancellationToken: ct))));

            /* Two servers with one name: nothing is picked, both are listed by storage name, and each storage name resolves alone. */
            var ambiguous = Parse(await Tools.DisableServer(rig.Mcp, "twin", cancellationToken: ct));
            Assert.Equal("refused", Status(ambiguous));
            Assert.Equal("ambiguous", ambiguous["reason"]!.GetValue<string>());
            var message = ambiguous["message"]!.GetValue<string>();
            Assert.Contains("twin-a.example.test", message, StringComparison.Ordinal);
            Assert.Contains("twin-b.example.test", message, StringComparison.Ordinal);
            Assert.Equal(4L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM config_monitored_servers WHERE is_enabled", ct));
            Assert.Equal("changed", Status(Parse(await Tools.DisableServer(rig.Mcp, "twin-b.example.test", cancellationToken: ct))));
            Assert.Equal(1L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM config_monitored_servers WHERE NOT is_enabled AND server_id = 6234", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task SetServerExcludedDatabases_TakesEffectOnTheNextRead_AndItsUndoRestoresTheList()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 6211, "reports-north", "ARRAY['tempdb','model']", ct);
            var modifiedBefore = await ScalarAsync<string>(rig.Owner, "SELECT modified_at::text FROM config_monitored_servers WHERE server_id = 6211", ct);

            var set = Parse(await Tools.SetServerExcludedDatabases(rig.Mcp, "reports-north", ["Staging", "Archive", "staging", " "], cancellationToken: ct));
            Assert.Equal("changed", Status(set));
            Assert.Equal(new[] { "tempdb", "model" }, Strings(set["previous"]!["excluded_databases"]));
            Assert.Equal(new[] { "Staging", "Archive" }, Strings(set["current"]!["excluded_databases"]));
            Assert.NotEqual(modifiedBefore, await ScalarAsync<string>(rig.Owner, "SELECT modified_at::text FROM config_monitored_servers WHERE server_id = 6211", ct));
            Assert.Equal(new[] { "Staging", "Archive" }, (await ServiceViewAsync(rig, ct)).EnabledServers.Single().ExcludedDatabases.ToArray());

            var state = Parse(await Tools.GetCollectionState(rig.Mcp, ct));
            Assert.Equal(2, state["servers"]![0]!["excluded_databases"]!.AsArray().Count);

            /* The undo the answer carries, run as given, puts the first list back. */
            var undo = set["undo"]!;
            Assert.Equal("set_server_excluded_databases", undo["tool"]!.GetValue<string>());
            var back = Parse(await Tools.SetServerExcludedDatabases(
                rig.Mcp, undo["arguments"]!["server_name"]!.GetValue<string>(), Strings(undo["arguments"]!["databases"]), cancellationToken: ct));
            Assert.Equal("changed", Status(back));
            Assert.Equal(new[] { "tempdb", "model" }, (await ServiceViewAsync(rig, ct)).EnabledServers.Single().ExcludedDatabases.ToArray());

            /* An empty list clears; the same list again changes nothing. */
            Assert.Equal("changed", Status(Parse(await Tools.SetServerExcludedDatabases(rig.Mcp, "reports-north", [], cancellationToken: ct))));
            Assert.Empty((await ServiceViewAsync(rig, ct)).EnabledServers.Single().ExcludedDatabases);
            Assert.Equal("unchanged", Status(Parse(await Tools.SetServerExcludedDatabases(rig.Mcp, "reports-north", [], cancellationToken: ct))));

            /* Bad arguments are refused in words and write nothing. */
            await ExecAsync(rig.Owner, "UPDATE config_monitored_servers SET excluded_databases = ARRAY['keep'] WHERE server_id = 6211", ct);
            var tooMany = Parse(await Tools.SetServerExcludedDatabases(rig.Mcp, "reports-north", Enumerable.Range(0, 300).Select(i => "db" + i).ToArray(), cancellationToken: ct));
            Assert.Equal("refused", Status(tooMany));
            Assert.Contains("more than 256", tooMany["message"]!.GetValue<string>(), StringComparison.Ordinal);
            Assert.Equal("refused", Status(Parse(await Tools.SetServerExcludedDatabases(rig.Mcp, "reports-north", ["bad\u0001name"], cancellationToken: ct))));
            Assert.Equal("refused", Status(Parse(await Tools.SetServerExcludedDatabases(rig.Mcp, "reports-north", [new string('x', 129)], cancellationToken: ct))));
            Assert.Equal("refused", Status(Parse(await Tools.SetServerExcludedDatabases(rig.Mcp, "reports-north", null!, cancellationToken: ct))));
            Assert.Equal("not_found", Parse(await Tools.SetServerExcludedDatabases(rig.Mcp, "nope", ["a"], cancellationToken: ct))["reason"]!.GetValue<string>());
            Assert.Equal("keep", await ScalarAsync<string>(rig.Owner, "SELECT array_to_string(excluded_databases, ',') FROM config_monitored_servers WHERE server_id = 6211", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    [Fact]
    public async Task ReloadConfig_BumpsTheConfigVersion_AndChangesNoSetting()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 6221, "reload-east", "ARRAY['tempdb']", ct);
            var versionBefore = await ScalarAsync<long>(rig.Owner, "SELECT config_version FROM config_service", ct);
            var reloaded = Parse(await Tools.ReloadConfig(rig.Mcp, ct));
            Assert.Equal("changed", Status(reloaded));
            Assert.Equal(versionBefore, reloaded["previous"]!["config_version"]!.GetValue<long>());
            Assert.True(reloaded["current"]!["config_version"]!.GetValue<long>() > versionBefore);
            var view = await ServiceViewAsync(rig, ct);
            Assert.False(view.Paused);
            Assert.Equal(new[] { "tempdb" }, view.EnabledServers.Single().ExcludedDatabases.ToArray());
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }
}

public sealed class CollectionStateToolsPolicyTests
{
    private static readonly string[] Writes =
    [
        "pause_collection", "resume_collection", "disable_server", "enable_server", "set_server_excluded_databases", "reload_config",
    ];

    [Fact]
    public void EachWrite_IsRefusedWithoutAllowWrites_AndNeverOfferedToTheInvestigationAgent()
    {
        foreach (var tool in Writes)
        {
            Assert.False(DarlingMcpWritePolicy.Effective.Off.Offers(tool), tool);
            Assert.False(new DarlingMcpWritePolicy.Effective(Writes: false, ServerAdmin: false, Investigations: true).Offers(tool), tool);
            Assert.True(new DarlingMcpWritePolicy.Effective(Writes: true, ServerAdmin: false).Offers(tool), tool);
            Assert.Contains(tool, DarlingMcpWritePolicy.NeverAgentTools);
            Assert.False(DarlingMcpWritePolicy.IsAgentAction(tool, new HashSet<string>(Writes)), tool);
        }

        /* The read is served like the other reads. */
        Assert.True(DarlingMcpWritePolicy.Effective.Off.Offers("get_collection_state"));
    }

    [Fact]
    public void EditServer_StillRefusesEnabledAndExcludedDatabases_AndPointsAtTheNewTools()
    {
        var method = typeof(DarlingMcpServerAdminTools).GetMethods()
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == "edit_server");
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;
        Assert.Contains("set_server_excluded_databases", description, StringComparison.Ordinal);
        Assert.Contains("disable_server", description, StringComparison.Ordinal);
    }
}
