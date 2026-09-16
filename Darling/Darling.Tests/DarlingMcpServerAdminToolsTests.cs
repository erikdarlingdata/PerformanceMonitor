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
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Ungated (no-live-store) contract for the server-onboarding MCP tools (add_servers / remove_server): the tool
/// surface is EXACTLY the two names (both static, on a [McpServerToolType] class, returning Task&lt;string&gt;),
/// the advertised tools/list schema is Gemini-clean (#1074) with the expected required-param set, and — the
/// load-bearing safety property — a structurally-bad request (malformed JSON, empty array, an entry with a bad
/// field or an unsupported Entra/MFA auth) is REJECTED as invalid WITHOUT ever probing a server or opening a store
/// connection (proved against a dead data source + a probe seam that throws if reached). The pure dedupe partition
/// (case-folded, first-occurrence-wins) is unit-tested directly. The live store INSERT/DELETE + config_version
/// bump round-trip (probe stubbed to success) is gated below.
/// </summary>
public sealed class DarlingMcpServerAdminToolsSurfaceTests
{
    /// <summary>A dead data source (unroutable port) — proves the validate-before-write path bails on a bad request
    /// WITHOUT ever opening a connection (the call returns before touching the store).</summary>
    private const string DeadStore = "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    /// <summary>A probe seam that FAILS the test if the pure-validation path ever reaches it — so a bad request
    /// that returns before probing is proved to have skipped the network hit entirely.</summary>
    private static readonly DarlingMcpServerAdminTools.ServerProbe ThrowingProbe =
        (_, _) => throw new InvalidOperationException("the probe must not run for a structurally-invalid request");

    private static readonly string[] ExpectedToolSurface =
    {
        "add_servers",
        "remove_server",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpServerAdminTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_IsExactlyTheTwoServerAdminTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(ExpectedToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpServerAdminTools).GetCustomAttribute<McpServerToolTypeAttribute>());
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
    [InlineData("add_servers", "servers_json")]
    [InlineData("remove_server", "server_name")]
    public void ParamContract_MatchesContract(string toolName, string expectedCsv)
    {
        Assert.Equal(expectedCsv.Split(','), McpParams(toolName).Select(p => p.Name).ToArray());
    }

    [Theory]
    [InlineData("add_servers", "servers_json")]
    [InlineData("remove_server", "server_name")]
    public void ParamContract_BothTools_RequireTheirTarget(string toolName, string requiredCsv)
    {
        var required = McpParams(toolName).Where(p => !p.Optional).Select(p => p.Name)
            .OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal(requiredCsv.Split(',').OrderBy(n => n, StringComparer.Ordinal).ToArray(), required);
    }

    private static System.Collections.Generic.Dictionary<string, ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpServerAdminTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().ToDictionary(t => t.ProtocolTool.Name, t => t.ProtocolTool);
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_ForBothTools()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(2, tools.Count);
        var violations = tools.Values.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
    }

    [Theory]
    [InlineData("add_servers", "servers_json")]
    [InlineData("remove_server", "server_name")]
    public void AdvertisedSchema_RequiredParams_MatchTheContract(string toolName, string expectedCsv)
    {
        var expected = expectedCsv.Length == 0 ? Array.Empty<string>() : expectedCsv.Split(',');
        var required = DarlingMcpSchemaAssert.RequiredOf(BuildToolSchemas()[toolName].InputSchema)
            .OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal(expected.OrderBy(n => n, StringComparer.Ordinal).ToArray(), required);
    }

    /* ---------------- validate BEFORE write (no connection opened, no probe) ---------------- */

    [Theory]
    [InlineData("not json")]     // not valid JSON
    [InlineData("{\"host\":\"x\"}")] // a JSON object, not an array
    [InlineData("[]")]           // empty array
    public async Task AddServers_UnusablePayload_ReturnsInvalid_WithoutTouchingStoreOrProbe(string json)
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpServerAdminTools.AddServersAsync(dead, json, ThrowingProbe, CancellationToken.None);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
    }

    [Theory]
    [InlineData("[{\"database\":\"x\"}]")]                                        // missing host
    [InlineData("[{\"host\":\"x\",\"auth\":\"Entra\"}]")]                         // bare/interactive Entra rejected
    [InlineData("[{\"host\":\"x\",\"auth\":\"EntraMFA\"}]")]                      // interactive MFA rejected (#3484)
    [InlineData("[{\"host\":\"x\",\"auth\":\"DeviceCode\"}]")]                    // interactive device-code rejected (#3484)
    [InlineData("[{\"host\":\"x\",\"auth\":\"ServicePrincipal\"}]")]             // SP without client id/secret (#3484)
    [InlineData("[{\"host\":\"x\",\"auth\":\"ServicePrincipal\",\"username\":\"app\"}]")] // SP without secret (#3484)
    [InlineData("[{\"host\":\"x\",\"auth\":\"SQL\"}]")]                          // SQL without username/password
    [InlineData("[{\"host\":\"x\",\"auth\":\"SQL\",\"username\":\"u\"}]")]       // SQL without password
    [InlineData("[{\"host\":\"x\",\"encrypt_mode\":\"nope\"}]")]                 // bad encrypt_mode enum
    [InlineData("[{\"host\":\"x\",\"trust_server_certificate\":\"yes\"}]")]      // bool field, wrong type
    public async Task AddServers_AllEntriesInvalid_ReturnsPerEntryInvalid_WithoutTouchingStoreOrProbe(string json)
    {
        /* Every entry is structurally invalid, so there is no candidate to dedupe / probe / insert — the store is
           never opened (the dead store would throw) and the throwing probe is never reached. */
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpServerAdminTools.AddServersAsync(dead, json, ThrowingProbe, CancellationToken.None);

        using var doc = JsonDocument.Parse(result);
        Assert.Equal(0, doc.RootElement.GetProperty("added").GetInt32());
        Assert.Equal(0, doc.RootElement.GetProperty("skipped").GetInt32());
        Assert.Equal(1, doc.RootElement.GetProperty("failed").GetInt32());
        Assert.Equal("invalid", doc.RootElement.GetProperty("results")[0].GetProperty("status").GetString());
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public async Task RemoveServer_BlankName_ReturnsInvalid_WithoutTouchingStore(string name)
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpServerAdminTools.RemoveServer(dead, name);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
    }

    /* ---------------- pure parse + dedupe ---------------- */

    [Fact]
    public void ParseRequest_ValidWindowsEntry_BuildsIntegratedProbeConfig_NoSecret()
    {
        var (entries, invalid, wholeError) = DarlingMcpServerAdminTools.ParseRequest("[{\"host\":\"sql01\"}]");

        Assert.Null(wholeError);
        Assert.Empty(invalid);
        var entry = Assert.Single(entries);
        Assert.Equal("sql01", entry.DisplayName);
        Assert.Equal("integrated", entry.ProbeConfig.Auth);
        Assert.Null(entry.PlaintextPassword);
        /* Fail-closed TLS defaults are the MonitoredServer defaults. */
        Assert.Equal("Mandatory", entry.ProbeConfig.EncryptMode);
        Assert.False(entry.ProbeConfig.TrustServerCertificate);
    }

    [Fact]
    public void ParseRequest_SqlEntry_CarriesPlaintextForProbe_AndAllExposedOptions()
    {
        var (entries, invalid, wholeError) = DarlingMcpServerAdminTools.ParseRequest(
            "[{\"host\":\"sql02\",\"display_name\":\"Prod\",\"database\":\"AppDb\",\"auth\":\"SQL\",\"username\":\"monitor\"," +
            "\"password\":\"p@ss\",\"encrypt_mode\":\"Strict\",\"trust_server_certificate\":true,\"read_only_intent\":true}]");

        Assert.Null(wholeError);
        Assert.Empty(invalid);
        var entry = Assert.Single(entries);
        Assert.Equal("Prod", entry.DisplayName);
        Assert.Equal("sql", entry.ProbeConfig.Auth);
        Assert.Equal("monitor", entry.ProbeConfig.Username);
        Assert.Equal("p@ss", entry.PlaintextPassword);
        Assert.Equal("AppDb", entry.ProbeConfig.Database);
        Assert.Equal("Strict", entry.ProbeConfig.EncryptMode);
        Assert.True(entry.ProbeConfig.TrustServerCertificate);
        Assert.True(entry.ProbeConfig.ReadOnlyIntent);
        /* read_only_intent flows into the storage identity key (host:RO), matching the shared identity rule. */
        Assert.Equal(ServerIdHelper.BuildStorageName("sql02", "AppDb", true), entry.StorageKey);
    }

    [Fact]
    public void ParseRequest_ServicePrincipalEntry_CarriesClientIdAndSecret_ForProbe()
    {
        /* #3484: a non-interactive Entra service principal — the application/client id in username, the client
           secret in password, DPAPI-encrypted after a successful probe exactly like a SQL password. */
        var (entries, invalid, wholeError) = DarlingMcpServerAdminTools.ParseRequest(
            "[{\"host\":\"azuredb.database.windows.net\",\"database\":\"AppDb\",\"auth\":\"ServicePrincipal\"," +
            "\"username\":\"11111111-2222-3333-4444-555555555555\",\"password\":\"the-client-secret\"}]");

        Assert.Null(wholeError);
        Assert.Empty(invalid);
        var entry = Assert.Single(entries);
        Assert.Equal("serviceprincipal", entry.ProbeConfig.Auth);
        Assert.True(entry.ProbeConfig.UsesServicePrincipal);
        Assert.Equal("11111111-2222-3333-4444-555555555555", entry.ProbeConfig.Username);
        Assert.Equal("the-client-secret", entry.PlaintextPassword);
    }

    [Fact]
    public void ParseRequest_ManagedIdentityEntry_CarriesNoSecret_OptionalUserAssignedClientId()
    {
        /* #3484: managed identity is secret-less. A user-assigned identity names its client id in username; a
           system-assigned identity omits it. Either way no secret is carried or stored. */
        var (entries, invalid, wholeError) = DarlingMcpServerAdminTools.ParseRequest(
            "[{\"host\":\"azuredb.database.windows.net\",\"auth\":\"ManagedIdentity\"," +
            "\"username\":\"66666666-7777-8888-9999-000000000000\"}]");

        Assert.Null(wholeError);
        Assert.Empty(invalid);
        var entry = Assert.Single(entries);
        Assert.Equal("managedidentity", entry.ProbeConfig.Auth);
        Assert.True(entry.ProbeConfig.UsesManagedIdentity);
        Assert.Equal("66666666-7777-8888-9999-000000000000", entry.ProbeConfig.Username);
        Assert.Null(entry.PlaintextPassword);
    }

    [Fact]
    public void ParseRequest_MixedBatch_SplitsValidFromInvalid_PreservingOrder()
    {
        var (entries, invalid, wholeError) = DarlingMcpServerAdminTools.ParseRequest(
            "[{\"host\":\"good1\"},{\"auth\":\"SQL\"},{\"host\":\"good2\"}]");

        Assert.Null(wholeError);
        Assert.Equal(2, entries.Count);
        var bad = Assert.Single(invalid);
        Assert.Equal("invalid", bad.Status);
        Assert.Equal(1, bad.Order); // the second element (index 1)
    }

    [Fact]
    public void PartitionDuplicates_CaseVariantEntries_CollapseToOneAdded_OneDuplicate()
    {
        /* "SQL2016" and "sql2016" build the SAME storage identity under OrdinalIgnoreCase (BuildStorageName is
           case-preserving; the gate is case-folded), so the second is a duplicate of the first in-batch. */
        var (entries, _, _) = DarlingMcpServerAdminTools.ParseRequest("[{\"host\":\"SQL2016\"},{\"host\":\"sql2016\"}]");
        Assert.Equal(2, entries.Count);
        Assert.Equal(entries[0].StorageKey, entries[1].StorageKey, StringComparer.OrdinalIgnoreCase);

        var (ready, duplicates) = DarlingMcpServerAdminTools.PartitionDuplicates(entries, Array.Empty<string>());

        Assert.Single(ready);
        var dup = Assert.Single(duplicates);
        Assert.Equal("duplicate", dup.Status);
        Assert.Equal(1, dup.Order); // the second entry is the duplicate
    }

    [Fact]
    public void PartitionDuplicates_EntryMatchingAnExistingServer_IsSkipped()
    {
        var (entries, _, _) = DarlingMcpServerAdminTools.ParseRequest("[{\"host\":\"sql2019\"}]");

        /* Seed the gate with the SAME server (a case variant of the existing store key) → no ready, one duplicate. */
        var (ready, duplicates) = DarlingMcpServerAdminTools.PartitionDuplicates(entries, new[] { "SQL2019" });

        Assert.Empty(ready);
        Assert.Single(duplicates);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip for the server-onboarding MCP tools against a real PostgreSQL — the
/// probe is STUBBED to success (no live SQL Server), so this exercises the STORE side: add_servers INSERTs the
/// config_monitored_servers rows (SQL-auth password DPAPI-encrypted at rest, Windows-auth server secret-free),
/// the case-folded duplicate is skipped, the write self-bumps config_version (the reload beacon) via the existing
/// trigger, and remove_server resolves + DELETEs. Own-scoped per the shared-store doctrine (GUID-suffixed hosts +
/// a finally cleanup). No live SQL Server connection is made in CI — the real end-to-end probe is what the human
/// dogfoods (remove sql2016, re-add via MCP).
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpServerAdminToolsLivePostgresTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The stubbed probe — a healthy on-prem Enterprise box; no live SQL Server is touched.</summary>
    private static readonly DarlingMcpServerAdminTools.ServerProbe SuccessProbe =
        (_, _) => Task.FromResult(new ConnectionProbeResult(
            Success: true, MajorVersion: 15, EngineEdition: 3, EngineEditionDescription: "Enterprise",
            IsAzureSqlDb: false, IsAzureManagedInstance: false, IsAwsRds: false, HasMsdbAccess: true, Error: null));

    [Fact]
    public async Task AddServers_InsertsEncryptsDedupesBumpsVersion_ThenRemoveServer_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the server-admin MCP tools live test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Explicit search_path so the tools' bare config_monitored_servers / servers resolve regardless of the
           store's database default (mirrors the other live tool tests). */
        var dataSourceConnectionString = new NpgsqlConnectionStringBuilder(cs)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString;
        await using var postgres = NpgsqlDataSource.Create(dataSourceConnectionString);

        var suffix = Guid.NewGuid().ToString("N")[..12];
        var sqlHost = "mcp-add-sql-" + suffix;
        var winHost = "mcp-add-win-" + suffix;
        var sqlId = ServerIdHelper.GetDeterministicHashCode(ServerIdHelper.BuildStorageName(sqlHost, null, false));
        var winId = ServerIdHelper.GetDeterministicHashCode(ServerIdHelper.BuildStorageName(winHost, null, false));
        var password = "P@ss-" + Guid.NewGuid().ToString("N");

        await CleanupAsync(connection, ct, sqlId, winId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING");

        var bodySucceeded = false;
        try
        {
            var versionBefore = Convert.ToInt64(await ScalarAsync(connection, ct, "SELECT config_version FROM config_service WHERE id = 1"));

            /* SQL-auth server + Windows-auth server + a case-variant DUPLICATE of the Windows host. */
            var json =
                $"[{{\"host\":\"{sqlHost}\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"{password}\"," +
                $"\"encrypt_mode\":\"Strict\",\"trust_server_certificate\":true}}," +
                $"{{\"host\":\"{winHost}\"}}," +
                $"{{\"host\":\"{winHost.ToUpperInvariant()}\"}}]";
            var added = await DarlingMcpServerAdminTools.AddServersAsync(postgres, json, SuccessProbe, ct);
            using (var doc = JsonDocument.Parse(added))
            {
                Assert.Equal(2, doc.RootElement.GetProperty("added").GetInt32());
                Assert.Equal(1, doc.RootElement.GetProperty("skipped").GetInt32());
                Assert.Equal(0, doc.RootElement.GetProperty("failed").GetInt32());
            }

            /* The config_monitored_servers write self-bumped config_version (the service's reload beacon) via the
               existing trg_bump_monitored_servers trigger — proving the mcp beacon column-grant covers this write. */
            var versionAfter = Convert.ToInt64(await ScalarAsync(connection, ct, "SELECT config_version FROM config_service WHERE id = 1"));
            Assert.True(versionAfter > versionBefore, "config_version should self-bump on a config_monitored_servers write");

            /* SQL server: the password is DPAPI-ENCRYPTED at rest (not plaintext) and round-trips; the exposed TLS
               options + auth landed as sent. (Darling.Tests is net10.0-windows, so DPAPI is available here.) */
            var storedSecret = await ScalarAsync(connection, ct, $"SELECT encrypted_password FROM config_monitored_servers WHERE server_id = {sqlId}") as string;
            Assert.False(string.IsNullOrEmpty(storedSecret));
            Assert.NotEqual(password, storedSecret);
            Assert.Equal(password, DarlingSecrets.Unprotect(storedSecret!));
            Assert.Equal("sql", await ScalarAsync(connection, ct, $"SELECT auth FROM config_monitored_servers WHERE server_id = {sqlId}") as string);
            Assert.Equal("Strict", await ScalarAsync(connection, ct, $"SELECT encrypt_mode FROM config_monitored_servers WHERE server_id = {sqlId}") as string);
            Assert.True((bool)(await ScalarAsync(connection, ct, $"SELECT trust_server_certificate FROM config_monitored_servers WHERE server_id = {sqlId}"))!);

            /* Windows server: integrated auth, NO stored secret. */
            Assert.Equal("integrated", await ScalarAsync(connection, ct, $"SELECT auth FROM config_monitored_servers WHERE server_id = {winId}") as string);
            Assert.True(await ScalarAsync(connection, ct, $"SELECT encrypted_password FROM config_monitored_servers WHERE server_id = {winId}") is null or DBNull);

            /* Re-adding both existing servers → all duplicates, nothing added. */
            var again = await DarlingMcpServerAdminTools.AddServersAsync(
                postgres, $"[{{\"host\":\"{sqlHost}\"}},{{\"host\":\"{winHost}\"}}]", SuccessProbe, ct);
            using (var doc = JsonDocument.Parse(again))
            {
                Assert.Equal(0, doc.RootElement.GetProperty("added").GetInt32());
                Assert.Equal(2, doc.RootElement.GetProperty("skipped").GetInt32());
            }

            /* remove_server resolves against the servers registry, so register the SQL host there (same server_id
               config_monitored_servers keys on), then remove — the config row is deleted. */
            await DarlingMcpTestData.RegisterServerAsync(connection, sqlId, sqlHost, ct);
            Assert.Equal("removed", DarlingMcpTestData.StatusOf(await DarlingMcpServerAdminTools.RemoveServer(postgres, sqlHost)));
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(connection, ct, $"SELECT count(*) FROM config_monitored_servers WHERE server_id = {sqlId}")));

            /* Remove again: the servers-registry row still resolves, but the config row is gone → not_found. */
            Assert.Equal("not_found", DarlingMcpTestData.StatusOf(await DarlingMcpServerAdminTools.RemoveServer(postgres, sqlHost)));

            /* A name that does not resolve at all → not_found. */
            Assert.Equal("not_found", DarlingMcpTestData.StatusOf(await DarlingMcpServerAdminTools.RemoveServer(postgres, "no-such-server-" + suffix)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt, sqlId, winId));
        }
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, CancellationToken ct, string sql)
    {
        using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct, int sqlId, int winId)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM config_monitored_servers WHERE server_id IN ({sqlId}, {winId})");
        await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM servers WHERE server_id IN ({sqlId}, {winId})");
    }
}
