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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The rules the MCP <c>add_servers</c> tool applies to its caller (<see cref="DarlingMcpAddServersPolicy"/>).
/// Everything runs without a store: a refused entry is reported before the store is opened or a server probed,
/// which the dead data source and the throwing probe prove, and the acceptance cases run on the pure screen.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class DarlingMcpAddServersPolicyTests
{
    /// <summary>A data source nothing listens behind: a refused batch must return without ever using it.</summary>
    private const string DeadStore = "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    /* A ready password key, so a SQL entry in the screen cases is not refused for a literal password. */
    private static readonly PasswordKeyStatus HealthyKey = new(true, null, "0123456789abcdef", null);

    private const string Setting = "mcp.allowWindowsAuthOverUnverifiedTls";

    private static readonly DarlingMcpServerAdminTools.ServerProbe ThrowingProbe =
        (_, _) => throw new InvalidOperationException("the probe must not run for a refused entry");

    private static readonly DarlingMcpAddServersPolicy SettingOff = new(allowWindowsAuthOverUnverifiedTls: false);
    private static readonly DarlingMcpAddServersPolicy SettingOn = new(allowWindowsAuthOverUnverifiedTls: true);

    private const string WindowsTrust = "[{\"host\":\"sql01\",\"auth\":\"Windows\",\"trust_server_certificate\":true}]";
    private const string WindowsDefaultAuthTrust = "[{\"host\":\"sql01\",\"trust_server_certificate\":true}]";
    private const string WindowsOptional = "[{\"host\":\"sql01\",\"auth\":\"Windows\",\"encrypt_mode\":\"Optional\"}]";
    private const string WindowsTrustAndOptional = "[{\"host\":\"sql01\",\"trust_server_certificate\":true,\"encrypt_mode\":\"optional\"}]";

    /* Not refused by default: the verified default, the strict mode, and every mode that is not Windows. */
    private const string WindowsDefaults = "[{\"host\":\"sql01\"}]";
    private const string WindowsMandatory = "[{\"host\":\"sql01\",\"auth\":\"Windows\",\"encrypt_mode\":\"Mandatory\",\"trust_server_certificate\":false}]";
    private const string WindowsStrict = "[{\"host\":\"sql01\",\"auth\":\"Windows\",\"encrypt_mode\":\"Strict\"}]";
    private const string SqlTrust = "[{\"host\":\"sql01\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"x\",\"trust_server_certificate\":true}]";
    private const string SqlOptional = "[{\"host\":\"sql01\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"x\",\"encrypt_mode\":\"Optional\"}]";
    private const string ManagedIdentityTrust = "[{\"host\":\"sql01\",\"auth\":\"ManagedIdentity\",\"trust_server_certificate\":true}]";
    private const string ServicePrincipalOptional = "[{\"host\":\"sql01\",\"auth\":\"ServicePrincipal\",\"username\":\"app\",\"password\":\"x\",\"encrypt_mode\":\"Optional\"}]";
    private const string PostgresSqlTrust = "[{\"host\":\"pg01\",\"engine\":\"postgres\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"x\",\"trust_server_certificate\":true}]";

    [Theory]
    [InlineData(WindowsTrust)]
    [InlineData(WindowsDefaultAuthTrust)]
    [InlineData(WindowsOptional)]
    [InlineData(WindowsTrustAndOptional)]
    public async Task WindowsAuth_OverAnUnverifiedConnection_IsRefusedByDefault_WithoutTouchingTheStoreOrProbe(string json)
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);

        var result = await DarlingMcpServerAdminTools.AddServersAsync(
            dead, json, ThrowingProbe, CancellationToken.None, policy: SettingOff);

        using var doc = JsonDocument.Parse(result);
        Assert.Equal(1, doc.RootElement.GetProperty("requested").GetInt32());
        Assert.Equal(0, doc.RootElement.GetProperty("added").GetInt32());
        Assert.Equal(1, doc.RootElement.GetProperty("failed").GetInt32());
        var row = doc.RootElement.GetProperty("results")[0];
        Assert.Equal("sql01", row.GetProperty("server").GetString());
        Assert.Equal("invalid", row.GetProperty("status").GetString());
        Assert.Contains(Setting, row.GetProperty("detail").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheRefusal_NamesWhatWasUnverified_AndTheWayOut()
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);

        var result = await DarlingMcpServerAdminTools.AddServersAsync(
            dead, WindowsTrustAndOptional, ThrowingProbe, CancellationToken.None, policy: SettingOff);

        using var doc = JsonDocument.Parse(result);
        var detail = doc.RootElement.GetProperty("results")[0].GetProperty("detail").GetString()!;
        Assert.Contains("trust_server_certificate is true", detail, StringComparison.Ordinal);
        Assert.Contains("encrypt_mode is Optional", detail, StringComparison.Ordinal);
        Assert.Contains("SQL", detail, StringComparison.Ordinal);
        Assert.Contains("Mandatory", detail, StringComparison.Ordinal);
        Assert.Contains("darling.json", detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARefusedEntry_DoesNotStopTheRestOfTheBatch_AndKeepsItsPlace()
    {
        /* The second entry is refused, the first is structurally invalid: neither is a candidate, so the store is
           never opened, and the results come back in input order with the refusal counted as failed. */
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        const string json =
            "[{\"database\":\"x\"},{\"host\":\"sql02\",\"auth\":\"Windows\",\"trust_server_certificate\":true}]";

        var result = await DarlingMcpServerAdminTools.AddServersAsync(
            dead, json, ThrowingProbe, CancellationToken.None, policy: SettingOff);

        using var doc = JsonDocument.Parse(result);
        Assert.Equal(2, doc.RootElement.GetProperty("requested").GetInt32());
        Assert.Equal(2, doc.RootElement.GetProperty("failed").GetInt32());
        Assert.Equal("(entry 1)", doc.RootElement.GetProperty("results")[0].GetProperty("server").GetString());
        Assert.Equal("sql02", doc.RootElement.GetProperty("results")[1].GetProperty("server").GetString());
        Assert.Contains(Setting, doc.RootElement.GetProperty("results")[1].GetProperty("detail").GetString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(WindowsTrust, true)]
    [InlineData(WindowsDefaultAuthTrust, true)]
    [InlineData(WindowsOptional, true)]
    [InlineData(WindowsTrustAndOptional, true)]
    [InlineData(WindowsDefaults, false)]
    [InlineData(WindowsMandatory, false)]
    [InlineData(WindowsStrict, false)]
    [InlineData(SqlTrust, false)]
    [InlineData(SqlOptional, false)]
    [InlineData(ManagedIdentityTrust, false)]
    [InlineData(ServicePrincipalOptional, false)]
    [InlineData(PostgresSqlTrust, false)]
    public void TheScreen_RefusesOnlyWindowsAuth_OverAnUnverifiedConnection_ByDefault(string json, bool refusedByDefault)
    {
        var (entries, invalid, wholeError) = DarlingMcpServerAdminTools.ParseRequest(json, HealthyKey);
        Assert.Null(wholeError);
        Assert.Empty(invalid);
        Assert.Single(entries);

        var results = new List<DarlingMcpServerAdminTools.ServerResult>();
        var allowed = DarlingMcpAddServersPolicy.Screen(SettingOff, entries, results);

        Assert.Equal(refusedByDefault ? 0 : 1, allowed.Count);
        Assert.Equal(refusedByDefault ? 1 : 0, results.Count);
        if (refusedByDefault)
        {
            Assert.Equal(DarlingMcpServerAdminTools.AddStatus.Invalid, results[0].Status);
            Assert.Contains(Setting, results[0].Detail, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData(WindowsTrust)]
    [InlineData(WindowsDefaultAuthTrust)]
    [InlineData(WindowsOptional)]
    [InlineData(WindowsTrustAndOptional)]
    public void WithTheSettingOn_TheSameEntriesAreAccepted(string json)
    {
        var (entries, _, _) = DarlingMcpServerAdminTools.ParseRequest(json, HealthyKey);

        var results = new List<DarlingMcpServerAdminTools.ServerResult>();
        var allowed = DarlingMcpAddServersPolicy.Screen(SettingOn, entries, results);

        Assert.Single(allowed);
        Assert.Empty(results);
    }

    [Theory]
    [InlineData(WindowsTrust)]
    [InlineData(WindowsOptional)]
    public void WithNoPolicy_TheOperatorsOwnCommandIsNotGated(string json)
    {
        /* --add-server calls the onboarding core with no policy; the setting is about the MCP caller only. */
        var (entries, _, _) = DarlingMcpServerAdminTools.ParseRequest(json, HealthyKey);

        var results = new List<DarlingMcpServerAdminTools.ServerResult>();
        var allowed = DarlingMcpAddServersPolicy.Screen(policy: null, entries, results);

        Assert.Single(allowed);
        Assert.Empty(results);
    }

    [Fact]
    public void TheDefaultPolicy_HasEverySettingOff()
    {
        var (entries, _, _) = DarlingMcpServerAdminTools.ParseRequest(WindowsTrust, HealthyKey);

        var results = new List<DarlingMcpServerAdminTools.ServerResult>();
        var allowed = DarlingMcpAddServersPolicy.Screen(DarlingMcpAddServersPolicy.Default, entries, results);

        Assert.Empty(allowed);
        Assert.Single(results);
    }

    [Fact]
    public void McpConfig_Setting_DefaultsOff_AndParsesFromDarlingJson()
    {
        var omitted = DarlingConfig.Parse(@"{ ""postgres"": { ""managed"": true }, ""servers"": [ { ""host"": ""SQL2022"" } ], ""mcp"": { ""enabled"": true } }");
        Assert.False(omitted.Mcp.AllowWindowsAuthOverUnverifiedTls);

        var set = DarlingConfig.Parse(@"{ ""postgres"": { ""managed"": true }, ""servers"": [ { ""host"": ""SQL2022"" } ], ""mcp"": { ""enabled"": true, ""allowWindowsAuthOverUnverifiedTls"": true } }");
        Assert.True(set.Mcp.AllowWindowsAuthOverUnverifiedTls);
    }

    [Fact]
    public void TheSettingName_IsTheDarlingJsonKeyUnderMcp()
    {
        Assert.Equal("mcp.allowWindowsAuthOverUnverifiedTls", DarlingMcpAddServersPolicy.UnverifiedWindowsAuthSetting);
        Assert.Equal(
            "allowWindowsAuthOverUnverifiedTls",
            typeof(McpConfig).GetProperty(nameof(McpConfig.AllowWindowsAuthOverUnverifiedTls))!
                .GetCustomAttributes(typeof(System.Text.Json.Serialization.JsonPropertyNameAttribute), inherit: false)
                .Cast<System.Text.Json.Serialization.JsonPropertyNameAttribute>().Single().Name);
    }

    /* ---------------- wiring ---------------- */

    [Fact]
    public void TheScreen_RunsBeforeTheStoreIsOpened()
    {
        var source = File.ReadAllText(
            RepoPath("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpServerAdminTools.cs"));

        Assert.Contains("DarlingMcpAddServersPolicy.Screen(policy, entries, results)", source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheHost_BuildsThePolicyFromTheConfiguredSetting()
    {
        var source = File.ReadAllText(
            RepoPath("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs"));

        Assert.Contains(
            "new DarlingMcpAddServersPolicy(config.Mcp.AllowWindowsAuthOverUnverifiedTls)", source, StringComparison.Ordinal);
    }

    private static string RepoPath(params string[] segments) => Path.Combine(new[] { RepoRoot() }.Concat(segments).ToArray());

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}
