/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>The web Manage Servers grid's read (#5239), without a store: the statement's shape, the credential
/// guard, and the display-ready rows.</summary>
public sealed class AdminServersRouteTests
{
    private static readonly DateTime Now = new(2026, 3, 1, 12, 0, 0, DateTimeKind.Utc);

    private static DarlingAdminServersReader.Row Config(
        int id, string name, string host, bool enabled = true, string auth = "integrated", decimal cost = 0m,
        string? collectedName = null, string? collectedDisplay = null, DateTime? last = null, DateTime? registered = null,
        bool readOnly = false, string engine = "sqlserver", string? engineKind = null, int? sqlMajor = null, int? pgMajor = null) =>
        new(id, name, host, null, readOnly, engine, 0, auth, enabled, cost, new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Unspecified),
            collectedName, collectedDisplay, sqlMajor, pgMajor, null, engineKind, registered, last);

    /// <summary>The columns the statement reads, qualified by the config alias.</summary>
    private static string[] ConfigColumnsRead() =>
        Regex.Matches(DarlingAdminServersReader.AdminServersSql, @"\bc\.([a-z_]+)")
            .Select(m => m.Groups[1].Value).Distinct().ToArray();

    [Fact]
    public void TheStatement_ReadsTheConfigTable_WithNoEnabledFilter_AndNoDollarParameters()
    {
        var sql = DarlingAdminServersReader.AdminServersSql;
        Assert.Contains("FROM config.config_monitored_servers c", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN servers s ON s.server_id = c.server_id", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("WHERE", sql.Replace("WHERE cl.server_id", ""), StringComparison.Ordinal);
        Assert.DoesNotContain("is_enabled", sql.Replace("c.is_enabled,", ""), StringComparison.Ordinal);
        Assert.DoesNotContain("$", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheStatement_NamesNoCredentialColumn_AndNoWildcard()
    {
        var sql = DarlingAdminServersReader.AdminServersSql;
        Assert.DoesNotContain("*", sql, StringComparison.Ordinal);
        foreach (var banned in new[] { "username", "password", "encrypted", "secret", "remediation", "token", "key" })
        {
            Assert.DoesNotContain(banned, sql, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void EveryConfigColumnTheStatementReads_IsOneTheViewerRoleMayRead_AndNoneIsASecretColumn()
    {
        var acl = DarlingManagedRoles.ViewerRestrictedConfigTables.Single(t => t.Table == "config_monitored_servers");
        var read = ConfigColumnsRead();
        Assert.NotEmpty(read);
        foreach (var column in read)
        {
            Assert.Contains(column, acl.NonSecretColumns);
            Assert.DoesNotContain(column, acl.SecretColumns);
        }

        /* The four facts the grid adds must be among them. */
        foreach (var column in new[] { "auth", "is_enabled", "monthly_cost_usd", "created_at" })
        {
            Assert.Contains(column, read);
        }
    }

    [Fact]
    public void TheRoutePayload_CarriesNoCredentialKey_AndTheSecretValuesNeverAppear()
    {
        var json = DarlingAdminServersReader.Render(new[] { Config(1, "Alpha", "alpha-01", auth: "sql", cost: 10m) }, Now);
        using var doc = JsonDocument.Parse(json);
        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var row in doc.RootElement.GetProperty("servers").EnumerateArray())
        {
            foreach (var property in row.EnumerateObject())
            {
                keys.Add(property.Name);
            }
        }

        Assert.DoesNotContain(keys, k => k.Contains("username", StringComparison.OrdinalIgnoreCase)
            || k.Contains("password", StringComparison.OrdinalIgnoreCase)
            || k.Contains("encrypted", StringComparison.OrdinalIgnoreCase)
            || k.Contains("secret", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(
            new[] { "added", "auth", "display_name", "engine", "freshness", "last_collected", "monthly_cost", "monthly_cost_usd", "read_only", "server_name", "status", "version" },
            keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
    }

    [Theory]
    [InlineData("sql", "SQL Server")]
    [InlineData("SQL", "SQL Server")]
    [InlineData("integrated", "Windows")]
    [InlineData("entra-mfa", "entra-mfa")]
    [InlineData("", null)]
    [InlineData(null, null)]
    public void TheAuthMode_IsTheDesktopsWord_OrTheStoredValue(string? stored, string? expected) =>
        Assert.Equal(expected, DarlingAdminServersReader.AuthLabel(stored));

    [Theory]
    [InlineData("0", "$0")]
    [InlineData("0.4", "$0")]
    [InlineData("-5", "-$5")]
    [InlineData("0.5", "$1")]
    [InlineData("1", "$1")]
    [InlineData("1234.5", "$1,235")]
    [InlineData("2.5", "$3")]
    [InlineData("1234567", "$1,234,567")]
    public void TheMonthlyCost_IsAlwaysFormatted_DollarsWithGroupsAndHalvesRoundedAway(string cost, string expected) =>
        Assert.Equal(expected, DarlingAdminServersReader.CostLabel(decimal.Parse(cost, System.Globalization.CultureInfo.InvariantCulture)));

    [Fact]
    public void ADisabledServer_IsARow_AndSaysDisabled_WhileAnEnabledOneSaysEnabled()
    {
        var rows = DarlingAdminServersReader.Build(new[]
        {
            Config(1, "Alpha", "alpha-01", enabled: false),
            Config(2, "Bravo", "bravo-a", enabled: true),
        }, Now);
        Assert.Equal(new[] { "Disabled", "Enabled" }, rows.Select(r => r.status).ToArray());
    }

    [Fact]
    public void AConfiguredServerThatNeverConnected_ShowsItsConfigFields_AndNeverCollectedFreshness()
    {
        var row = Assert.Single(DarlingAdminServersReader.Build(
            new[] { Config(7, "Gamma", "gamma-01", readOnly: true, engine: "postgres", cost: 99m) }, Now));
        Assert.Equal("gamma-01:pg:RO", row.server_name);
        Assert.Equal("Gamma", row.display_name);
        Assert.Equal("postgres", row.engine);
        Assert.Equal("PostgreSQL", row.version);
        Assert.Equal("AwaitingFirstCollection", row.freshness);
        Assert.True(row.read_only);
        Assert.Null(row.last_collected);
        Assert.Equal("$99", row.monthly_cost);
        Assert.Equal("2026-01-02T03:04:05.0000000", row.added);
    }

    [Fact]
    public void ACollectedServer_TakesItsNameVersionAndFreshnessFromTheCollectedRow_ThroughTheListServersRules()
    {
        var fresh = Config(1, "Alpha", "alpha-01", collectedName: "alpha-01", collectedDisplay: "Alpha",
            last: Now.AddMinutes(-1), registered: Now.AddDays(-30), sqlMajor: 16, engineKind: "sqlserver");
        var dark = Config(2, "Beta", "beta-01", collectedName: "beta-01", collectedDisplay: "Beta",
            last: null, registered: Now.AddDays(-400), sqlMajor: 15, engineKind: "sqlserver");
        var rows = DarlingAdminServersReader.Build(new[] { fresh, dark }, Now);
        Assert.Equal("SQL Server 2022", rows[0].version);
        Assert.Equal(DarlingMcpDataToolsFreshness(fresh.LastCollection, fresh.RegisteredAt), rows[0].freshness);
        Assert.Equal(DarlingMcpDataToolsFreshness(null, dark.RegisteredAt), rows[1].freshness);
        Assert.Equal("Offline", rows[1].freshness);
        Assert.Equal(Now.AddMinutes(-1).ToString("o"), rows[0].last_collected);
    }

    private static string DarlingMcpDataToolsFreshness(DateTime? last, DateTime? registered) =>
        PerformanceMonitor.Darling.Service.Mcp.DarlingMcpDataTools.FreshnessStatus(last, registered, Now);

    [Fact]
    public void TheRows_AreInTotalOrder_DisplayNameIgnoringCase_ThenServerNameOrdinal_ThenServerId_WhateverTheReadOrder()
    {
        var rows = new[]
        {
            Config(1, "bravo", "alpha-02"),
            Config(2, "Bravo", "alpha-01"),
            Config(3, "Alpha", "zulu-b"),
            Config(4, "alpha", "zulu-a"),
        };
        var expected = new[] { "zulu-a", "zulu-b", "alpha-01", "alpha-02" };
        Assert.Equal(expected, DarlingAdminServersReader.Build(rows, Now).Select(r => r.server_name).ToArray());
        Assert.Equal(expected, DarlingAdminServersReader.Build(rows.Reverse().ToArray(), Now).Select(r => r.server_name).ToArray());
    }

    [Fact]
    public void ARenamedConfigRow_ShowsItsNewName_BeforeTheServerReconnects()
    {
        var renamed = Config(1, "Renamed", "alpha-01", collectedName: "alpha-01", collectedDisplay: "OldName",
            last: Now.AddMinutes(-1), registered: Now.AddDays(-30), sqlMajor: 16, engineKind: "sqlserver");
        var row = Assert.Single(DarlingAdminServersReader.Build(new[] { renamed }, Now));
        Assert.Equal("Renamed", row.display_name);
        Assert.Equal("alpha-01", row.server_name);
    }

    [Fact]
    public void TwoRowsTiedOnDisplayNameAndServerName_FallToServerId_WhateverTheReadOrder()
    {
        /* The cost carries the id so the rows can be told apart in the output. */
        var rows = new[] { Config(9, "Same", "alpha-01", cost: 9m), Config(3, "Same", "alpha-01", cost: 3m), Config(5, "Same", "alpha-01", cost: 5m) };
        var expected = new[] { 3m, 5m, 9m };
        Assert.Equal(expected, DarlingAdminServersReader.Build(rows, Now).Select(r => r.monthly_cost_usd).ToArray());
        Assert.Equal(expected, DarlingAdminServersReader.Build(rows.Reverse().ToArray(), Now).Select(r => r.monthly_cost_usd).ToArray());
    }

    [Theory]
    [InlineData(false, "GET", "/api/admin/servers", true)]
    [InlineData(false, "HEAD", "/api/admin/servers", true)]
    [InlineData(false, "POST", "/api/admin/servers", false)]
    [InlineData(false, "PUT", "/api/admin/servers", false)]
    [InlineData(false, "PATCH", "/api/admin/servers", false)]
    [InlineData(false, "DELETE", "/api/admin/servers", false)]
    [InlineData(true, "GET", "/api/admin/servers", true)]
    public void TheMethodGate_AllowsOnlyGet_OnTheAdminServersPath_ForReadOnlyAndEditingSignIns(bool canEdit, string method, string path, bool expected) =>
        Assert.Equal(expected, PerformanceMonitor.Darling.Service.Hosting.DarlingWebSeat.IsRequestAllowed(
            new PerformanceMonitor.Darling.Service.Hosting.DarlingWebSeat("who", canEdit), method, path));

    [Fact]
    public void TheRoute_IsMappedByMapAll_BesideTheTagRoutes_AndOnlyAsAGet()
    {
        var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs").ReplaceLineEndings("\n");
        Assert.Contains("MapServerTags(app, postgres, logger);\n        MapAdminServers(app, postgres);", source);
        Assert.Contains("app.MapGet(DarlingAdminServersReader.Route,", source);
        Assert.DoesNotContain("MapPost(DarlingAdminServersReader.Route", source);
        Assert.Equal("/api/admin/servers", DarlingAdminServersReader.Route);
    }
}
