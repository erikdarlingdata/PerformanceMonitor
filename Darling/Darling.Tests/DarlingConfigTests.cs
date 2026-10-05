/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

using PerformanceMonitor.Darling.Service.Mcp;

namespace Darling.Tests;

/// <summary>
/// Pins the Darling config plane (M2 slice B): the sample file parses (comments allowed),
/// validation catches the real misconfigurations, the connection string mirrors Lite's posture
/// (MARS, 15s connect, Encrypt fail-closed to Mandatory), and DPAPI password protection
/// round-trips with the blob preferred over plaintext.
/// </summary>
public sealed class DarlingConfigTests
{
    private static MonitoredServer Server(Action<MonitoredServer>? mutate = null)
    {
        var server = new MonitoredServer { Name = "S1", Host = "SQL2022" };
        mutate?.Invoke(server);
        return server;
    }

    private static DarlingConfig ValidConfig(Action<DarlingConfig>? mutate = null)
    {
        var config = new DarlingConfig
        {
            Postgres = new PostgresConfig { ConnectionString = "Host=localhost;Database=darling" },
            Servers = { Server() },
        };
        mutate?.Invoke(config);
        return config;
    }

    [Fact]
    public void SampleConfig_ParsesWithComments()
    {
        var samplePath = Path.Combine(AppContext.BaseDirectory, "darling.sample.json");
        var config = DarlingConfig.Parse(File.ReadAllText(samplePath));

        Assert.Equal(2, config.Servers.Count);
        Assert.False(config.Servers[0].UsesSqlAuth);
        Assert.True(config.Servers[1].UsesSqlAuth);
        Assert.Equal("monitor", config.Servers[1].Username);
        Assert.Equal(new[] { "StageDb" }, config.Servers[1].ExcludedDatabases);

        /* The sample ships the MANAGED bundled-Postgres mode — the zero-admin default: the
           connection string is derived at runtime, never set, and the whole sample validates
           clean as shipped. */
        Assert.True(config.Postgres.Managed);
        Assert.Equal(5641, config.Postgres.Port);
        Assert.Null(config.Postgres.DataDirectory);
        Assert.Equal("", config.Postgres.ConnectionString);
        Assert.Empty(config.Validate());

        /* Plan capture is on by default and the sample documents it explicitly (PG TOAST). */
        Assert.True(config.CapturePlans);

        /* Object-DDL / schema-change collection is on by default and the sample documents the noisy-box opt-out. */
        Assert.True(config.CollectSchemaChangeEvents);

        /* Phase-5 slice D: the alert/delivery sections parse; the sample documents shape without
           enabling delivery (empty SMTP host/from, empty webhook URLs). */
        Assert.True(config.Alerts.Enabled);
        Assert.Equal(80, config.Alerts.CpuThresholdPercent);
        Assert.Equal("total", config.Alerts.CpuMode);
        /* #1141/#1236: the sample documents the delivery-mode knobs (global default Summary / 5, and a
           per-server override that inherits the global when null). */
        Assert.Equal(AlertNotificationMode.Summary, config.Alerts.DeliveryMode);
        Assert.Equal(5, config.Alerts.PerEventMax);
        Assert.Null(config.Servers[0].AlertDeliveryModeOverride);
        Assert.Equal(587, config.Smtp.Port);
        Assert.Equal("", config.Smtp.Host);
        Assert.Equal("dba-team@example.com", config.Smtp.To);
        Assert.Equal("", config.Webhooks.TeamsUrl);
        Assert.Equal("", config.Webhooks.SlackUrl);
    }

    [Fact]
    public void DeliveryMode_DefaultsSummary_ParsesEnumNames_AndPerServerOverride()
    {
        /* Defaults mirror the V18 DDL: global Summary / 5, per-server override null (inherit). */
        var config = new DarlingConfig();
        Assert.Equal(AlertNotificationMode.Summary, config.Alerts.DeliveryMode);
        Assert.Equal(5, config.Alerts.PerEventMax);

        /* darling.json carries the enum by NAME (JsonStringEnumConverter), and the per-server override
           is nullable ("PerEvent" on one server, absent/null on another = inherit). */
        var parsed = DarlingConfig.Parse(@"{
            ""postgres"": { ""managed"": true },
            ""servers"": [
                { ""host"": ""noisy"", ""alertDeliveryModeOverride"": ""PerEvent"" },
                { ""host"": ""quiet"" }
            ],
            ""alerts"": { ""deliveryMode"": ""PerEvent"", ""perEventMax"": 3 }
        }");
        Assert.Equal(AlertNotificationMode.PerEvent, parsed.Alerts.DeliveryMode);
        Assert.Equal(3, parsed.Alerts.PerEventMax);
        Assert.Equal(AlertNotificationMode.PerEvent, parsed.Servers[0].AlertDeliveryModeOverride);
        Assert.Null(parsed.Servers[1].AlertDeliveryModeOverride);
    }

    [Fact]
    public void CapturePlans_DefaultsTrue_AndParsesOverride()
    {
        /* Default true: Darling captures execution plans (PG TOAST compresses them transparently),
           unlike Lite. A config with no "capturePlans" key keeps the default; explicit false turns
           capture off (e.g. to shave storage across a very large fleet). */
        Assert.True(new DarlingConfig().CapturePlans);

        var omitted = DarlingConfig.Parse(@"{ ""postgres"": { ""managed"": true }, ""servers"": [ { ""host"": ""SQL2022"" } ] }");
        Assert.True(omitted.CapturePlans);

        var disabled = DarlingConfig.Parse(@"{ ""postgres"": { ""managed"": true }, ""servers"": [ { ""host"": ""SQL2022"" } ], ""capturePlans"": false }");
        Assert.False(disabled.CapturePlans);
    }

    [Fact]
    public void CollectSchemaChangeEvents_DefaultsTrue_AndParsesOverride()
    {
        /* Default true: the default-trace collector records Object:Created/Altered/Deleted schema-change
           events (today's behavior). A config with no key keeps the default; explicit false suppresses the
           Object-DDL slice on a noisy/benchmark box — e.g. HammerDB TPC-H Q15's create/drop revenue-view
           flood — while leaving the file-growth / ErrorLog / audit categories collected. The shared
           collector's equivalent of the full Dashboard proc's @include_object_events. */
        Assert.True(new DarlingConfig().CollectSchemaChangeEvents);

        var omitted = DarlingConfig.Parse(@"{ ""postgres"": { ""managed"": true }, ""servers"": [ { ""host"": ""SQL2022"" } ] }");
        Assert.True(omitted.CollectSchemaChangeEvents);

        var disabled = DarlingConfig.Parse(@"{ ""postgres"": { ""managed"": true }, ""servers"": [ { ""host"": ""SQL2022"" } ], ""collectSchemaChangeEvents"": false }");
        Assert.False(disabled.CollectSchemaChangeEvents);
    }

    [Fact]
    public void ConnectAs_DefaultsToAdmin_AndParsesViewer()
    {
        /* V8 security split: the Viewer connects as the admin role by default (reads both schemas +
           writes config); "viewer" points a locked-down deployment at the read-only role. */
        Assert.Equal("admin", new PostgresConfig().ConnectAs);

        var omitted = DarlingConfig.Parse(@"{ ""postgres"": { ""managed"": true }, ""servers"": [ { ""host"": ""SQL2022"" } ] }");
        Assert.Equal("admin", omitted.Postgres.ConnectAs);

        var readOnly = DarlingConfig.Parse(@"{ ""postgres"": { ""managed"": true, ""connectAs"": ""viewer"" }, ""servers"": [ { ""host"": ""SQL2022"" } ] }");
        Assert.Equal("viewer", readOnly.Postgres.ConnectAs);
    }

    [Fact]
    public void Validate_CatchesRealMisconfigurations()
    {
        Assert.Empty(ValidConfig().Validate());

        Assert.Contains(ValidConfig(c => c.Postgres.ConnectionString = "").Validate(),
            p => p.Contains("postgres.connectionString", StringComparison.Ordinal));
        Assert.Contains(ValidConfig(c => c.Servers.Clear()).Validate(),
            p => p.Contains("at least one", StringComparison.Ordinal));
        Assert.Contains(ValidConfig(c => c.Servers[0].Host = "").Validate(),
            p => p.Contains("host is required", StringComparison.Ordinal));
        Assert.Contains(ValidConfig(c => c.Servers[0].Auth = "kerberos").Validate(),
            p => p.Contains("auth must be", StringComparison.Ordinal));

        var sqlNoCreds = ValidConfig(c => c.Servers[0].Auth = "sql").Validate();
        Assert.Contains(sqlNoCreds, p => p.Contains("requires username", StringComparison.Ordinal));
        Assert.Contains(sqlNoCreds, p => p.Contains("encryptedPassword", StringComparison.Ordinal));

        /* #3484: a service principal takes the same two fields as sql auth (client id + secret), so a missing
           secret is a hard error; supplied, it validates. */
        Assert.Contains(
            ValidConfig(c => { c.Servers[0].Auth = "serviceprincipal"; c.Servers[0].Username = "app-id"; c.Servers[0].EncryptedPassword = null; c.Servers[0].Password = null; }).Validate(),
            p => p.Contains("service principal auth requires encryptedPassword", StringComparison.Ordinal));
        Assert.Empty(
            ValidConfig(c => { c.Servers[0].Auth = "serviceprincipal"; c.Servers[0].Username = "app-id"; c.Servers[0].EncryptedPassword = null; c.Servers[0].Password = "secret"; }).Validate());

        /* Managed identity is secret-less and needs no mandatory field — valid with or without a user-assigned client id. */
        Assert.Empty(ValidConfig(c => { c.Servers[0].Auth = "managedidentity"; c.Servers[0].Username = null; c.Servers[0].EncryptedPassword = null; c.Servers[0].Password = null; }).Validate());
        Assert.Empty(ValidConfig(c => { c.Servers[0].Auth = "managedidentity"; c.Servers[0].Username = "user-assigned-id"; c.Servers[0].EncryptedPassword = null; c.Servers[0].Password = null; }).Validate());
    }

    [Fact]
    public void Validate_ManagedPostgresMatrix()
    {
        /* Managed alone is valid — the connection string is derived, not configured. */
        Assert.Empty(ValidConfig(c => { c.Postgres.Managed = true; c.Postgres.ConnectionString = ""; }).Validate());

        /* Managed + connectionString is a hard error (no silent precedence rule). */
        Assert.Contains(ValidConfig(c => c.Postgres.Managed = true).Validate(),
            p => p.Contains("pick one", StringComparison.Ordinal));

        /* A nonsense managed port fails fast, before initdb ever bakes it into a cluster. */
        Assert.Contains(
            ValidConfig(c => { c.Postgres.Managed = true; c.Postgres.ConnectionString = ""; c.Postgres.Port = 0; }).Validate(),
            p => p.Contains("postgres.port", StringComparison.Ordinal));

        /* Unmanaged defaults are untouched: managed off + port 5641 + null data directory. */
        var postgres = new PostgresConfig();
        Assert.False(postgres.Managed);
        Assert.Equal(5641, postgres.Port);
        Assert.Null(postgres.DataDirectory);
    }

    [Fact]
    public void ConnectionString_MirrorsLitePosture_Integrated()
    {
        var connectionString = MonitoredServerConnection.BuildConnectionString(Server());
        var parsed = new SqlConnectionStringBuilder(connectionString);

        Assert.Equal("SQL2022", parsed.DataSource);
        Assert.Equal("master", parsed.InitialCatalog);
        Assert.Equal("PerformanceMonitorDarling", parsed.ApplicationName);
        Assert.Equal(15, parsed.ConnectTimeout);
        Assert.Equal(60, parsed.CommandTimeout);
        Assert.True(parsed.MultipleActiveResultSets);
        Assert.True(parsed.IntegratedSecurity);
        Assert.Equal(SqlConnectionEncryptOption.Mandatory, parsed.Encrypt);
        Assert.Equal(ApplicationIntent.ReadWrite, parsed.ApplicationIntent);
    }

    [Fact]
    public void ConnectionString_SqlAuth_AzureDatabase_ReadOnly_StrictAndFailClosed()
    {
        var server = Server(s =>
        {
            s.Auth = "sql";
            s.Username = "monitor";
            s.Database = "app1";
            s.ReadOnlyIntent = true;
            s.EncryptMode = "Strict";
        });
        var parsed = new SqlConnectionStringBuilder(MonitoredServerConnection.BuildConnectionString(server, "pw"));

        Assert.Equal("app1", parsed.InitialCatalog);
        Assert.Equal("monitor", parsed.UserID);
        Assert.Equal("pw", parsed.Password);
        Assert.False(parsed.IntegratedSecurity);
        Assert.Equal(ApplicationIntent.ReadOnly, parsed.ApplicationIntent);
        Assert.Equal(SqlConnectionEncryptOption.Strict, parsed.Encrypt);

        /* Unknown mode fails closed to Mandatory, matching Lite. */
        var weird = new SqlConnectionStringBuilder(
            MonitoredServerConnection.BuildConnectionString(Server(s => s.EncryptMode = "banana")));
        Assert.Equal(SqlConnectionEncryptOption.Mandatory, weird.Encrypt);

        /* SQL auth without a resolved password is a hard error, not a silent empty password. */
        Assert.Throws<InvalidOperationException>(() =>
            MonitoredServerConnection.BuildConnectionString(Server(s => { s.Auth = "sql"; s.Username = "u"; })));
    }

    [Fact]
    public void ConnectionString_EntraServicePrincipal_SetsServicePrincipalAuth_WithClientIdAndSecret()
    {
        /* #3484: non-interactive Entra service principal — client id as UserID, client secret as Password
           (resolved from EncryptedPassword like a SQL password), the ActiveDirectoryServicePrincipal mode set. */
        var server = Server(s => { s.Auth = "serviceprincipal"; s.Username = "app-client-id"; s.Database = "app1"; });
        var parsed = new SqlConnectionStringBuilder(MonitoredServerConnection.BuildConnectionString(server, "client-secret"));

        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryServicePrincipal, parsed.Authentication);
        Assert.Equal("app-client-id", parsed.UserID);
        Assert.Equal("client-secret", parsed.Password);
        Assert.False(parsed.IntegratedSecurity);

        /* A service principal with no resolved secret is a hard error, exactly like sql auth. */
        Assert.Throws<InvalidOperationException>(() =>
            MonitoredServerConnection.BuildConnectionString(Server(s => { s.Auth = "serviceprincipal"; s.Username = "app"; })));
    }

    [Fact]
    public void ConnectionString_ManagedIdentity_SetsManagedIdentityAuth_OptionalClientId_NoSecret()
    {
        /* #3484: managed identity is secret-less. A user-assigned identity names its client id in UserID; a
           system-assigned identity omits it. Neither carries a password. */
        var userAssigned = new SqlConnectionStringBuilder(
            MonitoredServerConnection.BuildConnectionString(Server(s => { s.Auth = "managedidentity"; s.Username = "ua-client-id"; })));
        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryManagedIdentity, userAssigned.Authentication);
        Assert.Equal("ua-client-id", userAssigned.UserID);
        Assert.False(userAssigned.IntegratedSecurity);

        var systemAssigned = new SqlConnectionStringBuilder(
            MonitoredServerConnection.BuildConnectionString(Server(s => { s.Auth = "managedidentity"; s.Username = null; })));
        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryManagedIdentity, systemAssigned.Authentication);
        Assert.True(string.IsNullOrEmpty(systemAssigned.UserID));
    }

    [Fact]
    public void StorageName_UsesSharedIdentityRule()
    {
        Assert.Equal("SQL2022", Server().StorageName);
        Assert.Equal("myserver:app1", Server(s => { s.Host = "myserver"; s.Database = "app1"; }).StorageName);
        Assert.Equal("myserver:app1:RO", Server(s => { s.Host = "myserver"; s.Database = "app1"; s.ReadOnlyIntent = true; }).StorageName);
    }

    [Fact]
    public void Network_Sections_AbsentByDefault()
    {
        /* Omitting the network objects = the secure default (loopback-only); both parse to null. */
        var config = DarlingConfig.Parse(@"{ ""postgres"": { ""managed"": true }, ""servers"": [ { ""host"": ""SQL2022"" } ] }");
        Assert.Null(config.Postgres.Network);
        Assert.Null(config.Mcp.Network);
    }

    [Fact]
    public void Network_ParsesPostgresRoleAndMcpToken()
    {
        var config = DarlingConfig.Parse(@"{
            ""postgres"": {
                ""managed"": true,
                ""network"": { ""listen"": ""192.168.1.205"", ""allowFrom"": ""192.168.1.0/24"", ""role"": ""admin"" }
            },
            ""mcp"": {
                ""enabled"": true,
                ""network"": { ""listen"": ""192.168.1.205"", ""allowFrom"": ""192.168.1.0/24"", ""token"": ""dev-token"" }
            },
            ""servers"": [ { ""host"": ""SQL2022"" } ]
        }");

        Assert.NotNull(config.Postgres.Network);
        Assert.Equal("192.168.1.205", config.Postgres.Network!.Listen);
        Assert.Equal("192.168.1.0/24", config.Postgres.Network.AllowFrom);
        Assert.Equal("admin", config.Postgres.Network.Role);
        Assert.True(config.Postgres.Network.IsConfigured);

        Assert.NotNull(config.Mcp.Network);
        Assert.Equal("192.168.1.205", config.Mcp.Network!.Listen);
        Assert.Equal("dev-token", config.Mcp.Network.Token);
        Assert.True(config.Mcp.Network.IsConfigured);
    }

    /// <summary>
    /// #5288, "no new setting = unchanged", the allowFrom half: a darling.json that spells allowFrom as ONE plain
    /// string, exactly as every existing config does, loads that string untouched on both listeners (the
    /// converter reshapes an ARRAY only), parses to the one-entry list <c>IPNetwork</c> gave, and resolves the
    /// same bind. The tls and hostName halves (#5288): neither key is in the file, so both read null, the MCP
    /// listener's TLS shape is "not configured" (plain HTTP, as today), and the host-name normalizer has nothing
    /// to say about an absent name.
    /// </summary>
    [Fact]
    public void NoNewSettings_McpTlsAndHostNameNull_AllowFromStringAsToday()
    {
        var config = DarlingConfig.Parse(@"{
            ""postgres"": { ""managed"": true },
            ""mcp"": { ""enabled"": true, ""network"": { ""listen"": ""192.168.1.205"", ""allowFrom"": ""192.168.1.0/24"", ""token"": ""dev-token"" } },
            ""web"": { ""enabled"": true, ""network"": { ""listen"": ""192.168.1.205"", ""allowFrom"": ""192.168.1.0/24"", ""token"": ""dev-token"" } },
            ""servers"": [ { ""host"": ""SQL2022"" } ]
        }");

        Assert.Equal("192.168.1.0/24", config.Mcp.Network!.AllowFrom);
        Assert.Equal("192.168.1.0/24", config.Web.Network!.AllowFrom);

        var asList = PerformanceMonitor.Darling.Service.Hosting.CidrAllowList.Parse(config.Mcp.Network.AllowFrom!);
        Assert.Equal(System.Net.IPNetwork.Parse("192.168.1.0/24").ToString(), asList.ToString());
        Assert.Equal(1, asList.Count);

        var mcpBind = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpHostService.ResolveMcpBind(config.Mcp, managed: true, inContainer: false);
        Assert.Equal(PerformanceMonitor.Darling.Service.Mcp.DarlingMcpHostService.McpBindMode.NetworkAndLoopback, mcpBind.Mode);
        Assert.Equal(PerformanceMonitor.Darling.Service.Mcp.DarlingMcpHostService.McpBindReason.NetworkExposed, mcpBind.Reason);

        var webBind = PerformanceMonitor.Darling.Service.Mcp.DarlingWebHostService.ResolveWebBind(config.Web, managed: true, inContainer: false);
        Assert.Equal(PerformanceMonitor.Darling.Service.Hosting.DarlingHostBinding.BindMode.NetworkAndLoopback, webBind.Mode);
        Assert.Equal(PerformanceMonitor.Darling.Service.Hosting.DarlingHostBinding.BindReason.NetworkExposed, webBind.Reason);

        /* The tls and hostName halves (#5288). */
        Assert.Null(config.Mcp.Network.Tls);
        Assert.Null(config.Mcp.Network.HostName);
        Assert.Null(config.Web.Network.Tls);
        Assert.Null(McpNetworkConfig.NormalizeHostName(config.Mcp.Network.HostName));
        Assert.Equal(
            PerformanceMonitor.Darling.Service.Hosting.DarlingWebTls.TlsShape.NotConfigured,
            PerformanceMonitor.Darling.Service.Hosting.DarlingWebTls.Describe(config.Mcp.Network.Tls, "mcp").Shape);
        Assert.True(config.Mcp.Network.IsConfigured);
    }

    [Fact]
    public void Network_ParsesMcpTlsAndHostName()
    {
        /* #5288: the two keys bind by the names the README and the sample document, and the tls block is the same
           type the web uses, so its slots resolve the same way and Describe reads it under the MCP section. */
        var config = DarlingConfig.Parse(@"{
            ""postgres"": { ""managed"": true },
            ""mcp"": {
                ""enabled"": true,
                ""network"": {
                    ""listen"": ""192.168.1.205"", ""allowFrom"": ""192.168.1.0/24"", ""token"": ""dev-token"",
                    ""hostName"": ""mcp.corp.example"",
                    ""tls"": { ""certPath"": ""/certs/mcp.crt"", ""keyPath"": ""/certs/mcp.key"" }
                }
            },
            ""servers"": [ { ""host"": ""SQL2022"" } ]
        }");

        var network = config.Mcp.Network!;
        Assert.Equal("mcp.corp.example", network.HostName);
        Assert.Equal("mcp.corp.example", McpNetworkConfig.NormalizeHostName(network.HostName));
        Assert.NotNull(network.Tls);
        Assert.Equal("/certs/mcp.crt", network.Tls!.CertPath);
        Assert.Equal("/certs/mcp.key", network.Tls.KeyPath);
        Assert.Equal(
            PerformanceMonitor.Darling.Service.Hosting.DarlingWebTls.TlsShape.Pem,
            PerformanceMonitor.Darling.Service.Hosting.DarlingWebTls.Describe(network.Tls, "mcp").Shape);
        Assert.True(network.IsConfigured);
    }

    [Fact]
    public void McpNetworkConfig_IsConfigured_SeesTlsAndHostName_IgnoresBlankOnes()
    {
        /* IsConfigured drives the BYO "network.* is ignored" notice, so a block carrying only a certificate or only
           a host name must trip it, and a block of blank values must not: the two rules WebNetworkConfig has. */
        Assert.False(new McpNetworkConfig().IsConfigured);
        Assert.False(new McpNetworkConfig { Tls = new WebTlsConfig(), HostName = "   " }.IsConfigured);
        Assert.True(new McpNetworkConfig { Tls = new WebTlsConfig { PfxPath = "/certs/mcp.pfx" } }.IsConfigured);
        Assert.True(new McpNetworkConfig { HostName = "mcp.corp.example" }.IsConfigured);
    }

    [Theory]
    [InlineData("mcp.corp.example", "mcp.corp.example")]
    [InlineData("mcp-01.corp.example", "mcp-01.corp.example")]
    [InlineData("localhost", "localhost")]
    [InlineData("darling", "darling")]
    public void NormalizeHostName_AcceptsABareDnsName(string raw, string expected)
        /* One label is a DNS name too: a LAN box is routinely reached by its short name. */
        => Assert.Equal(expected, McpNetworkConfig.NormalizeHostName(raw));

    [Theory]
    [InlineData("  mcp.corp.example  ", "mcp.corp.example")]
    [InlineData("\tmcp.corp.example\r\n", "mcp.corp.example")]
    [InlineData("mcp.corp.example.", "mcp.corp.example")]
    [InlineData("  mcp.corp.example. ", "mcp.corp.example")]
    public void NormalizeHostName_TrimsWhitespace_AndStripsOneTrailingDot(string raw, string expected)
        => Assert.Equal(expected, McpNetworkConfig.NormalizeHostName(raw));

    [Theory]
    [InlineData("MCP.Corp.Example")]
    [InlineData("Darling-Box")]
    public void NormalizeHostName_KeepsCaseAsWritten(string raw)
        /* The Host-header guard compares ignoring case, so folding here would only change what the log shows. */
        => Assert.Equal(raw, McpNetworkConfig.NormalizeHostName(raw));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\t\r\n")]
    public void NormalizeHostName_BlankIsNotSet_NullWithNothingToWarnAbout(string? raw)
        => Assert.Null(McpNetworkConfig.NormalizeHostName(raw));

    [Theory]
    [InlineData("10.1.2.3")]
    [InlineData("192.168.1.205")]
    [InlineData("::1")]
    [InlineData("[::1]")]
    [InlineData("fe80::1")]
    public void NormalizeHostName_RefusesAnIpAddress(string raw)
        /* The guard already admits the listen IP by itself, and a name that is really an address would be a second,
           unwritten way in. */
        => Assert.Null(McpNetworkConfig.NormalizeHostName(raw));

    [Theory]
    [InlineData("mcp.corp.example:5152")]
    [InlineData("https://mcp.corp.example")]
    [InlineData("http://mcp.corp.example:5152/")]
    [InlineData("mcp.corp.example/mcp")]
    [InlineData("user@mcp.corp.example")]
    [InlineData("*.corp.example")]
    [InlineData("*")]
    [InlineData("mcp corp.example")]
    public void NormalizeHostName_RefusesAPortASchemeAPathAWildcardOrWhitespaceInside(string raw)
        => Assert.Null(McpNetworkConfig.NormalizeHostName(raw));

    [Theory]
    [InlineData("mcp.corp.example..")]
    [InlineData(".")]
    [InlineData("..")]
    [InlineData(".mcp.corp.example")]
    [InlineData("mcp..corp.example")]
    public void NormalizeHostName_RefusesEmptyLabels_AndMoreThanOneTrailingDot(string raw)
        /* One trailing dot is a fully qualified name; two is a typo, and a result still ending in a dot would
           never match what a client sends as its Host. */
        => Assert.Null(McpNetworkConfig.NormalizeHostName(raw));

    [Theory]
    [InlineData("b\u00FCcher.example", "xn--bcher-kva.example")]
    [InlineData("mcp.b\u00FCcher.example", "mcp.xn--bcher-kva.example")]
    [InlineData("\u043F\u0440\u0438\u043C\u0435\u0440.example", "xn--e1afmkfd.example")]
    [InlineData("\u65E5\u672C\u8A9E.example", "xn--wgv71a119e.example")]
    [InlineData("  b\u00FCcher.example. ", "xn--bcher-kva.example")]
    [InlineData("b\u00FCcher\u3002example", "xn--bcher-kva.example")]
    [InlineData("B\u00DCCHER.Example", "xn--bcher-kva.example")]
    public void NormalizeHostName_MapsAUnicodeNameToItsPunycodeForm(string raw, string expected)
        /* #5288: a client sends the ASCII (xn--) form in its Host header, so that is the form the guard has to hold.
           The trim and the one trailing dot still come first; an ideographic full stop between labels is a dot; and
           IDNA folds case, which changes nothing the guard (it compares ignoring case) can see. */
        => Assert.Equal(expected, McpNetworkConfig.NormalizeHostName(raw));

    [Theory]
    [InlineData("xn--bcher-kva.example")]
    [InlineData("XN--BCHER-KVA.Example")]
    [InlineData("MCP.Corp.Example")]
    public void NormalizeHostName_AnAsciiName_IsNeverMapped_SoItKeepsItsCaseAndSpelling(string raw)
        /* The mapping runs only for a name with a non-ASCII character. An ASCII name that is already punycode must
           not be decoded and encoded again, and an upper-case one must not be folded: the rows above that keep the
           case as written (NormalizeHostName_KeepsCaseAsWritten) stay true for every ASCII name. */
        => Assert.Equal(raw, McpNetworkConfig.NormalizeHostName(raw));

    [Theory]
    [InlineData("b\u00FCcher..example")]       // an empty label
    [InlineData("b\u00FCcher.example..")]      // two trailing dots, the same typo as in an ASCII name
    [InlineData("b\u00FCcher.example\u3002")]  // a trailing ideographic full stop maps to a trailing dot
    [InlineData("b\u200D\u00FCcher.example")]  // a zero-width joiner the IDN rules do not allow there
    public void NormalizeHostName_RefusesAnInvalidInternationalizedName(string raw)
        => Assert.Null(McpNetworkConfig.NormalizeHostName(raw));

    [Fact]
    public void NormalizeHostName_RefusesALoneSurrogate_AndALabelTooLongOnceEncoded()
    {
        /* Built at run time: a lone surrogate in an [InlineData] literal is not safe to serialize into a test id. */
        Assert.Null(McpNetworkConfig.NormalizeHostName("mcp" + (char)0xD800 + ".example"));

        /* The 63-character label limit applies to the ENCODED form: 30 u-umlauts encode to a valid label, 60 do not. */
        Assert.NotNull(McpNetworkConfig.NormalizeHostName(new string('\u00FC', 30) + ".example"));
        Assert.Null(McpNetworkConfig.NormalizeHostName(new string('\u00FC', 60) + ".example"));
    }

    [Theory]
    [InlineData("https://b\u00FCcher.example")]    // a scheme
    [InlineData("b\u00FCcher.example:5152")]       // a port
    [InlineData("b\u00FCcher.example/mcp")]        // a path
    [InlineData("*.b\u00FCcher.example")]          // a wildcard
    [InlineData("b\u00FCcher corp.example")]       // whitespace inside
    [InlineData("\uFF11\uFF10.1.2.3")]             // a fullwidth "10": the whole name MAPS to an IPv4 address
    [InlineData("\uFF3B\uFF1A\uFF1A\uFF11\uFF3D")] // a fullwidth "[::1]": it maps to an IPv6 address
    public void NormalizeHostName_RunsTheDnsTestOnTheAsciiForm_SoAMappedNonNameIsStillRefused(string raw)
        /* The mapping lets these through (it only rewrites the Unicode); what refuses them is the same
           Uri.CheckHostName test an ASCII name gets, now run on the mapped form. Without that second look a fullwidth
           IP address would become an ASCII one and be admitted as a name. */
        => Assert.Null(McpNetworkConfig.NormalizeHostName(raw));

    [Fact]
    public void NormalizeHostName_ASetButRefusedValue_IsDistinguishableFromNotSet()
    {
        /* The host logs ONE Warning for a value that is set but refused (and ignores it), and nothing for an unset
           one. The distinction is "HostName not blank" together with a null result, so both halves must hold. */
        var refused = new McpNetworkConfig { HostName = "https://mcp.corp.example" };
        Assert.False(string.IsNullOrWhiteSpace(refused.HostName));
        Assert.Null(McpNetworkConfig.NormalizeHostName(refused.HostName));

        var unset = new McpNetworkConfig { HostName = "  " };
        Assert.True(string.IsNullOrWhiteSpace(unset.HostName));
        Assert.Null(McpNetworkConfig.NormalizeHostName(unset.HostName));
    }

    [Fact]
    public void McpNetworkConfig_ResolveToken_PrefersEncrypted_FlagsPlaintext()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        /* Plaintext fallback flags usedPlaintext (so the MCP host caller warns). */
        var plain = new McpNetworkConfig { Token = "dev-token" };
        Assert.Equal("dev-token", plain.ResolveToken(out var usedPlaintext));
        Assert.True(usedPlaintext);

        /* encryptedToken preferred (DPAPI round-trip) over an also-present plaintext, and NOT flagged. */
        var blob = DarlingSecrets.Protect("secret-token");
        var encrypted = new McpNetworkConfig { EncryptedToken = blob, Token = "wrong-plaintext" };
        Assert.Equal("secret-token", encrypted.ResolveToken(out usedPlaintext));
        Assert.False(usedPlaintext);

        /* Neither set -> null. */
        Assert.Null(new McpNetworkConfig().ResolveToken(out usedPlaintext));
        Assert.False(usedPlaintext);
    }

    [Fact]
    public void Validate_IgnoresNetworkExposureConfig_NeverFatal()
    {
        /* D-validate: network-exposure rules are enforced at the point of use (degrade to loopback),
           NEVER in the all-fatal Validate(). An EXPOSED-but-INVALID network (bad CIDR, bad role, MCP with
           no token) must add NO problem — a typo in an optional, default-off endpoint can't kill collection. */
        var config = new DarlingConfig
        {
            Postgres = new PostgresConfig
            {
                Managed = true,
                Network = new PostgresNetworkConfig { Listen = "192.168.1.205", AllowFrom = "not-a-cidr", Role = "root" },
            },
            Mcp = new McpConfig { Enabled = true, Network = new McpNetworkConfig { Listen = "0.0.0.0" /* no token */ } },
            Servers = { Server() },
        };

        Assert.Empty(config.Validate());
    }

    [Fact]
    public void Secrets_DpapiRoundTrip_BlobPreferredOverPlaintext()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var blob = DarlingSecrets.Protect("s3cret!");
        Assert.NotEqual("s3cret!", blob);
        Assert.Equal("s3cret!", DarlingSecrets.Unprotect(blob));

        var server = Server(s => { s.Auth = "sql"; s.Username = "u"; s.EncryptedPassword = blob; s.Password = "wrong-plaintext"; });
        Assert.Equal("s3cret!", DarlingSecrets.ResolvePassword(server, out var usedPlaintext));
        Assert.False(usedPlaintext);

        var devServer = Server(s => { s.Auth = "sql"; s.Username = "u"; s.Password = "dev-pw"; });
        Assert.Equal("dev-pw", DarlingSecrets.ResolvePassword(devServer, out usedPlaintext));
        Assert.True(usedPlaintext);
    }

    /// <summary>
    /// #2087: add_servers stores env:/file: secret REFERENCES verbatim in the encrypted-password slot on
    /// Linux (a pointer is not a secret, and DPAPI does not exist there). The resolver must therefore
    /// recognize a reference in that slot and resolve it instead of feeding it to DPAPI Unprotect — which
    /// would throw on every platform, since a reference is not a base64 blob.
    /// </summary>
    [Fact]
    public void ResolvePassword_ReferenceInEncryptedSlot_ResolvesInsteadOfUnprotecting()
    {
        Environment.SetEnvironmentVariable("DARLING_TEST_2087_PW", "ref-resolved!");
        try
        {
            var server = Server(s => { s.Auth = "sql"; s.Username = "u"; s.EncryptedPassword = "env:DARLING_TEST_2087_PW"; });
            Assert.Equal("ref-resolved!", DarlingSecrets.ResolvePassword(server, out var usedPlaintext));

            /* A reference is not plaintext-in-config — callers must not warn on it (the #1804 rule). */
            Assert.False(usedPlaintext);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_TEST_2087_PW", null);
        }
    }

    /// <summary>
    /// #2087's storage half: a reference passes through UNTOUCHED on every platform (the Linux onboarding
    /// path), a literal still DPAPI-encrypts on Windows, and Windows-auth (no password) stays null.
    /// </summary>
    [Fact]
    public void ProtectPasswordForStorage_ReferencesPassThrough_LiteralsEncrypt()
    {
        Assert.Equal(
            "file:/run/secrets/sql_password",
            DarlingMcpServerAdminTools.ProtectPasswordForStorage("file:/run/secrets/sql_password"));
        Assert.Equal(
            "env:SQL_PW",
            DarlingMcpServerAdminTools.ProtectPasswordForStorage("env:SQL_PW"));
        Assert.Null(DarlingMcpServerAdminTools.ProtectPasswordForStorage(null));
        Assert.Null(DarlingMcpServerAdminTools.ProtectPasswordForStorage(""));

        if (OperatingSystem.IsWindows())
        {
            var stored = DarlingMcpServerAdminTools.ProtectPasswordForStorage("literal-pw");
            Assert.NotNull(stored);
            Assert.NotEqual("literal-pw", stored);
            Assert.Equal("literal-pw", DarlingSecrets.Unprotect(stored!));
        }
    }
}
