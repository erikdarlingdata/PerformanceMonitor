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
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>#5097: the config seeding, section shaping, output checks and exit-2 paths. Names and secrets are synthetic.</summary>
public sealed class DiagnosticsBundleHardeningTests
{
    private static int ClosedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /* A config must list at least one server, so the file names one decoy; the registry-only server the tests worry about is not in it. */
    private static string WriteConfig(DirectoryInfo root, string serversJson = """[ { "name": "alpha-sql-01", "host": "alpha-sql-01.example.test" } ]""")
    {
        var path = Path.Combine(root.FullName, "darling.json");
        File.WriteAllText(path, $$"""
            {
              "postgres": { "connectionString": "Host=127.0.0.1;Port={{ClosedPort()}};Username=betaowner;Password=Zq8-secretvalue;Database=darlingtest;Timeout=2" },
              "servers": {{serversJson}}
            }
            """);
        return path;
    }

    private static string WriteLog(DirectoryInfo root, string line)
    {
        var logs = Directory.CreateDirectory(Path.Combine(root.FullName, "logs"));
        File.WriteAllText(Path.Combine(logs.FullName, $"darling-service_{DateTime.Now:yyyyMMdd}.log"), $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [ERROR] [Store] {line}\n");
        return logs.FullName;
    }

    private static async Task<(int Exit, string Err)> RunAsync(params string[] args)
    {
        var error = new StringWriter();
        var exit = await DarlingCliCommands.DiagnosticsBundleAsync(args, new StringWriter(), error, CancellationToken.None);
        return (exit, error.ToString());
    }

    /* ── exit 2: the registry cannot be read, so the service log carries no text ── */

    [Fact]
    public async Task StoreDown_ServiceLogIsCountsOnly_SoARegistryOnlyServerNameCannotReachTheFile()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-down-");
        try
        {
            var logs = WriteLog(root, "collector failed for zeta-07 at 203.0.113.9");
            var bundle = Path.Combine(root.FullName, "b.json");
            var (exit, _) = await RunAsync(bundle, "--config", WriteConfig(root), "--log-dir", logs);
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.StoreUnreachable, exit);
            var text = await File.ReadAllTextAsync(bundle);
            Assert.DoesNotContain("zeta-07", text, StringComparison.OrdinalIgnoreCase);
            var log = JsonNode.Parse(text)!["sections"]!["service_log"]!;
            Assert.True(log["text_withheld"]!.GetValue<bool>());
            Assert.Null(log["entries"]);
            Assert.Equal(1, log["counts"]![0]!["count"]!.GetValue<int>());
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task StoreDown_TheUnreachableSection_CarriesNoSentence_OnlyFixedCodes()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-downmsg-");
        try
        {
            var options = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(root.FullName, "b.json"), "--log-dir", root.FullName }).Options!;
            var config = new DarlingConfig { Servers = { new MonitoredServer { Name = "alpha-sql-01" } } };

            /* The store's own help text quotes the data directory; an error message quotes an address and an account nobody registered. */
            var error = new InvalidOperationException(@"The managed store credential (E:\qxclient\darling\pg\store.cred) does not exist; connect to 198.51.100.9 failed for user 'QXOTHER\svc_unseen'");
            var outcome = await DiagnosticsBundleRunner.BuildAsync(
                options, config, null, null, error, "missing_store_credential", CancellationToken.None);
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.StoreUnreachable, outcome.ExitCode);
            foreach (var forbidden in new[] { "qxclient", "198.51.100.9", "QXOTHER", "svc_unseen", "store.cred" })
            {
                Assert.DoesNotContain(forbidden, outcome.Text!, StringComparison.OrdinalIgnoreCase);
            }

            var section = JsonNode.Parse(outcome.Text!)!["sections"]!["store_unreachable"]!.AsObject();
            Assert.Equal("missing_store_credential", section["reason"]!.GetValue<string>());
            Assert.Null(section["message"]);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task StoreDown_ASocketFailure_CarriesTheReasonAndTheSocketErrorName()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-downsock-");
        try
        {
            var options = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(root.FullName, "b.json"), "--log-dir", root.FullName }).Options!;
            var config = new DarlingConfig { Servers = { new MonitoredServer { Name = "alpha-sql-01" } } };
            var error = new Npgsql.NpgsqlException("Failed to connect to 198.51.100.9:5432", new SocketException((int)SocketError.ConnectionRefused));
            var outcome = await DiagnosticsBundleRunner.BuildAsync(options, config, null, null, error, null, CancellationToken.None);
            var section = JsonNode.Parse(outcome.Text!)!["sections"]!["store_unreachable"]!.AsObject();
            Assert.Equal("connect_failed", section["reason"]!.GetValue<string>());
            Assert.Equal("ConnectionRefused", section["socket_error"]!.GetValue<string>());
            Assert.DoesNotContain("198.51.100.9", outcome.Text!, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task StoreDown_WithIncludeLogText_WarnsOnStderrAndInTheManifest()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-downtext-");
        try
        {
            var logs = WriteLog(root, "collector failed for plain text");
            var bundle = Path.Combine(root.FullName, "b.json");
            var (exit, err) = await RunAsync(bundle, "--config", WriteConfig(root), "--log-dir", logs, "--include-log-text");
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.StoreUnreachable, exit);
            Assert.Contains("cannot be aliased", err, StringComparison.Ordinal);
            Assert.Contains("--include-log-text", err, StringComparison.Ordinal);
            var tree = JsonNode.Parse(await File.ReadAllTextAsync(bundle))!;
            Assert.Contains(tree["manifest"]!["notes"]!.AsArray(), n => n!.GetValue<string>().Contains("cannot be aliased", StringComparison.Ordinal));
            Assert.NotEmpty(tree["sections"]!["service_log"]!["entries"]!.AsArray());
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task StoreDown_ServerOption_SaysItWasIgnored_OnStderrAndInTheSection()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-downserver-");
        try
        {
            var bundle = Path.Combine(root.FullName, "b.json");
            var (exit, err) = await RunAsync(bundle, "--config", WriteConfig(root), "--log-dir", root.FullName, "--server", "alpha-01");
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.StoreUnreachable, exit);
            Assert.Contains("--server was ignored", err, StringComparison.Ordinal);
            var tree = JsonNode.Parse(await File.ReadAllTextAsync(bundle))!;
            Assert.Contains("--server was ignored", tree["sections"]!["store_unreachable"]!["server_scope"]!.GetValue<string>(), StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /* ── the alias map ── */

    [Fact]
    public async Task AliasMap_SamePathAsTheBundle_ExitsOne_AndWritesNothing()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-samemap-");
        try
        {
            var bundle = Path.Combine(root.FullName, "b.json");
            var (exit, err) = await RunAsync(bundle, "--config", WriteConfig(root), "--alias-map", Path.Combine(root.FullName, ".", "B.JSON".ToLowerInvariant()));
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.ConfigError, exit);
            Assert.Contains("different file", err, StringComparison.Ordinal);
            Assert.False(File.Exists(bundle));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task AliasMap_ExistingFile_NeedsForce()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-mapexists-");
        try
        {
            var map = Path.Combine(root.FullName, "map.txt");
            await File.WriteAllTextAsync(map, "keep me");
            var bundle = Path.Combine(root.FullName, "b.json");
            var (exit, err) = await RunAsync(bundle, "--config", WriteConfig(root), "--alias-map", map);
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.ConfigError, exit);
            Assert.Contains("--force", err, StringComparison.Ordinal);
            Assert.Equal("keep me", await File.ReadAllTextAsync(map));
            Assert.False(File.Exists(bundle));

            var (forced, _) = await RunAsync(bundle, "--config", WriteConfig(root), "--log-dir", root.FullName, "--alias-map", map, "--force");
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.StoreUnreachable, forced);
            Assert.StartsWith("DO NOT ATTACH", (await File.ReadAllLinesAsync(map))[0], StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /* ── argument nits ── */

    [Fact]
    public void ParseArgs_PathEndingInADot_GetsOneDotJson_AndIncludeLogTextIsRead()
    {
        Assert.Equal("out.json", DiagnosticsBundle.ParseArgs(new[] { "out." }).Options!.OutputPath);
        var options = DiagnosticsBundle.ParseArgs(new[] { "out", "--include-log-text" }).Options!;
        Assert.True(options.IncludeLogText);
        Assert.False(DiagnosticsBundle.ParseArgs(new[] { "out" }).Options!.IncludeLogText);
    }

    /* ── the temp file ── */

    [Fact]
    public void WriteAtomically_UsesARandomTempName_LeavesNothingBehind_AndDoesNotFollowAPlantedFile()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-write-");
        try
        {
            var path = Path.Combine(root.FullName, "b.json");
            File.WriteAllText(path + ".tmp", "planted");
            Assert.Null(DiagnosticsBundle.WriteAtomically(path, "hello", force: false));
            Assert.Equal("hello", File.ReadAllText(path));
            Assert.Equal("planted", File.ReadAllText(path + ".tmp"));
            Assert.Equal(2, root.GetFiles().Length);

            /* An existing target without force fails the move and removes the temp file. */
            Assert.NotNull(DiagnosticsBundle.WriteAtomically(path, "other", force: false));
            Assert.Equal("hello", File.ReadAllText(path));
            Assert.Equal(2, root.GetFiles().Length);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /* ── fleet tags, engine keys, alias-then-cut ── */

    [Fact]
    public void FleetOverview_TagsBecomeACount_WithNoNames()
    {
        var fleet = JsonNode.Parse("""{"total":2,"tags":[{"id":1,"name":"ClientAlphaTeam","parent_id":null},{"id":2,"name":"ClientBetaTeam","parent_id":1}]}""");
        var shaped = DiagnosticsBundleRunner.ShapeFleetOverview(fleet)!;
        Assert.Equal(2, shaped["tags"]!["count"]!.GetValue<int>());
        Assert.DoesNotContain("Client", shaped.ToJsonString(), StringComparison.Ordinal);
        Assert.Equal(2, shaped["total"]!.GetValue<int>());
    }

    [Fact]
    public void ConfigShape_ServersByEngine_IsKeyedByTheParsedEngine_NeverByConfigText()
    {
        var config = new DarlingConfig
        {
            Servers =
            {
                new MonitoredServer { Name = "a", Engine = "zetacorp-special" },
                new MonitoredServer { Name = "b", Engine = "Aurora" },
                new MonitoredServer { Name = "c" },
            },
        };
        var shape = DiagnosticsBundle.BuildConfigShape(config);
        Assert.DoesNotContain("zetacorp", shape.ToJsonString(), StringComparison.OrdinalIgnoreCase);
        var engines = shape["servers_by_engine"]!.AsObject();
        Assert.Equal(2, engines["sqlserver"]!.GetValue<int>());
        Assert.Equal(1, engines["postgresql"]!.GetValue<int>());
    }

    [Fact]
    public void ServiceLogText_IsAliasedWhole_BeforeItIsCut_SoANameAcrossTheCutIsNotLeftAsAPrefix()
    {
        var aliaser = new BundleAliaser();
        aliaser.AddName(AliasKind.Host, "sql01.contoso-corp.example.test");
        var t = DateTime.Now;
        var cut = DiagnosticsBundleServiceLog.MaxEntryChars;
        var message = new string('x', cut - 17) + " sql01.contoso-corp.example.test failed";
        var log = $"{t:yyyy-MM-dd HH:mm:ss.fff} [ERROR] [Cat] {message}";
        var entries = DiagnosticsBundleServiceLog.Parse(log, false, t.AddHours(-1), DiagnosticsBundleServiceLog.AliasInputChars);
        var section = new JsonObject
        {
            ["status"] = "ok",
            ["entries"] = new JsonArray(new JsonObject { ["message"] = entries[0].Message }),
        };

        var sections = new[] { new BundleSection("service_log", section, false, DiagnosticsBundleRunner.CapServiceLogMessages) };
        var (text, leaks, _) = DiagnosticsBundle.Assemble(sections, aliaser, DiagnosticsBundle.BuildManifest(1, "fleet", 0));
        Assert.Empty(leaks);
        Assert.DoesNotContain("contoso", text!, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("sql01", text!, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("host-1", text!, StringComparison.Ordinal);
    }

    [Fact]
    public void ServiceLogRead_KeepsEnoughTextForTheAliasPass_ThroughTheRealReadPath()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-cutread-");
        try
        {
            var message = new string('x', DiagnosticsBundleServiceLog.MaxEntryChars - 17) + " sql01.contoso-corp.example.test failed";
            WriteLog(root, message);
            var read = DiagnosticsBundleServiceLog.Read(Path.Combine(root.FullName, "logs"), "--log-dir", DateTime.Now.AddHours(-1));
            var aliaser = new BundleAliaser();
            aliaser.AddName(AliasKind.Host, "sql01.contoso-corp.example.test");
            var sections = new[] { new BundleSection("service_log", read, false, DiagnosticsBundleRunner.CapServiceLogMessages) };
            var (text, leaks, _) = DiagnosticsBundle.Assemble(sections, aliaser, DiagnosticsBundle.BuildManifest(1, "fleet", 0));
            Assert.Empty(leaks);
            Assert.DoesNotContain("contoso", text!, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("sql01", text!, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void StatementText_IsAliasedWhole_BeforeItIsCompacted()
    {
        var aliaser = new BundleAliaser();
        aliaser.AddName(AliasKind.Database, "gamma_orders_ledger");
        var query = "select " + new string('c', 225) + " from gamma_orders_ledger.t";
        var section = new JsonObject { ["cumulative"] = new JsonObject { ["statements"] = new JsonArray(new JsonObject { ["query"] = query }) } };
        var sections = new[] { new BundleSection("store_statements", section, false, DiagnosticsBundleRunner.CompactStatementTexts) };
        var (text, leaks, _) = DiagnosticsBundle.Assemble(sections, aliaser, DiagnosticsBundle.BuildManifest(1, "fleet", 0));
        Assert.Empty(leaks);
        Assert.DoesNotContain("gamma", text!, StringComparison.OrdinalIgnoreCase);
    }

    /* ── notification identifiers and service accounts ── */

    [Fact]
    public void NotificationIdentifiers_AreSeeded_AndWebhookUrlsAreSecrets()
    {
        var config = new DarlingConfig();
        config.Smtp.Host = "mail.zetacorp.example.test";
        config.Smtp.From = "alerts@omegacorp.example.test";
        config.Smtp.To = "ops@epsilon-ops.example.test, dba@deltaorg.example.test";
        config.Web.PublicBaseUrl = "https://dash.gammahost.example.test:5153/";
        config.Webhooks.SlackUrl = "https://hooks.betahooks.example.test/services/T000/B000/XXXSECRETXXX";
        config.Webhooks.TeamsProxy = "http://proxy.thetaprox.example.test:3128";
        var aliaser = new BundleAliaser();
        DiagnosticsBundle.SeedNotificationIdentifiers(aliaser, config);
        var text = aliaser.Alias(
            "smtp mail.zetacorp.example.test from omegacorp.example.test to epsilon-ops.example.test deltaorg.example.test dash.gammahost.example.test "
            + "hook https://hooks.betahooks.example.test/services/T000/B000/XXXSECRETXXX via proxy.thetaprox.example.test");
        foreach (var forbidden in new[] { "zetacorp", "omegacorp", "epsilon-ops", "deltaorg", "gammahost", "betahooks", "XXXSECRET", "thetaprox" })
        {
            Assert.DoesNotContain(forbidden, text, StringComparison.OrdinalIgnoreCase);
        }
    }

    /* ── mcp.network.hostName (#5288) ──
       The operator types this DNS name, and the service log writes it twice: the certificate-name warning names its
       NORMALIZED form at every start (the trailing dot dropped, an internationalized name in its ASCII form), and the
       warning for a value that is not a bare DNS name echoes it AS WRITTEN. A bundle built from a config that sets
       the name holds neither spelling. The warning texts below are the product's own. */

    /* The service-log section as the bundle builds it: parsed, aliased whole, then checked by the output verifier. */
    private static string BundleOfServiceLogMessages(BundleAliaser aliaser, params string[] messages)
    {
        var t = DateTime.Now;
        var log = string.Join("\n", messages.Select(m => $"{t:yyyy-MM-dd HH:mm:ss.fff} [WARN ] [Mcp] {m}"));
        var entries = DiagnosticsBundleServiceLog.Parse(log, false, t.AddHours(-1), DiagnosticsBundleServiceLog.AliasInputChars);
        Assert.Equal(messages.Length, entries.Count);
        var section = new JsonObject
        {
            ["status"] = "ok",
            ["entries"] = new JsonArray(entries.Select(e => (JsonNode?)new JsonObject { ["message"] = e.Message }).ToArray()),
        };

        var sections = new[] { new BundleSection("service_log", section, false, DiagnosticsBundleRunner.CapServiceLogMessages) };
        var (text, leaks, _) = DiagnosticsBundle.Assemble(sections, aliaser, DiagnosticsBundle.BuildManifest(1, "fleet", 0));
        Assert.Empty(leaks);
        return text!;
    }

    [Theory]
    [InlineData("gate.kappacorp.example.test", "kappacorp")]
    [InlineData("  Gate.KappaCorp.Example.Test.  ", "kappacorp")]    // the log names it trimmed and without the trailing dot
    [InlineData("gate.käppacorp.example.test", "kppacorp")]     // written in Unicode, named in its ASCII form
    public void McpHostName_InTheCertificateNameWarning_ComesOutAliased(string configured, string distinctive)
    {
        var config = new DarlingConfig { Mcp = { Network = new McpNetworkConfig { HostName = configured } } };
        var aliaser = new BundleAliaser();
        DiagnosticsBundle.SeedFromConfig(aliaser, config, null);

        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=bundle-test", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
        var normalized = DarlingMcpHostService.NormalizedHostName(configured);
        var warning = DarlingListenerTls.SanWarning(ListenerTlsLabels.Mcp, certificate, IPAddress.Parse("192.0.2.10"), 5152, normalized);
        Assert.Contains(normalized!, warning!, StringComparison.Ordinal);

        var text = BundleOfServiceLogMessages(aliaser, warning!);
        Assert.DoesNotContain(normalized!, text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(distinctive, text, StringComparison.OrdinalIgnoreCase);
        Assert.Matches(@"host-\d+", text);
    }

    [Theory]
    [InlineData("https://gate.kappacorp.example.test:5152/")]
    [InlineData("gate.kappacorp.example.test:5152")]
    [InlineData("*.kappacorp.example.test")]
    public void McpHostName_ARefusedValue_AndTheHostHeadersThatFollowIt_ComeOutAliased(string configured)
    {
        var config = new DarlingConfig { Mcp = { Network = new McpNetworkConfig { HostName = configured } } };
        var aliaser = new BundleAliaser();
        DiagnosticsBundle.SeedFromConfig(aliaser, config, null);

        var logger = new CapturingTestLogger();
        Assert.Null(DarlingMcpHostService.ResolveAllowedHostName(configured, networkMode: true, logger));
        var warning = Assert.Single(logger.Lines);
        Assert.Contains(configured, warning, StringComparison.Ordinal);

        /* The value is refused, so a client that connects by the real name is refused too, and the log names that Host alone. */
        var refusal = "the Host header 'gate.kappacorp.example.test' is not an address this endpoint binds" + DarlingMcpHostService.HostRefusalAdmits;
        var text = BundleOfServiceLogMessages(aliaser, warning, refusal);
        Assert.DoesNotContain("kappacorp", text, StringComparison.OrdinalIgnoreCase);
    }

    /* The service log's own words for its two listeners ("MCP server", "Web dashboard", mcp.network.allowFrom): a host
       name that starts with one of them is aliased whole, with its domain, and the words stay readable. The texts are
       the cleartext warning each listener logs at every start. */
    [Theory]
    [InlineData("mcp", "mcp.corp.example.test")]              // mcp.network.hostName
    [InlineData("web", "web.corp.example.test")]              // web.publicBaseUrl
    [InlineData("dashboard", "dashboard.corp.example.test")]  // web.publicBaseUrl
    public void AHostNameThatStartsWithAListenerWord_KeepsTheServiceLogsWordsAndAliasesTheHost(string firstLabel, string hostName)
    {
        var config = firstLabel == "mcp"
            ? new DarlingConfig { Mcp = { Network = new McpNetworkConfig { HostName = hostName } } }
            : new DarlingConfig { Web = { PublicBaseUrl = "https://" + hostName + "/" } };
        var aliaser = new BundleAliaser();
        DiagnosticsBundle.SeedFromConfig(aliaser, config, null);

        var text = BundleOfServiceLogMessages(
            aliaser,
            DarlingListenerTls.CleartextWarning(ListenerTlsLabels.Mcp),
            DarlingListenerTls.CleartextWarning(ListenerTlsLabels.Web),
            "clients connect to " + hostName,
            "the Host header '" + hostName + "' is not an address this endpoint binds" + DarlingMcpHostService.HostRefusalAdmits);

        Assert.Contains("MCP server is LAN-exposed WITHOUT TLS", text, StringComparison.Ordinal);
        /* The Host-refusal line names mcp.network.listen and mcp.network.hostName: the settings stay readable, the Host header named beside them is aliased. */
        Assert.Contains("mcp.network.listen when LAN-exposed, or mcp.network.hostName when LAN-exposed", text, StringComparison.Ordinal);
        Assert.Contains("mcp.network.allowFrom bounds only who can route to the port", text, StringComparison.Ordinal);
        Assert.Contains("Web dashboard is LAN-exposed WITHOUT TLS", text, StringComparison.Ordinal);
        Assert.Contains("web.network.allowFrom bounds only who can route to the port", text, StringComparison.Ordinal);
        Assert.DoesNotContain(hostName, text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("corp.example", text, StringComparison.OrdinalIgnoreCase);
        Assert.Matches(@"clients connect to host-\d+", text);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void McpHostName_NotSet_SeedsNothing(string? configured)
    {
        var withName = new BundleAliaser();
        DiagnosticsBundle.SeedFromConfig(withName, new DarlingConfig { Mcp = { Network = new McpNetworkConfig { HostName = configured } } }, null);
        var without = new BundleAliaser();
        DiagnosticsBundle.SeedFromConfig(without, new DarlingConfig(), null);
        Assert.Equal(without.TokenCount, withName.TokenCount);
    }

    [Theory]
    [InlineData(@"QXCORP\svc_zeta", "QXCORP", "svc_zeta")]
    [InlineData(@".\svc_local", null, "svc_local")]
    [InlineData("svc_up@qxcorp.example.test", "qxcorp.example.test", "svc_up")]
    [InlineData("LocalSystem", null, null)]
    [InlineData(@"NT AUTHORITY\NetworkService", null, null)]
    [InlineData(@"NT SERVICE\SomeService", null, null)]
    public void ServiceAccount_IsSplitIntoDomainAndLogin(string objectName, string? domain, string? login)
    {
        Assert.Equal((domain, login), DiagnosticsBundle.ParseServiceAccount(objectName));
    }

    [Fact]
    public void SeedFromConfig_AddsTheUserDomainName_AndNotesWhenTheServiceAccountCannotBeRead()
    {
        var aliaser = new BundleAliaser();
        var notes = DiagnosticsBundle.SeedFromConfig(aliaser, new DarlingConfig(), null);
        if (!OperatingSystem.IsWindows())
        {
            Assert.Contains(notes, n => n.Contains("not Windows", StringComparison.Ordinal));
        }

        var domain = Environment.UserDomainName;
        if (domain.Length >= 2 && !new[] { "system", "administrator", "root", "localsystem", "service", "networkservice", "admin" }.Contains(domain, StringComparer.OrdinalIgnoreCase))
        {
            Assert.DoesNotContain(domain, aliaser.Alias("domain " + domain + " here"), StringComparison.OrdinalIgnoreCase);
        }
    }

    /* ── the secret length floor ── */

    [Fact]
    public void ConfigSecrets_OfAnyLength_AreTracked()
    {
        var config = new DarlingConfig();
        config.Servers.Add(new MonitoredServer { Name = "alpha", Password = "xy" });
        var aliaser = new BundleAliaser();
        DiagnosticsBundle.SeedFromConfig(aliaser, config, null);
        Assert.Equal("pw [redacted] ok", aliaser.Alias("pw xy ok"));
    }
}
