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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
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
    public async Task StoreDown_TheUnreachableMessage_GoesThroughTheAliasPass_NotJustTheVerifier()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-downmsg-");
        try
        {
            var options = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(root.FullName, "b.json"), "--log-dir", root.FullName }).Options!;
            var config = new DarlingConfig { Servers = { new MonitoredServer { Name = "alpha-sql-01" } } };

            /* An address and a quoted account the name set has never seen: only the alias pass (not the verifier) can remove them. */
            var outcome = await DiagnosticsBundleRunner.BuildAsync(
                options, config, null, null, null, "connect to 198.51.100.9 failed for user 'QXOTHER\\svc_unseen'", CancellationToken.None);
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.StoreUnreachable, outcome.ExitCode);
            Assert.DoesNotContain("198.51.100.9", outcome.Text!, StringComparison.Ordinal);
            Assert.DoesNotContain("QXOTHER", outcome.Text!, StringComparison.Ordinal);
            Assert.DoesNotContain("svc_unseen", outcome.Text!, StringComparison.Ordinal);
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

    /* ── H5, M1, M2 ── */

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

    /* ── M5, H3 ── */

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

    /* ── L1: the secret floor ── */

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
