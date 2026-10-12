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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#5097: the verifier's treatment of ordinary names, the quoted-account rule, the service-account notes, the cut-after-alias order and the alias-map writer. Names are synthetic.</summary>
public sealed class DiagnosticsBundleRound2Tests
{
    private static (string? Text, int Leaks) Assemble(BundleAliaser aliaser, params BundleSection[] sections)
    {
        var (text, leaks, _) = DiagnosticsBundle.Assemble(sections, aliaser, DiagnosticsBundle.BuildManifest(1, "fleet", 0));
        return (text, leaks.Count);
    }

    private static BundleSection Section(string name, JsonNode node, Func<JsonNode?, JsonNode?>? after = null) => new(name, node, false, after);

    /* ── a name that is an ordinary word must not turn the verb unusable ── */

    [Theory]
    [InlineData("Store", "store_log")]
    [InlineData("Data", "data_volume")]
    [InlineData("Health", "collection_health")]
    public void ADatabaseNamedLikeAProductWord_ExitsClean_AndTheProductKeyIsIntact(string database, string key)
    {
        var aliaser = new BundleAliaser();
        aliaser.AddName(AliasKind.Database, database);
        var node = new JsonObject { ["status"] = "ok", [key] = 3, ["schema_version_stored"] = 163, ["rows"] = new JsonArray(new JsonObject { ["database_name"] = database }) };
        var (text, leaks) = Assemble(aliaser, Section("store_log", node));

        Assert.Equal(0, leaks);
        Assert.NotNull(text);
        Assert.Contains("\"" + key + "\"", text, StringComparison.Ordinal);
        Assert.Contains("\"store_log\"", text, StringComparison.Ordinal);
        Assert.Contains("db-1", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ARealNameInsideMintedAliasText_IsNotALeak_AndTheGluedPassNeverRewritesTheAlias()
    {
        var aliaser = new BundleAliaser();
        aliaser.AddName(AliasKind.Domain, "example.test");
        aliaser.AddName(AliasKind.Database, "main");
        aliaser.AddName(AliasKind.Host, "host");
        aliaser.AddName(AliasKind.Server, "alpha-sql-01");
        var node = new JsonObject
        {
            ["status"] = "ok",
            ["note"] = "reached example.test from alpha-sql-01 using main",
            ["glued"] = "mainalpha-sql-01",
        };
        var (text, leaks) = Assemble(aliaser, Section("store_log", node));

        Assert.Equal(0, leaks);
        Assert.NotNull(text);
        Assert.Contains("domain-1", text, StringComparison.Ordinal);
        Assert.Contains("server-1", text, StringComparison.Ordinal);
        Assert.DoesNotContain("dodb", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("doDB", text, StringComparison.Ordinal);
    }

    /* ── a monitored name used as a whole key is renamed, never exempted ── */

    [Theory]
    [InlineData("zeta07", true)]
    [InlineData("sales_db", false)]
    [InlineData("Store", false)]
    public void AKeyThatIsItselfAMonitoredName_IsRenamedToItsAlias_AndTheValueAndOrderSurvive(string name, bool isServer)
    {
        var aliaser = new BundleAliaser();
        aliaser.AddName(isServer ? AliasKind.Server : AliasKind.Database, name);
        var node = new JsonObject { ["status"] = "ok", ["first"] = 1, [name] = new JsonObject { ["x"] = 1 }, ["last"] = 2 };
        var (text, leaks) = Assemble(aliaser, Section("store_log", node));

        Assert.Equal(0, leaks);
        Assert.NotNull(text);
        /* "Store" is also a substring of the product's own store_log, so the key is checked whole. */
        Assert.DoesNotContain("\"" + name + "\"", text, StringComparison.OrdinalIgnoreCase);
        var alias = isServer ? "server-1" : "db-1";
        Assert.Contains("\"" + alias + "\"", text, StringComparison.Ordinal);
        Assert.True(text.IndexOf("\"first\"", StringComparison.Ordinal) < text.IndexOf("\"" + alias + "\"", StringComparison.Ordinal));
        Assert.True(text.IndexOf("\"" + alias + "\"", StringComparison.Ordinal) < text.IndexOf("\"last\"", StringComparison.Ordinal));
        Assert.Contains("\"x\": 1", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AKeyThatIsAProductWord_IsNotRenamed_EvenWhenANameIsASubstringOfIt()
    {
        var aliaser = new BundleAliaser();
        aliaser.AddName(AliasKind.Database, "Store");
        var node = new JsonObject { ["status"] = "ok", ["store_log"] = 1, ["server_id"] = 7 };
        var (text, leaks) = Assemble(aliaser, Section("store_log", node));

        Assert.Equal(0, leaks);
        Assert.NotNull(text);
        Assert.Contains("\"store_log\": 1", text, StringComparison.Ordinal);
        Assert.Contains("\"status\": \"ok\"", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ADataShapedKeyHoldingAName_StillFailsClosed()
    {
        var aliaser = new BundleAliaser();
        aliaser.AddName(AliasKind.Database, "Store");
        var node = new JsonObject { ["status"] = "ok", ["Store_Log"] = 1 };
        var (text, leaks) = Assemble(aliaser, Section("store_log", node));
        Assert.True(leaks > 0);
        Assert.Null(text);

        var spaced = new JsonObject { ["status"] = "ok", ["store log"] = 1 };
        var (spacedText, spacedLeaks) = Assemble(aliaser, Section("store_log", spaced));
        Assert.True(spacedLeaks > 0);
        Assert.Null(spacedText);
    }

    /* ── the quoted WORD\word rule ── */

    [Theory]
    [InlineData(@"open '..\logs' to see them")]
    [InlineData("see \"pg\\pg.log\" for the server log")]
    [InlineData(@"use 'server\instance' as the form")]
    [InlineData(@"read 'etc\app.conf' first")]
    public void AQuotedPathOrHelpText_RegistersNoAccount(string text)
    {
        var aliaser = new BundleAliaser();
        aliaser.Harvest(JsonNode.Parse(System.Text.Json.JsonSerializer.Serialize(new { message = text })));
        Assert.Empty(aliaser.AliasMapLines());
        Assert.Equal(text, aliaser.Alias(text));
    }

    [Fact]
    public void AQuotedNetBiosAccount_StillAliases()
    {
        var aliaser = new BundleAliaser();
        var text = aliaser.Alias(@"Login failed for user 'QXCORP\svc_zeta'.");
        Assert.DoesNotContain("QXCORP", text, StringComparison.Ordinal);
        Assert.DoesNotContain("svc_zeta", text, StringComparison.Ordinal);
        Assert.Contains("domain-", text, StringComparison.Ordinal);
        Assert.Contains("login-", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AHalfThatIsABundleKey_IsNotAnAccount()
    {
        var aliaser = new BundleAliaser();
        aliaser.NoteKeys(new JsonObject { ["store_log"] = 1, ["slow_reads"] = 2 });
        Assert.Equal(@"saw 'store_log\slow_reads' here", aliaser.Alias(@"saw 'store_log\slow_reads' here"));
        Assert.Empty(aliaser.AliasMapLines());
    }

    /* ── the service-account read ── */

    [Fact]
    public void ServiceAccount_MissingKey_NonStringValue_AndBuiltInAccounts_EachAddANote()
    {
        var (_, _, missing) = DiagnosticsBundle.ClassifyServiceAccount(keyFound: false, null);
        var (_, _, notText) = DiagnosticsBundle.ClassifyServiceAccount(keyFound: true, 42);
        var (_, _, system) = DiagnosticsBundle.ClassifyServiceAccount(keyFound: true, "LocalSystem");
        var (_, _, authority) = DiagnosticsBundle.ClassifyServiceAccount(keyFound: true, @"NT AUTHORITY\NetworkService");
        var (_, _, service) = DiagnosticsBundle.ClassifyServiceAccount(keyFound: true, @"NT SERVICE\SomeService");
        foreach (var note in new[] { missing, notText, system, authority, service })
        {
            Assert.False(string.IsNullOrWhiteSpace(note));
        }

        var (domain, login, none) = DiagnosticsBundle.ClassifyServiceAccount(keyFound: true, @"QXCORP\svc_zeta");
        Assert.Equal("QXCORP", domain);
        Assert.Equal("svc_zeta", login);
        Assert.Null(none);
        Assert.DoesNotContain("SomeService", service!, StringComparison.Ordinal);
    }

    /* ── notification routes and the SMTP login ── */

    [Fact]
    public void NotificationRoutes_AndTheSmtpUsername_AreSeeded()
    {
        var config = new DarlingConfig();
        config.Smtp.Username = "qxsmtpuser@qxmail.example.test";
        config.NotificationRoutes = new[]
        {
            new PerformanceMonitor.Notifications.NotificationRoute(
                1, "All", "https://hooks.qxrouteteams.example.test/services/T1/B1/ROUTESECRET01", "", "", "pdroutekey-QX99", "ops@qxroutedom.example.test, dba@qxroutedom2.example.test", true),
        };
        var aliaser = new BundleAliaser();
        DiagnosticsBundle.SeedNotificationIdentifiers(aliaser, config);
        var text = aliaser.Alias(
            "route https://hooks.qxrouteteams.example.test/services/T1/B1/ROUTESECRET01 key pdroutekey-QX99 mail qxroutedom.example.test and qxroutedom2.example.test login qxsmtpuser at qxmail.example.test");
        foreach (var forbidden in new[] { "qxrouteteams", "pdroutekey", "ROUTESECRET01", "qxroutedom", "qxsmtpuser", "qxmail" })
        {
            Assert.DoesNotContain(forbidden, text, StringComparison.OrdinalIgnoreCase);
        }
    }

    /* ── cut after alias ── */

    [Fact]
    public void AnFqdnStraddlingTheHealthCut_LeavesNoPrefix()
    {
        const string host = "sql01.contoso-corp.example.test";
        var error = new string('x', DarlingCutLength - 10) + " " + host + " failed";
        Assert.True(error.IndexOf(host, StringComparison.Ordinal) < DarlingCutLength && error.IndexOf(host, StringComparison.Ordinal) + host.Length > DarlingCutLength);
        var node = new JsonObject
        {
            ["status"] = "ok",
            ["health_by_server"] = new JsonArray(new JsonObject
            {
                ["server_name"] = "alpha-sql-01",
                ["health"] = new JsonObject
                {
                    ["collectors"] = new JsonArray(new JsonObject { ["collector"] = "wait_stats", ["last_error"] = error, ["last_error_truncated"] = false }),
                },
            }),
        };
        var aliaser = new BundleAliaser();
        aliaser.AddName(AliasKind.Host, host);
        var (text, leaks) = Assemble(aliaser, Section("collection", node, DiagnosticsBundleRunner.CutHealthTexts));

        Assert.Equal(0, leaks);
        Assert.NotNull(text);
        Assert.DoesNotContain("sql01", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("contoso", text, StringComparison.OrdinalIgnoreCase);
        var row = JsonNode.Parse(text!)!["sections"]!["collection"]!["health_by_server"]![0]!["health"]!["collectors"]![0]!;
        Assert.True(row["last_error_truncated"]!.GetValue<bool>() || row["last_error"]!.GetValue<string>().Length <= DarlingCutLength + 20);
    }

    private const int DarlingCutLength = 500;

    [Fact]
    public void SlowReads_DropTheStatementSummary_AndKeepTheStatements()
    {
        var node = JsonNode.Parse("""{"reads":[{"statement_summary":"#1 query_store_stats @ gamma_orders 1a2b","statements":[{"label":"query_store_stats @ gamma_orders 1a2b3c","ms":1.5}]}]}""");
        var shaped = DiagnosticsBundleRunner.DropStatementSummaries(node)!;
        Assert.Null(shaped["reads"]![0]!["statement_summary"]);
        Assert.NotNull(shaped["reads"]![0]!["statements"]![0]!["label"]);
    }

    /* ── the alias map ── */

    private static async Task<(int Exit, string Err)> RunAsync(params string[] args)
    {
        var error = new StringWriter();
        var exit = await DarlingCliCommands.DiagnosticsBundleAsync(args, new StringWriter(), error, CancellationToken.None);
        return (exit, error.ToString());
    }

    private static string WriteConfig(DirectoryInfo root)
    {
        var path = Path.Combine(root.FullName, "darling.json");
        File.WriteAllText(path, """
            {
              "postgres": { "connectionString": "Host=127.0.0.1;Port=1;Username=betaowner;Password=Zq8-secretvalue;Database=darlingtest;Timeout=2" },
              "servers": [ { "name": "alpha-sql-01", "host": "alpha-sql-01.example.test" } ]
            }
            """);
        return path;
    }

    /// <summary>Runs a helper process (a link tool) and returns whether it exited 0.</summary>
    private static bool RunTool(string fileName, string arguments, params string[] argumentList)
    {
        var start = new System.Diagnostics.ProcessStartInfo(fileName)
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        if (arguments.Length > 0)
        {
            start.Arguments = arguments;
        }

        foreach (var argument in argumentList)
        {
            start.ArgumentList.Add(argument);
        }

        using var process = System.Diagnostics.Process.Start(start)!;
        process.StandardOutput.ReadToEnd();
        process.StandardError.ReadToEnd();
        process.WaitForExit();
        return process.ExitCode == 0;
    }

    /// <summary>
    /// Builds a symbolic link of the right kind: a directory link for a directory target, a file link otherwise. Windows
    /// needs a privilege for this, so a refusal there ends the test as skipped with that reason; off Windows a refusal
    /// is a failure, because the attack can always be built.
    /// </summary>
    private static void MakeSymbolicLink(string link, string target, bool directory)
    {
        try
        {
            if (directory)
            {
                Directory.CreateSymbolicLink(link, target);
            }
            else
            {
                File.CreateSymbolicLink(link, target);
            }
        }
        catch (Exception ex) when (ex is UnauthorizedAccessException or IOException)
        {
            Assert.True(OperatingSystem.IsWindows(), "Symbolic links must be creatable off Windows: " + ex.Message);
            Assert.Skip("This Windows account lacks the symbolic-link privilege (SeCreateSymbolicLink); the junction and hard-link facts cover the unprivileged cases.");
        }

        FileSystemInfo made = directory ? new DirectoryInfo(link) : new FileInfo(link);
        Assert.NotNull(made.LinkTarget);
    }

    /// <summary>A hard link needs no privilege on NTFS or on POSIX, so this never skips.</summary>
    private static void MakeHardLink(string link, string existing)
    {
        var made = OperatingSystem.IsWindows()
            ? RunTool("cmd.exe", "/c mklink /H \"" + link + "\" \"" + existing + "\"")
            : RunTool("ln", string.Empty, existing, link);
        Assert.True(made, "The hard link could not be created.");
    }

    /// <summary>A directory junction needs no privilege on Windows, so this never skips there.</summary>
    private static void MakeJunction(string link, string targetDirectory)
    {
        Assert.True(RunTool("cmd.exe", "/c mklink /J \"" + link + "\" \"" + targetDirectory + "\""), "The junction could not be created.");
    }

    /// <summary>Proves a directory link really is traversable before the refusal is asserted: a file created through it shows in the target.</summary>
    private static void AssertTraversable(string link, string targetDirectory)
    {
        Assert.True(Directory.Exists(link));
        var probe = Guid.NewGuid().ToString("N") + ".probe";
        File.WriteAllText(Path.Combine(link, probe), "x");
        Assert.True(File.Exists(Path.Combine(targetDirectory, probe)));
    }

    [Fact]
    public void AliasMap_AFileLinkedBundlePath_ThatPointsAtTheMap_IsRefused()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-maplink-");
        try
        {
            var real = Path.Combine(root.FullName, "real.txt");
            var link = Path.Combine(root.FullName, "b.json");
            MakeSymbolicLink(link, real, directory: false);

            var options = DiagnosticsBundle.ParseArgs(new[] { link, "--alias-map", real }).Options!;
            Assert.Contains("different file", DiagnosticsBundle.CheckAliasMap(options), StringComparison.Ordinal);

            /* The map path itself a link to the bundle's real location, which does not exist yet. */
            var mapLink = Path.Combine(root.FullName, "m.txt");
            var bundle = Path.Combine(root.FullName, "out.json");
            MakeSymbolicLink(mapLink, bundle, directory: false);
            var second = DiagnosticsBundle.ParseArgs(new[] { bundle, "--alias-map", mapLink }).Options!;
            Assert.Contains("different file", DiagnosticsBundle.CheckAliasMap(second), StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void AliasMap_ADirectorySymlink_ThatHoldsTheBundle_IsRefused()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-dirlink-");
        try
        {
            var dir = Directory.CreateDirectory(Path.Combine(root.FullName, "dir"));
            var dirLink = Path.Combine(root.FullName, "dirlink");
            MakeSymbolicLink(dirLink, dir.FullName, directory: true);
            AssertTraversable(dirLink, dir.FullName);

            var third = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(dir.FullName, "x.json"), "--alias-map", Path.Combine(dirLink, "x.json") }).Options!;
            Assert.Contains("different file", DiagnosticsBundle.CheckAliasMap(third), StringComparison.Ordinal);

            var reversed = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(dirLink, "x.json"), "--alias-map", Path.Combine(dir.FullName, "x.json") }).Options!;
            Assert.Contains("different file", DiagnosticsBundle.CheckAliasMap(reversed), StringComparison.Ordinal);

            /* A different file in the linked directory is not refused. */
            var other = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(dir.FullName, "x.json"), "--alias-map", Path.Combine(dirLink, "y.txt") }).Options!;
            Assert.Null(DiagnosticsBundle.CheckAliasMap(other));
        }
        finally
        {
            /* Delete the link itself first so the recursive delete never walks into its target. */
            var linkPath = Path.Combine(root.FullName, "dirlink");
            if (new DirectoryInfo(linkPath).LinkTarget is not null)
            {
                Directory.Delete(linkPath, recursive: false);
            }

            root.Delete(recursive: true);
        }
    }

    [Fact]
    [System.Runtime.Versioning.UnsupportedOSPlatform("windows")]
    public void AliasMap_APathComponent_ThatExistsButCannotBeInspected_IsRefused()
    {
        Assert.SkipWhen(OperatingSystem.IsWindows(), "Denying a directory by mode needs POSIX permissions.");
        Assert.SkipWhen(geteuid() == 0, "Root can inspect a directory whose mode is 000.");
        var root = Directory.CreateTempSubdirectory("darling-bundle-denied-");
        var denied = Path.Combine(root.FullName, "A");
        try
        {
            Directory.CreateDirectory(Path.Combine(denied, "sub"));
            File.SetUnixFileMode(denied, UnixFileMode.None);

            /* Precondition: inspecting a child of the denied directory really throws. */
            Assert.Throws<UnauthorizedAccessException>(() => File.GetAttributes(Path.Combine(denied, "sub")));

            var options = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(root.FullName, "b.json"), "--alias-map", Path.Combine(denied, "sub", "x.json") }).Options!;
            var problem = DiagnosticsBundle.CheckAliasMap(options);
            Assert.NotNull(problem);
            Assert.Contains("could not be resolved (a path component could not be inspected)", problem, StringComparison.Ordinal);
            Assert.Contains("cannot prove the map is a different file", problem, StringComparison.Ordinal);

            /* A component that does not exist yet is still compared as written. */
            var fresh = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(root.FullName, "b.json"), "--alias-map", Path.Combine(root.FullName, "missing", "x.json") }).Options!;
            Assert.Null(DiagnosticsBundle.CheckAliasMap(fresh));
        }
        finally
        {
            File.SetUnixFileMode(denied, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            root.Delete(recursive: true);
        }
    }

    [System.Runtime.InteropServices.DllImport("libc")]
    private static extern uint geteuid();

    [Fact]
    public void AliasMap_AJunction_ThatHoldsTheBundle_IsRefused()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Junctions are a Windows feature.");
        var root = Directory.CreateTempSubdirectory("darling-bundle-junction-");
        try
        {
            var dir = Directory.CreateDirectory(Path.Combine(root.FullName, "dir"));
            var junction = Path.Combine(root.FullName, "junction");
            MakeJunction(junction, dir.FullName);
            AssertTraversable(junction, dir.FullName);

            var viaJunction = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(dir.FullName, "x.json"), "--alias-map", Path.Combine(junction, "x.json") }).Options!;
            Assert.Contains("different file", DiagnosticsBundle.CheckAliasMap(viaJunction), StringComparison.Ordinal);

            var reversed = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(junction, "x.json"), "--alias-map", Path.Combine(dir.FullName, "x.json") }).Options!;
            Assert.Contains("different file", DiagnosticsBundle.CheckAliasMap(reversed), StringComparison.Ordinal);

            var other = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(dir.FullName, "x.json"), "--alias-map", Path.Combine(junction, "y.txt") }).Options!;
            Assert.Null(DiagnosticsBundle.CheckAliasMap(other));
        }
        finally
        {
            /* Delete the junction itself first so the recursive delete never walks into its target. */
            var junctionPath = Path.Combine(root.FullName, "junction");
            if (Directory.Exists(junctionPath))
            {
                Directory.Delete(junctionPath, recursive: false);
            }

            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task AliasMap_WrittenThroughAPlantedLink_ReplacesTheLink_AndNeverTheVictim()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-mapwrite-");
        try
        {
            var victim = Path.Combine(root.FullName, "victim.txt");
            await File.WriteAllTextAsync(victim, "VICTIM");
            var map = Path.Combine(root.FullName, "map.txt");
            MakeSymbolicLink(map, victim, directory: false);
            Assert.Equal("VICTIM", await File.ReadAllTextAsync(map));

            var logs = Directory.CreateDirectory(Path.Combine(root.FullName, "logs")).FullName;
            var (exit, _) = await RunAsync(Path.Combine(root.FullName, "b.json"), "--config", WriteConfig(root), "--log-dir", logs, "--alias-map", map, "--force");
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.StoreUnreachable, exit);
            Assert.Equal("VICTIM", await File.ReadAllTextAsync(victim));
            Assert.Null(new FileInfo(map).LinkTarget);
            Assert.StartsWith("DO NOT ATTACH", await File.ReadAllTextAsync(map), StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task AliasMap_WrittenOverAHardLink_ReplacesTheEntry_AndNeverTheVictim()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-maphard-");
        try
        {
            var victim = Path.Combine(root.FullName, "victim.txt");
            await File.WriteAllTextAsync(victim, "VICTIM");
            var map = Path.Combine(root.FullName, "map.txt");
            MakeHardLink(map, victim);

            /* Precondition: the two names are one file, so a write through either would show in the other. */
            Assert.Equal("VICTIM", await File.ReadAllTextAsync(map));
            await File.AppendAllTextAsync(map, "!");
            Assert.Equal("VICTIM!", await File.ReadAllTextAsync(victim));
            await File.WriteAllTextAsync(victim, "VICTIM");

            var logs = Directory.CreateDirectory(Path.Combine(root.FullName, "logs")).FullName;
            var (exit, _) = await RunAsync(Path.Combine(root.FullName, "b.json"), "--config", WriteConfig(root), "--log-dir", logs, "--alias-map", map, "--force");
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.StoreUnreachable, exit);
            Assert.Equal("VICTIM", await File.ReadAllTextAsync(victim));
            Assert.StartsWith("DO NOT ATTACH", await File.ReadAllTextAsync(map), StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task AliasMap_EmptyPath_ExitsOne()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-mapempty-");
        try
        {
            var (exit, err) = await RunAsync(Path.Combine(root.FullName, "b.json"), "--config", WriteConfig(root), "--alias-map", "");
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.ConfigError, exit);
            Assert.Contains("--alias-map needs a path", err, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(root.FullName, "b.json")));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }
}
