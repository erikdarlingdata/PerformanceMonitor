/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>Keeps the owned set (<see cref="DarlingOwnedSecrets"/>) complete as the service grows.</summary>
[Collection("darling-owned-secrets")]
public sealed class DarlingOwnedSecretsCensusTests
{
    private static readonly string[] s_serviceDir = { "Darling", "PerformanceMonitor.Darling.Service" };

    private static DarlingConfig Populated()
    {
        var config = new DarlingConfig();
        Fill(config, 0);
        return config;
    }

    /* Sets every writable string property in the graph to env:<Type>.<Prop> so the walk's reach is measured, not assumed. */
    private static void Fill(object node, int depth)
    {
        if (depth > 6)
        {
            return;
        }

        foreach (var p in node.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (p.GetIndexParameters().Length != 0 || !p.CanWrite || p.GetCustomAttribute<System.Text.Json.Serialization.JsonIgnoreAttribute>() is not null)
            {
                continue;
            }

            var t = p.PropertyType;
            if (t == typeof(string))
            {
                p.SetValue(node, $"env:{node.GetType().Name}.{p.Name}");
            }
            else if (t.IsClass && t.Assembly == typeof(DarlingConfig).Assembly && t.GetConstructor(Type.EmptyTypes) is not null)
            {
                var child = p.GetValue(node) ?? Activator.CreateInstance(t)!;
                p.SetValue(node, child);
                Fill(child, depth + 1);
            }
            else if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(List<>)
                     && t.GetGenericArguments()[0].Assembly == typeof(DarlingConfig).Assembly
                     && t.GetGenericArguments()[0].GetConstructor(Type.EmptyTypes) is not null)
            {
                var list = (IList)(p.GetValue(node) ?? Activator.CreateInstance(t)!);
                if (list.Count == 0)
                {
                    list.Add(Activator.CreateInstance(t.GetGenericArguments()[0])!);
                }

                p.SetValue(node, list);
                Fill(list[0]!, depth + 1);
            }
        }
    }

    [Fact]
    public void Walk_CoversTheEntriesTheOldCaptureMissed()
    {
        var found = DarlingOwnedSecrets.CollectReferences(Populated());
        foreach (var expected in new[]
        {
            "env:WebOidcConfig.ClientSecret",
            "env:WebOidcConfig.EncryptedClientSecret",
            "env:MonitoredServer.EncryptedPassword",
            "env:MonitoredServer.RemediationEncryptedPassword",
            "env:SmtpConfig.Password",
            "env:SmtpConfig.EncryptedPassword",
            "env:WebTlsConfig.EncryptedPfxPassword",
            "env:WebTlsConfig.PfxPassword",
        })
        {
            Assert.Contains(expected, found);
        }

        Assert.Contains(found, f => f.StartsWith("env:", StringComparison.Ordinal) && f.EndsWith(".EncryptedToken", StringComparison.Ordinal));
    }

    [Fact]
    public void Parse_CapturesReferencesAsWritten_NotResolved()
    {
        var dir = Directory.CreateTempSubdirectory().FullName;
        var secretFile = Path.Combine(dir, "store-conn.txt");
        File.WriteAllText(secretFile, "Host=h;Password=resolved-value");
        const string envName = "DARLING_TEST_OIDC_SECRET_OWNEDSET";
        Environment.SetEnvironmentVariable(envName, "oidc-resolved");
        try
        {
            var json = "{\"postgres\":{\"connectionString\":\"file:" + secretFile.Replace("\\", "\\\\") + "\"},"
                     + "\"web\":{\"network\":{\"oidc\":{\"clientSecret\":\"env:" + envName + "\"}}}}";
            var config = DarlingConfig.Parse(json);
            Assert.Equal("Host=h;Password=resolved-value", config.Postgres.ConnectionString);
            Assert.Contains("file:" + secretFile, config.SecretReferencesAsWritten);
            Assert.Contains("env:" + envName, config.SecretReferencesAsWritten);
            Assert.DoesNotContain(config.SecretReferencesAsWritten, r => r.Contains("resolved", StringComparison.Ordinal));

            var set = DarlingOwnedSecrets.Compute(config, Path.Combine(dir, "darling.json"));
            Assert.Contains(secretFile, set.Paths);
            Assert.Contains(envName, set.EnvNames);
        }
        finally
        {
            Environment.SetEnvironmentVariable(envName, null);
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void Compute_IncludesTheManagedStoreFilesInTheDataDirectoryParent()
    {
        var root = Path.Combine(Path.GetTempPath(), "darling-owned-" + Guid.NewGuid().ToString("N"));
        var config = new DarlingConfig();
        config.Postgres.DataDirectory = Path.Combine(root, "pgdata");
        var set = DarlingOwnedSecrets.Compute(config, Path.Combine(root, "darling.json"));

        Assert.Contains(config.Postgres.DataDirectory, set.Paths);
        foreach (var name in new[]
        {
            "pg-credential.dpapi", "pg-admin-credential.dpapi", "pg-viewer-credential.dpapi",
            "pg-mcp-credential.dpapi", "server.crt", "server.key", "pg.log",
        })
        {
            Assert.Contains(Path.Combine(root, name), set.Paths);
        }
    }

    [Fact]
    public void Compute_ADefaultManagedConfiguration_OwnsTheResolvedStoreDirectorysFiles()
    {
        var root = Path.Combine(Path.GetTempPath(), "darling-owned-" + Guid.NewGuid().ToString("N"));
        var config = new DarlingConfig();
        config.Postgres.Managed = true;
        config.Postgres.DataDirectory = null;
        var set = DarlingOwnedSecrets.Compute(config, Path.Combine(root, "darling.json"));

        var resolved = DarlingManagedPostgres.ResolveDataDirectory(config.Postgres);
        Assert.Contains(resolved, set.Paths);
        Assert.Contains(DarlingManagedPostgres.CredentialPathFor(resolved), set.Paths);
        Assert.Contains(DarlingManagedPostgres.AdminCredentialPathFor(resolved), set.Paths);
        Assert.Contains(Path.Combine(Path.GetDirectoryName(resolved)!, "server.key"), set.Paths);
        Assert.Contains(Path.Combine(Path.GetDirectoryName(resolved)!, "pg.log"), set.Paths);
    }

    [Fact]
    public void Compute_OwnsTheComposeCredentialDirectory_AndTheLogHashKeyDirectory()
    {
        var root = Path.Combine(Path.GetTempPath(), "darling-owned-" + Guid.NewGuid().ToString("N"));
        var configPath = Path.Combine(root, "darling.json");
        var config = new DarlingConfig();
        config.Postgres.Managed = false;
        var set = DarlingOwnedSecrets.Compute(config, configPath);

        Assert.Contains(DarlingManagedRoles.ComposeStoreCredentialDirectory, set.Paths);
        Assert.Contains(DarlingLogHashKeyFile.DirectoryFor(config, configPath), set.Paths);
    }

    [Fact]
    public void Holder_SetThenCurrent()
    {
        var before = DarlingOwnedSecrets.Current;
        try
        {
            var set = new DarlingOwnedSet(new[] { "/x" }, new[] { "Y" });
            DarlingOwnedSecrets.Set(set);
            Assert.Same(set, DarlingOwnedSecrets.Current);
        }
        finally
        {
            DarlingOwnedSecrets.Set(before);
        }
    }

    [Fact]
    public void EveryEnvironmentVariableTheServiceReads_IsInTheOwnedSet()
    {
        var dir = PathTo(s_serviceDir);
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var file in Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories))
        {
            if (file.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar) || file.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar))
            {
                continue;
            }

            foreach (Match m in Regex.Matches(File.ReadAllText(file), "GetEnvironmentVariable\\(\"([^\"]+)\""))
            {
                names.Add(m.Groups[1].Value);
            }
        }

        Assert.NotEmpty(names);
        var set = DarlingOwnedSecrets.Compute(new DarlingConfig(), "darling.json");
        foreach (var n in names)
        {
            Assert.Contains(n, set.EnvNames, StringComparer.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void EverySecretLikeConfigProperty_IsReachedByTheWalk()
    {
        var src = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingConfig.cs");
        var props = Regex.Matches(src, "\\[JsonPropertyName\\(\"[^\"]+\"\\)\\]\\s*public string\\? (\\w+)\\s*\\{")
            .Select(m => m.Groups[1].Value)
            .Where(n => Regex.IsMatch(n, "secret|token|password|path|key|cert|connection", RegexOptions.IgnoreCase))
            .Distinct()
            .ToList();
        Assert.NotEmpty(props);

        var found = DarlingOwnedSecrets.CollectReferences(Populated());
        foreach (var prop in props)
        {
            Assert.Contains(found, f => f.EndsWith("." + prop, StringComparison.Ordinal));
        }
    }

    /* ═══════════ which test classes can change the owned set while another reads it (#5768) ═══════════ */

    private const string CollectionName = "darling-owned-secrets";

    private static readonly string[] s_testsDir = { "Darling", "Darling.Tests" };

    /// <summary>A top-level class declaration and the attribute lines right above it. The test files declare their classes
    /// at column 0 under a file-scoped namespace, so one match starts a class and the next match ends it.</summary>
    private static readonly Regex s_topLevelClass = new(
        @"^((?:\[[^\r\n]*\]\r?\n)*)(?:public|internal)\s+(?:(?:sealed|static|abstract|partial)\s+)*class\s+(\w+)",
        RegexOptions.Multiline | RegexOptions.CultureInvariant);

    /// <summary>A method declaration at member indent in a service source file.</summary>
    private static readonly Regex s_memberDeclaration = new(
        @"^    (?:(?:public|internal|private|protected|static|async|override)\s+)+[^=;\r\n(]*?\b(\w+)\s*(?:<[^>\r\n]*>)?\s*\(",
        RegexOptions.Multiline | RegexOptions.CultureInvariant);

    private static readonly Regex s_setsTheOwnedSet = new(@"\bDarlingOwnedSecrets\s*\.\s*Set\s*\(", RegexOptions.CultureInvariant);

    /// <summary>A class that asserts what the owned set refuses, or saves and puts back the current set.</summary>
    private static readonly Regex s_readsTheOwnedSet = new(
        @"\bDarlingOwnedSecrets\s*\.\s*(?:Current|ReferenceRefusalText|ReferenceRefusal|Unpopulated)\b|own configuration or secrets",
        RegexOptions.CultureInvariant);

    private static readonly Regex s_loadsAConfig = new(@"\bDarlingConfig\s*\.\s*Load\s*\(", RegexOptions.CultureInvariant);

    private static readonly Regex s_cliCall = new(@"\bDarlingCliCommands\s*\.\s*(\w+)\s*\(", RegexOptions.CultureInvariant);

    private static readonly Regex s_construction = new(@"\bnew\s+(\w+)\s*\(", RegexOptions.CultureInvariant);

    private sealed record SourceFile(string Name, string Text);

    private sealed record TestClass(string Name, bool InCollection, string Text);

    private static IEnumerable<SourceFile> SourceFiles(string[] directory)
    {
        var separator = Path.DirectorySeparatorChar;
        foreach (var file in Directory.EnumerateFiles(PathTo(directory), "*.cs", SearchOption.AllDirectories))
        {
            if (file.Contains($"{separator}obj{separator}", StringComparison.Ordinal)
                || file.Contains($"{separator}bin{separator}", StringComparison.Ordinal))
            {
                continue;
            }

            yield return new SourceFile(Path.GetFileName(file), File.ReadAllText(file));
        }
    }

    private static IEnumerable<TestClass> TestClasses()
    {
        foreach (var file in SourceFiles(s_testsDir))
        {
            var declarations = s_topLevelClass.Matches(file.Text);
            for (var i = 0; i < declarations.Count; i++)
            {
                var end = i + 1 < declarations.Count ? declarations[i + 1].Index : file.Text.Length;
                yield return new TestClass(
                    declarations[i].Groups[2].Value,
                    declarations[i].Groups[1].Value.Contains($"[Collection(\"{CollectionName}\")]", StringComparison.Ordinal),
                    file.Text[declarations[i].Index..end]);
            }
        }
    }

    /// <summary>The name of the member declared last before <paramref name="index"/> in <paramref name="text"/>.</summary>
    private static string EnclosingMember(string text, int index) =>
        s_memberDeclaration.Matches(text).LastOrDefault(m => m.Index < index)?.Groups[1].Value ?? "";

    /// <summary>
    /// The product calls that reach <see cref="DarlingConfig.Load"/>: the <see cref="DarlingCliCommands"/> members that
    /// call it, directly or through another member of that class (matched by name, so an overload of a member that loads
    /// counts too), and the service types outside it whose source calls it (the worker and the two host services).
    /// </summary>
    private static (HashSet<string> CliMembers, HashSet<string> Types) LoadingProductCalls()
    {
        var cliMembers = new HashSet<string>(StringComparer.Ordinal);
        var types = new HashSet<string>(StringComparer.Ordinal);
        foreach (var file in SourceFiles(s_serviceDir))
        {
            if (file.Name == "DarlingCliCommands.cs")
            {
                var declarations = s_memberDeclaration.Matches(file.Text);
                var spans = new List<(string Name, string Body)>();
                for (var i = 0; i < declarations.Count; i++)
                {
                    var end = i + 1 < declarations.Count ? declarations[i + 1].Index : file.Text.Length;
                    spans.Add((declarations[i].Groups[1].Value, file.Text[declarations[i].Index..end]));
                }

                foreach (var span in spans.Where(s => s_loadsAConfig.IsMatch(s.Body)))
                {
                    cliMembers.Add(span.Name);
                }

                for (var grew = true; grew;)
                {
                    grew = false;
                    foreach (var span in spans.Where(s => !cliMembers.Contains(s.Name)))
                    {
                        if (cliMembers.Any(name => Regex.IsMatch(span.Body, $@"(?<![\w.])(?:DarlingCliCommands\s*\.\s*)?{name}\s*\(")))
                        {
                            grew = cliMembers.Add(span.Name) || grew;
                        }
                    }
                }
            }
            else if (file.Name != "DarlingConfig.cs" && s_loadsAConfig.IsMatch(file.Text))
            {
                /* #7024: the type, not the partial: DarlingWorker.Bootstrap.cs declares DarlingWorker. */
                types.Add(Path.GetFileNameWithoutExtension(file.Name).Split('.')[0]);
            }
        }

        return (cliMembers, types);
    }

    private static bool ReachesALoad(string text, (HashSet<string> CliMembers, HashSet<string> Types) calls) =>
        s_loadsAConfig.IsMatch(text)
        || s_cliCall.Matches(text).Any(m => calls.CliMembers.Contains(m.Groups[1].Value))
        || s_construction.Matches(text).Any(m => calls.Types.Contains(m.Groups[1].Value));

    /// <summary>
    /// #5768: the product changes the owned set in one place, <see cref="DarlingConfig.Load"/>, which publishes the set it
    /// computed from the file it loaded. So any code that reaches <c>Load</c> changes the set for the whole process, and
    /// the census below finds those test classes by looking for <c>Load</c> and what calls it. Inside the service process
    /// <c>Load</c> is called only with no argument (the worker and the two host services, each reading the one file the
    /// service runs on), so a set published there always comes from that file. A call with a path is a CLI verb in
    /// <see cref="DarlingCliCommands"/>, which runs as its own process. A load of any other file inside the service would
    /// publish a set for a file the service does not run on: it must compute without publishing.
    /// </summary>
    [Fact]
    public void TheProductPublishesTheOwnedSet_OnlyFromDarlingConfigLoad_AndTheServiceLoadsOnlyItsOwnFile()
    {
        var writers = SourceFiles(s_serviceDir)
            .SelectMany(f => s_setsTheOwnedSet.Matches(f.Text).Select(m => $"{f.Name}:{EnclosingMember(f.Text, m.Index)}"))
            .ToList();
        Assert.Equal(new[] { "DarlingConfig.cs:Load" }, writers);

        var serviceLoads = SourceFiles(s_serviceDir)
            .Where(f => f.Name != "DarlingCliCommands.cs")
            .SelectMany(f => s_loadsAConfig.Matches(f.Text).Select(m => (f.Name, Call: f.Text.Substring(m.Index, m.Length + 1))))
            .ToList();
        Assert.NotEmpty(serviceLoads);
        Assert.All(serviceLoads, load => Assert.True(
            load.Call.EndsWith("Load()", StringComparison.Ordinal),
            $"{load.Name} loads a configuration file with an argument inside the service process. DarlingConfig.Load publishes "
            + "the owned set it computes, so the service would refuse references by a file it does not run on."));
    }

    /// <summary>
    /// #5768: the classes in the <c>darling-owned-secrets</c> collection set the owned set and then assert what it refuses.
    /// Test classes outside it change the set without calling <see cref="DarlingOwnedSecrets.Set"/>: every
    /// <see cref="DarlingConfig.Load"/> they reach publishes the set computed from their own file. One such load landed
    /// between <c>DarlingSecretsOwnedReferenceTests</c>' set and its next resolve, and a store row naming a reference the
    /// file declares for another server id resolved. Several of those classes are in other collections
    /// (<c>live-postgres</c>, <c>pg-cluster-roles</c>, <c>darling-config-env</c>), and a class is in one collection only,
    /// so this collection runs alone instead: xUnit starts it after every parallel collection has finished.
    /// </summary>
    [Fact]
    public void TheCollection_RunsAlone_BecauseTestClassesOutsideItPublishTheOwnedSet()
    {
        var calls = LoadingProductCalls();
        Assert.Contains("ValidateConfigAsync", calls.CliMembers);
        Assert.Contains("DarlingWorker", calls.Types);

        var publishers = TestClasses()
            .Where(c => !c.InCollection && ReachesALoad(c.Text, calls))
            .Select(c => c.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();
        Assert.True(
            publishers.Count > 0,
            "The scan found no test class outside the collection that reaches DarlingConfig.Load; it has stopped seeing the "
            + "test sources or the product calls.");

        var definition = typeof(DarlingOwnedSecretsCensusTests).Assembly.GetTypes()
            .Select(t => t.GetCustomAttribute<CollectionDefinitionAttribute>())
            .SingleOrDefault(a => a?.Name == CollectionName);
        Assert.True(
            definition is { DisableParallelization: true },
            $"The \"{CollectionName}\" collection must be declared with DisableParallelization = true. These test classes outside "
            + "it reach DarlingConfig.Load, which publishes the owned set, and can run beside it: "
            + string.Join(", ", publishers));
    }

    /// <summary>
    /// #5768: a class that sets the owned set, or asserts what it refuses, is in the <c>darling-owned-secrets</c>
    /// collection, so it runs alone with the set it put there.
    /// </summary>
    [Fact]
    public void EveryTestClassThatSetsOrReadsTheOwnedSet_IsInTheCollection()
    {
        var users = TestClasses()
            .Where(c => s_setsTheOwnedSet.IsMatch(c.Text) || s_readsTheOwnedSet.IsMatch(c.Text))
            .ToList();
        Assert.True(
            users.Count >= 2,
            $"The scan found {users.Count} test classes that set or read the owned set; it has stopped seeing the test sources.");

        var outside = users.Where(c => !c.InCollection).Select(c => c.Name).OrderBy(n => n, StringComparer.Ordinal).ToList();
        Assert.True(
            outside.Count == 0,
            $"These test classes set or read the owned set without [Collection(\"{CollectionName}\")]: " + string.Join(", ", outside));
    }
}
