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
}
