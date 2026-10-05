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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A server's password may be an <c>env:</c> / <c>file:</c> reference, but never one that points at Darling's own
/// configuration or secrets. The rule lives in the shared core, so the MCP tool is covered as well as the web route.
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class OwnedSecretReferenceRefusalTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "darling-ref-" + Guid.NewGuid().ToString("N"));
    private readonly DarlingOwnedSet _before = DarlingOwnedSecrets.Current;
    private readonly string _owned;

    public OwnedSecretReferenceRefusalTests()
    {
        Directory.CreateDirectory(Path.Combine(_root, "own"));
        Directory.CreateDirectory(Path.Combine(_root, "other"));
        _owned = Path.Combine(_root, "own");
        File.WriteAllText(Path.Combine(_owned, "secret.txt"), "x");
        DarlingOwnedSecrets.Set(new DarlingOwnedSet(new[] { _owned }, new[] { "DARLING_CONFIG" }));
    }

    public void Dispose()
    {
        DarlingOwnedSecrets.Set(_before);
        try
        {
            Directory.Delete(_root, true);
        }
        catch (IOException)
        {
            /* best effort */
        }
    }

    private static string? Check(string? password) => DarlingOwnedSecrets.ReferenceRefusal(password);

    private static string Body(string password) =>
        JsonSerializer.Serialize(new[] { new { host = "sql01", auth = "SQL", username = "monitor", password } });

    [Fact]
    public void TheMcpCore_RefusesAnOwnedFileAndAnOwnedEnvReference()
    {
        foreach (var pw in new[] { "file:" + Path.Combine(_owned, "secret.txt"), "env:DARLING_CONFIG" })
        {
            var (entries, invalid, whole) = DarlingMcpServerAdminTools.ParseRequest(Body(pw), isWindows: false);
            Assert.Null(whole);
            Assert.Empty(entries);
            var r = Assert.Single(invalid);
            Assert.Equal(DarlingOwnedSecrets.ReferenceRefusalText, r.Detail);
        }
    }

    [Fact]
    public void TheMcpCore_AcceptsAnUnrelatedReference_AndLeavesLiteralsAlone()
    {
        foreach (var pw in new[] { "file:" + Path.Combine(_root, "other", "pw"), "env:SOME_OTHER_VAR" })
        {
            var (entries, invalid, _) = DarlingMcpServerAdminTools.ParseRequest(Body(pw), isWindows: false);
            Assert.Empty(invalid);
            Assert.Single(entries);
        }

        Assert.Null(Check("hunter2"));
        Assert.Null(Check(""));
        Assert.Null(Check(null));
        Assert.Null(Check("   "));
    }

    [Fact]
    public void AnUnrelatedReference_IsAccepted()
    {
        Assert.Null(Check("env:SOME_OTHER_VAR"));
        Assert.Null(Check("file:" + Path.Combine(_root, "other", "pw")));
        Assert.Null(Check("file:" + _owned + "-sibling" + Path.DirectorySeparatorChar + "pw"));
    }

    [Fact]
    public void TheOwnedDirectoryItselfAndEverythingUnderIt_IsRefused()
    {
        Assert.NotNull(Check("file:" + _owned));
        Assert.NotNull(Check("file:" + Path.Combine(_owned, "missing", "deep.txt")));
    }

    [Fact]
    public void EnvNames_CompareOrdinally_OnThisPlatform()
    {
        Assert.NotNull(Check("env:DARLING_CONFIG"));
        Assert.Equal(OperatingSystem.IsWindows(), Check("env:darling_config") != null);
    }

    [Theory]
    [InlineData("relative/path")]
    [InlineData("~/secret")]
    [InlineData("/etc/../etc/hostname")]
    [InlineData("//server/share/x")]
    [InlineData("\\\\server\\share\\x")]
    [InlineData("\\\\?\\C:\\x")]
    [InlineData("\\\\.\\C:\\x")]
    [InlineData("C:\\x\\f.txt:stream")]
    [InlineData("/proc/self/root/etc/hostname")]
    [InlineData("/sys/kernel/x")]
    public void EachAliasForm_IsRefused_WithTheOneSentence(string path)
    {
        Assert.Equal(DarlingOwnedSecrets.ReferenceRefusalText, Check("file:" + path));
    }

    [Fact]
    public void ASymlinkIntoTheOwnedDirectory_IsRefused()
    {
        var link = Path.Combine(_root, "other", "alias");
        try
        {
            Directory.CreateSymbolicLink(link, _owned);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            return;
        }

        Assert.Equal(DarlingOwnedSecrets.ReferenceRefusalText, Check("file:" + Path.Combine(link, "secret.txt")));
    }

    [Fact]
    public void ASymlinkCycle_IsRefused()
    {
        var a = Path.Combine(_root, "other", "a");
        var b = Path.Combine(_root, "other", "b");
        try
        {
            Directory.CreateSymbolicLink(a, b);
            Directory.CreateSymbolicLink(b, a);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            return;
        }

        Assert.NotNull(Check("file:" + Path.Combine(a, "x")));
    }

    [Fact]
    public void EveryRefusal_IsOneSentence_NamingNoPathAndNoVariable()
    {
        var all = new[] { "file:" + _owned, "env:DARLING_CONFIG", "file:~/x", "file:/proc/1/environ" }
            .Select(Check).Distinct().ToList();
        var text = Assert.Single(all);
        Assert.DoesNotContain(_owned, text, StringComparison.Ordinal);
        Assert.DoesNotContain("DARLING_CONFIG", text, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryEnvironmentVariableTheServiceReads_IsInTheOwnedSet()
    {
        var repo = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(ThisFile())!, ".."));
        var missing = new List<string>();
        foreach (var dir in new[] { "PerformanceMonitor.Darling.Service", "PerformanceMonitor.Darling.Storage" })
        {
            foreach (var file in Directory.EnumerateFiles(Path.Combine(repo, dir), "*.cs", SearchOption.AllDirectories)
                .Where(f => !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)))
            {
                var text = File.ReadAllText(file);
                foreach (Match m in Regex.Matches(text, "GetEnvironmentVariable\\(\\s*\"([^\"]+)\""))
                {
                    if (!DarlingOwnedSecrets.ServiceEnvNames.Contains(m.Groups[1].Value))
                    {
                        missing.Add(m.Groups[1].Value);
                    }
                }

                foreach (Match m in Regex.Matches(text, "GetEnvironmentVariable\\(\\s*([A-Z][A-Za-z0-9_]*)\\s*[,)]"))
                {
                    var lit = Regex.Match(text, "\\b" + m.Groups[1].Value + "\\s*=\\s*\"([^\"]+)\"");
                    if (lit.Success && !DarlingOwnedSecrets.ServiceEnvNames.Contains(lit.Groups[1].Value))
                    {
                        missing.Add(lit.Groups[1].Value);
                    }
                }
            }
        }

        Assert.Empty(missing.Distinct());
    }

    private static string ThisFile([CallerFilePath] string path = "") => path;
}
