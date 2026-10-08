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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5602: a live test changes only Postgres objects that no other test can see, or it runs in the
/// <c>pg-cluster-roles</c> collection, which runs alone.
///
/// <para>Roles, role memberships, database-level grants, <c>ALTER ROLE ... SET</c>, <c>ALTER DATABASE ... SET</c> and
/// <c>ALTER SYSTEM</c> are stored for the whole cluster, not for the scratch database a test made. Two tests that
/// wrote one of them at the same time failed with "tuple concurrently updated" (#5560), and the product's
/// provisioning batch writes FIXED role names, so every test that runs it shares them. This census reads each
/// test class that reaches the <c>DARLING_TEST_PG</c> cluster (the same reach test
/// <see cref="LivePostgresCollectionHygieneTests"/> uses) and fails when its code (comments stripped) does one of
/// these and the class is neither in <see cref="PgClusterRolesCollection"/> nor on <see cref="AllowList"/>:</para>
/// <list type="bullet">
/// <item><description><c>CREATE/ALTER/DROP ROLE</c>, <c>DROP OWNED BY</c>, <c>GRANT ... TO</c> or <c>REVOKE ... FROM</c>
/// with a LITERAL role name. A name built with <c>{...}</c> from a per-run token is fine: nobody else writes
/// that catalog row.</description></item>
/// <item><description><c>ALTER SYSTEM SET/RESET</c>, <c>ALTER DATABASE x SET</c> and <c>GRANT/REVOKE ... ON DATABASE x</c>
/// with a literal database name (a scratch name built with <c>{...}</c> is fine).</description></item>
/// <item><description>A call of a product entry point that writes the fixed roles: the provisioning methods, the
/// statement_timeout re-assert, or the <c>DarlingManagedPostgres.*RoleName</c> constants.</description></item>
/// </list>
/// A lone SQL string that a test only ASSERTS on (a line with <c>Assert.</c> or <c>Regex</c> on it) is not an
/// execution and is skipped. Every allow-list entry carries its reason, and an entry that no longer matches
/// anything fails too, so the list cannot go stale.
/// </summary>
/* #1776 own-store: this class reads test source files and never connects to a database; the quoted store names in it are
   patterns it searches for. */
[Trait("Stage", "Guard")]
public sealed class PgClusterObjectCensusTests
{
    private const string CollectionName = "pg-cluster-roles";

    private static readonly string[] SharedClusterReach = { "\"DARLING_TEST_PG\"", "ScratchPostgres.CreateAsync(", "LivePostgresStoreFixture" };

    /// <summary>Classes that reach the shared cluster, match a rule, and are safe. Every entry says why.</summary>
    internal static readonly IReadOnlyDictionary<string, string> AllowList = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["PgStatementTextScrubLiveTests"] =
            "the ALTER ROLE text is a sample statement fed to the scrubber as data; the class runs no role DDL",
        ["StatementAnalysisReadCutLiveTests"] =
            "the CREATE ROLE text is a sample statement fed to the read cut as data; the class runs no role DDL",
        ["StoreLogRemaskLiveTests"] =
            "the ALTER ROLE text is a sample log line fed to the re-mask as data; the class runs no role DDL",
        ["StoreStatementStatsLiveTests"] =
            "the ALTER ROLE and ALTER SYSTEM text is sample statement text fed to the scrubber as data; every role and database this class really creates carries a per-run suffix",
        ["MigrationUpgradeLadderLiveTests"] =
            "creates the cluster's own bootstrap role name only when it is missing (CREATE ROLE darling ... IF NOT EXISTS); on a CI cluster it already exists, so the write never happens",
        ["AlertHistoryDismissTests"] =
            "builds the shipped provisioning text and retargets its viewer GRANT at a per-run role; the batch itself is never executed",
        ["ViewerStoreLoginRawReadLiveTests"] =
            "lifts two shipped GRANT statements out of the provisioning text and runs them retargeted at a per-run role",
        ["ServerAddViewerRoleLiveTests"] =
            "lifts the shipped viewer GRANT statements out of the provisioning text and runs them retargeted at a per-run role",
        ["ServerEditLiveTests"] =
            "lifts the shipped viewer GRANT statements out of the provisioning text and runs them retargeted at a per-run role",
        ["ServerTagMcpRoleLiveTests"] =
            "lifts the shipped mcp GRANT statements out of the provisioning text and runs them retargeted at a per-run role",
        ["ServerTagViewerRoleLiveTests"] =
            "lifts the shipped viewer GRANT statements out of the provisioning text and runs them retargeted at a per-run role",
        ["ComposeTempFileLimitLiveTests"] =
            "renders the shipped compose-store role statements and retargets them at a per-run role; the fixed roles are not written",
    };

    [Fact]
    public void TheCollectionThatRunsAlone_IsDefinedWithParallelizationOff()
    {
        var definition = typeof(PgClusterRolesCollection).GetCustomAttribute<CollectionDefinitionAttribute>();
        Assert.NotNull(definition);
        Assert.Equal(CollectionName, definition!.Name);
        Assert.True(definition.DisableParallelization,
            $"The '{CollectionName}' collection exists so its members run alone, after every parallel collection (#5602). "
            + "Without DisableParallelization it is only another collection that overlaps the rest, and the census below "
            + "would be pointing tests at a lock that does not exist.");
    }

    [Fact]
    public void EveryLiveClassThatChangesAClusterWideObjectUnderAFixedName_RunsInThePgClusterRolesCollection_OrIsAllowListed()
    {
        var offenders = new List<string>();
        var allowed = new HashSet<string>(StringComparer.Ordinal);
        foreach (var hit in ScanClasses())
        {
            if (hit.InCollection)
            {
                continue;
            }

            if (AllowList.ContainsKey(hit.ClassName))
            {
                allowed.Add(hit.ClassName);
                continue;
            }

            offenders.Add($"{hit.File}:{hit.Line}  {hit.ClassName}  [{hit.Rule}]  {hit.Text}");
        }

        Assert.True(offenders.Count == 0,
            "These test classes reach the DARLING_TEST_PG cluster and change a cluster-wide Postgres object (a role, a "
            + "grant, a role or database setting, ALTER SYSTEM) under a fixed name, or run a product entry point that "
            + "writes the fixed admin/viewer/mcp roles, yet they are not in the " + CollectionName + " collection:\n\n"
            + string.Join("\n", offenders)
            + "\n\nPick one, deliberately (#5602):\n"
            + $"  - Add [Collection(\"{CollectionName}\")] (a DisableParallelization collection: it runs alone) when the test must "
            + "write the fixed roles or the cluster settings.\n"
            + "  - Give the object a name built from the scratch database name or a per-run token ({...}), so no other test can see it.\n"
            + "  - Or add the class to PgClusterObjectCensusTests.AllowList with the reason it is safe.");
    }

    [Fact]
    public void EveryAllowListEntry_StillMatchesARuleInAClassThatReachesTheCluster()
    {
        var matched = ScanClasses().Where(h => !h.InCollection).Select(h => h.ClassName).ToHashSet(StringComparer.Ordinal);
        var stale = AllowList.Keys.Where(name => !matched.Contains(name)).OrderBy(name => name, StringComparer.Ordinal).ToList();

        Assert.True(stale.Count == 0,
            "These allow-list entries no longer match anything (the class was removed, joined the "
            + CollectionName + " collection, or stopped doing the thing). Delete them:\n" + string.Join("\n", stale));

        foreach (var (name, reason) in AllowList)
        {
            Assert.False(string.IsNullOrWhiteSpace(reason), $"allow-list entry {name} needs a reason");
        }
    }

    // ---- rules -------------------------------------------------------------------------------------------

    /// <summary>(rule name, pattern). A pattern matches one code line; a name built with <c>{</c> never matches.</summary>
    private static readonly (string Name, Regex Pattern)[] Rules =
    {
        ("role DDL with a literal name",
            new Regex(@"\b(?:CREATE|ALTER|DROP)\s+(?:ROLE|USER)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?(?!IF\b|NOT\b|EXISTS\b)[A-Za-z_]\w*", RegexOptions.Compiled)),
        ("DROP OWNED BY a literal role",
            new Regex(@"\bDROP\s+OWNED\s+BY\s+[A-Za-z_]\w*", RegexOptions.Compiled)),
        ("GRANT/REVOKE to or from a literal role",
            new Regex(@"\b(?:GRANT\b[^;""]*?\sTO|REVOKE\b[^;""]*?\sFROM)\s+(?!PUBLIC\b|pg_|CURRENT_USER\b|SESSION_USER\b)[A-Za-z_]\w*", RegexOptions.Compiled)),
        ("ALTER SYSTEM",
            new Regex(@"\bALTER\s+SYSTEM\s+(?:SET|RESET)\b", RegexOptions.Compiled)),
        ("ALTER DATABASE with a literal name",
            new Regex(@"\bALTER\s+DATABASE\s+[A-Za-z_]\w*\s+SET\b", RegexOptions.Compiled)),
        ("database-level grant on a literal database",
            new Regex(@"\bON\s+DATABASE\s+[A-Za-z_]\w*", RegexOptions.Compiled)),
        ("product entry point that writes the fixed roles",
            new Regex(@"\b(?:EnsureProvisionedAsync|EnsureComposeStoreProvisionedAsync|ProvisionComposeStoreAsync|ProvisionRolesAsync|ReassertComposeStatementTimeoutAsync|BuildProvisioningSql|BuildComposeStatementTimeoutSql)\s*\(", RegexOptions.Compiled)),
        ("the fixed role-name constants",
            new Regex(@"\bDarlingManagedPostgres\.(?:Admin|Viewer|Mcp)RoleName\b", RegexOptions.Compiled)),
    };

    private static readonly Regex ClassDeclaration =
        new(@"^(?:public|internal)\s+(?:sealed\s+|static\s+|abstract\s+|partial\s+)*class\s+(\w+)",
            RegexOptions.Compiled | RegexOptions.Multiline);

    private static readonly Regex BlockComment = new(@"/\*.*?\*/", RegexOptions.Compiled | RegexOptions.Singleline);

    private static readonly Regex LineComment = new(@"//[^\r\n]*", RegexOptions.Compiled);

    private sealed record Hit(string File, int Line, string ClassName, string Rule, string Text, bool InCollection);

    private static List<Hit> ScanClasses()
    {
        var directory = FindTestProjectDirectory();
        Assert.True(directory is not null,
            "Could not locate the Darling.Tests source directory (walked up from the test binary looking for "
            + "PerformanceMonitor.sln). This test scans source, so it fails rather than skips: a guard that skips stops guarding.");

        var thisFile = Path.GetFileName(ThisSourceFile());
        var hits = new List<Hit>();
        foreach (var file in Directory.EnumerateFiles(directory!, "*.cs", SearchOption.AllDirectories))
        {
            if (HasBuildOutputSegment(Path.GetRelativePath(directory!, file))
                || string.Equals(Path.GetFileName(file), thisFile, StringComparison.Ordinal))
            {
                continue;
            }

            var text = StripComments(File.ReadAllText(file));
            if (!ReachesCluster(text))
            {
                continue;
            }

            var declarations = ClassDeclaration.Matches(text);
            for (var i = 0; i < declarations.Count; i++)
            {
                var declaration = declarations[i];
                var end = i + 1 < declarations.Count ? declarations[i + 1].Index : text.Length;
                var body = text[declaration.Index..end];
                if (!ReachesCluster(body))
                {
                    continue;
                }

                var inCollection = HeaderAbove(text, declaration.Index).Contains($"[Collection(\"{CollectionName}\")]", StringComparison.Ordinal);
                var firstLine = text.AsSpan(0, declaration.Index).Count('\n') + 1;
                var lines = body.Split('\n');
                for (var n = 0; n < lines.Length; n++)
                {
                    var line = lines[n];
                    if (line.Contains("Assert.", StringComparison.Ordinal) || line.Contains("Regex", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    foreach (var (name, pattern) in Rules)
                    {
                        if (pattern.IsMatch(line))
                        {
                            hits.Add(new Hit(Path.GetFileName(file), firstLine + n, declaration.Groups[1].Value, name, line.Trim(), inCollection));
                            break;
                        }
                    }
                }
            }
        }

        return hits;
    }

    private static bool ReachesCluster(string source) => SharedClusterReach.Any(token => source.Contains(token, StringComparison.Ordinal));

    /// <summary>Blanks comments but keeps every newline, so a reported line number is the file's own.</summary>
    private static string StripComments(string source)
    {
        var withoutBlocks = BlockComment.Replace(source, m => new string('\n', m.Value.Count(c => c == '\n')));
        return LineComment.Replace(withoutBlocks, string.Empty);
    }

    private static string HeaderAbove(string text, int declarationIndex)
    {
        var start = declarationIndex;
        for (var lines = 0; lines < 25 && start > 0; lines++)
        {
            var previous = text.LastIndexOf('\n', start - 1);
            if (previous < 0)
            {
                start = 0;
                break;
            }

            start = previous;
        }

        return text[start..declarationIndex];
    }

    private static bool HasBuildOutputSegment(string relativePath) =>
        relativePath.Split('/', '\\').Any(segment =>
            string.Equals(segment, "bin", StringComparison.OrdinalIgnoreCase) || string.Equals(segment, "obj", StringComparison.OrdinalIgnoreCase));

    private static string ThisSourceFile([CallerFilePath] string? path = null) => path!;

    private static string? FindTestProjectDirectory()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                var source = Path.Combine(directory.FullName, "Darling", "Darling.Tests");
                return Directory.Exists(source) ? source : null;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
