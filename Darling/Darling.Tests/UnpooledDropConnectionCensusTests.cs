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
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A test that runs <c>DROP DATABASE</c> does it on an unpooled connection, and quiesces the database's TimescaleDB
/// jobs first (#5549).
///
/// <para><b>The defect this closes.</b> A PostgreSQL client backend crashed with 0xC0000005 in a TimescaleDB
/// <c>run_job</c>, after the same backend had run a scratch <c>DROP DATABASE ... WITH (FORCE)</c>. The admin connection
/// string is the caller's <c>DARLING_TEST_PG</c>, so a pooled admin connection went back into the pool every live test
/// draws from, and the next test took the backend that had just run the drop. The ruling: a database drop and a job
/// run never share a backend. <c>ScratchPostgres</c> opens its own create, drop and sweep connections unpooled
/// (<see cref="ScratchPostgres.UnpooledAdminConnectionString"/>); this census holds every other test file to the same
/// rule.</para>
///
/// <para><b>What this pins.</b> For every PostgreSQL <c>DROP DATABASE</c> string literal in the test project (comments
/// are blanked first): if the enclosing method opens a connection itself (<c>new NpgsqlConnection(</c> or
/// <c>LiveStoreCleanup.RunAsync(</c>), that method must also hold the evidence of an unpooled connection
/// (<c>UnpooledAdminConnectionString(</c>, <c>ExitDrainConnectionString(</c> or <c>Pooling = false</c>). A method that
/// is handed its connection (a drop helper that takes the cleanup connection as a parameter) is held to the weaker
/// file-level rule: the file must hold that evidence somewhere. The file-level rule cannot see which connection the
/// caller passes, so a drop helper's callers are the thing a reviewer still checks; the method-level rule is what stops
/// the common shape, a new drop with its own pooled admin connection beside it. A file that is not about PostgreSQL
/// drops at all is named in <see cref="Exceptions"/> with its reason.</para>
///
/// <para><b>The second rule: quiesce before the drop.</b> The same ruling says TimescaleDB jobs are quiesced before every
/// in-test drop, so a job worker is never running in a database while it is dropped. Every PostgreSQL <c>DROP DATABASE</c>
/// site (the same sites, under the same <see cref="Exceptions"/>) must sit in a method that also calls
/// <c>QuiesceTimescaleJobsAsync</c>, plain or <c>WITH (FORCE)</c> (the older census in
/// <see cref="ScratchPostgresQuiesceCensusTests"/> sees only the FORCE form). A method that cannot be dropping a database
/// with jobs, because the test never created the extension in it and never ran a migration there, is named in
/// <see cref="QuiesceExceptions"/> as <c>File.cs::Method</c> (or <c>File.cs</c> for the whole file) with its reason.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class UnpooledDropConnectionCensusTests
{
    /// <summary>The files that contain the text <c>DROP DATABASE</c> in a string but never drop a PostgreSQL database on a pooled connection, each with why.</summary>
    internal static readonly IReadOnlyDictionary<string, string> Exceptions = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["DarlingCollectorRunnerTests.cs"] = "T-SQL against SQL Server (DROP DATABASE [name]); SQL Server has no backend pool shared with a TimescaleDB job.",
        ["ProcedureStatsDeferredPlanFetchLiveTests.cs"] = "T-SQL against SQL Server (SINGLE_USER, then DROP DATABASE [name]).",
        ["QueryStatsDeferredPlanFetchLiveTests.cs"] = "T-SQL against SQL Server (SINGLE_USER, then DROP DATABASE [name]).",
        ["DropXeSessionsLiveTests.cs"] = "A SQL-injection session-name fixture: the text is data a validator must refuse, never run as a statement.",
        ["DropXeSessionsPerInstallTests.cs"] = "A SQL-injection session-name fixture: the text is data a validator must refuse, never run as a statement.",
        ["DropXeSessionsReadOnlyIntentTests.cs"] = "A SQL-injection session-name fixture: the text is data a validator must refuse, never run as a statement.",
        ["DropXeSessionsVerbTests.cs"] = "A SQL-injection session-name fixture and a list of forbidden verbs: text a validator must refuse, never run as a statement.",
        ["UnpooledDropConnectionCensusTests.cs"] = "This census: its own samples hold the drop text on purpose.",
        ["ScratchPostgresQuiesceTests.cs"] = "The quiesce census: its samples hold the drop text on purpose, and the live test there drops through ScratchPostgres.",
    };

    /// <summary>
    /// The PostgreSQL drop sites that skip <c>QuiesceTimescaleJobsAsync</c>, each as <c>File.cs::Method</c> (or <c>File.cs</c>
    /// for the whole file) with why the dropped database cannot hold a TimescaleDB job.
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, string> QuiesceExceptions = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["ComposeStoreRolesLiveTests.cs::TheComposeStore_ProvisionsItsOwnRoles_AndTheRealHostsConnectAsViewerAndMcp_Gated"] =
            "operator_app_3914 is a bare CREATE DATABASE from template1, made to stand for an operator's own database; no extension or migration ever runs in it.",
        ["PgReadBinaryFileCapabilityLiveTests.cs::IsGrantedAsync_OnAWin1252Database_GrantsTheRoute_AndCachesItsEncoding"] =
            "pm_test_enc1252 is a bare WIN1252 database made from template0, so it never holds the TimescaleDB extension.",
    };

    private static readonly Regex DropStatement = new("DROP DATABASE", RegexOptions.Compiled);

    private const string QuiesceHelper = "QuiesceTimescaleJobsAsync";

    /// <summary>The method name on a member's declaration line: the identifier that sits right before the first <c>(</c>.</summary>
    private static readonly Regex MethodName = new(@"(\w+)\s*\(", RegexOptions.Compiled);

    private static readonly Regex Evidence = new(
        @"UnpooledAdminConnectionString\(|ExitDrainConnectionString\(|Pooling\s*=\s*false", RegexOptions.Compiled);

    private static readonly Regex OwnConnection = new(@"new NpgsqlConnection\(|LiveStoreCleanup\.RunAsync\(", RegexOptions.Compiled);

    /// <summary>A member declaration at the class's own indent (four spaces), the line a method body is found from.</summary>
    private static readonly Regex MemberStart = new(@"^    (?:\[|public |private |internal |protected |static |async |override )", RegexOptions.Compiled);

    /// <summary>
    /// Every drop site that breaks the rule, as <c>file:line</c>, and how many drop sites were looked at.
    /// </summary>
    internal static (int Sites, List<string> Pooled) Scan(IEnumerable<(string Name, string Source)> files)
    {
        var sites = 0;
        var pooled = new List<string>();
        foreach (var (name, source) in files)
        {
            if (Exceptions.ContainsKey(Path.GetFileName(name)))
            {
                continue;
            }

            var blanked = ScratchPostgresQuiesceCensusTests.BlankComments(source);
            var lines = blanked.Split('\n');
            for (var i = 0; i < lines.Length; i++)
            {
                if (!DropStatement.IsMatch(lines[i]))
                {
                    continue;
                }

                sites++;
                var (start, end) = MethodAround(lines, i);
                var method = string.Join('\n', lines.Skip(start).Take(end - start + 1));
                var ok = OwnConnection.IsMatch(method) ? Evidence.IsMatch(method) : Evidence.IsMatch(blanked);
                if (!ok)
                {
                    pooled.Add($"{name}:{i + 1}");
                }
            }
        }

        return (sites, pooled);
    }

    /// <summary>
    /// Every PostgreSQL drop site whose method never calls <c>QuiesceTimescaleJobsAsync</c> and is not in
    /// <see cref="QuiesceExceptions"/>, as <c>file:line</c>, and how many drop sites were looked at.
    /// </summary>
    internal static (int Sites, List<string> Unquiesced) ScanForQuiesce(IEnumerable<(string Name, string Source)> files)
    {
        var sites = 0;
        var unquiesced = new List<string>();
        foreach (var (name, source) in files)
        {
            var file = Path.GetFileName(name);
            if (Exceptions.ContainsKey(file))
            {
                continue;
            }

            var blanked = ScratchPostgresQuiesceCensusTests.BlankComments(source);
            var lines = blanked.Split('\n');
            for (var i = 0; i < lines.Length; i++)
            {
                if (!DropStatement.IsMatch(lines[i]))
                {
                    continue;
                }

                sites++;
                var (start, end) = MethodAround(lines, i);
                var method = string.Join('\n', lines.Skip(start).Take(end - start + 1));
                if (method.Contains(QuiesceHelper, StringComparison.Ordinal))
                {
                    continue;
                }

                var declared = MethodName.Match(lines[start]);
                if (QuiesceExceptions.ContainsKey(file)
                    || (declared.Success && QuiesceExceptions.ContainsKey($"{file}::{declared.Groups[1].Value}")))
                {
                    continue;
                }

                unquiesced.Add($"{name}:{i + 1}");
            }
        }

        return (sites, unquiesced);
    }

    /// <summary>The first and last line of the member that holds line <paramref name="index"/>: up to the nearest member start at four spaces, down to the next closing brace at four spaces.</summary>
    private static (int Start, int End) MethodAround(string[] lines, int index)
    {
        var start = index;
        while (start > 0 && !MemberStart.IsMatch(lines[start]))
        {
            start--;
        }

        var end = index;
        while (end < lines.Length - 1 && !(lines[end].TrimEnd('\r') is "    }" or "    };"))
        {
            end++;
        }

        return (start, end);
    }

    [Fact]
    public void EveryPostgresDropInTheTestProject_RunsOnAnUnpooledConnection()
    {
        var directory = FindTestProjectDirectory();
        Assert.True(directory is not null, "could not find Darling/Darling.Tests from the test binary.");

        var files = Directory.EnumerateFiles(directory!, "*.cs", SearchOption.AllDirectories)
            .Select(path => (Name: Path.GetRelativePath(directory!, path).Replace('\\', '/'), Path: path))
            .Where(found => !found.Name.Split('/').Any(segment => segment is "bin" or "obj"))
            .Select(found => (found.Name, File.ReadAllText(found.Path)))
            .ToList();

        var (sites, pooled) = Scan(files);

        Assert.True(sites >= 9, $"the scan found {sites} PostgreSQL DROP DATABASE site(s); the project has at least 9, so the match found nothing.");
        Assert.True(pooled.Count == 0,
            "These DROP DATABASE statements run where no unpooled connection is in sight (#5549). A pooled connection returns the "
            + "backend that ran the drop to the pool every live test draws from, and a TimescaleDB job run on it crashed. Open the "
            + "connection with ScratchPostgres.UnpooledAdminConnectionString(...), or add the file to Exceptions with its reason: "
            + string.Join(", ", pooled));
    }

    [Fact]
    public void EveryPostgresDropInTheTestProject_QuiescesTheTimescaleJobsFirst()
    {
        var directory = FindTestProjectDirectory();
        Assert.True(directory is not null, "could not find Darling/Darling.Tests from the test binary.");

        var files = Directory.EnumerateFiles(directory!, "*.cs", SearchOption.AllDirectories)
            .Select(path => (Name: Path.GetRelativePath(directory!, path).Replace('\\', '/'), Path: path))
            .Where(found => !found.Name.Split('/').Any(segment => segment is "bin" or "obj"))
            .Select(found => (found.Name, File.ReadAllText(found.Path)))
            .ToList();

        var (sites, unquiesced) = ScanForQuiesce(files);

        Assert.True(sites >= 9, $"the scan found {sites} PostgreSQL DROP DATABASE site(s); the project has at least 9, so the match found nothing.");
        Assert.True(unquiesced.Count == 0,
            "These DROP DATABASE statements are not in a method that calls ScratchPostgres.QuiesceTimescaleJobsAsync (#5549). A drop "
            + "while a TimescaleDB job worker runs in the database kills the worker, and a job run next to a drop crashed a backend. "
            + "Call the helper before the drop, or add File.cs::Method to QuiesceExceptions with the reason the database holds no job: "
            + string.Join(", ", unquiesced));
    }

    [Fact]
    public void TheQuiesceExceptions_NameRealFiles_AndEachOneStillHoldsTheDropText()
    {
        var directory = FindTestProjectDirectory();
        Assert.True(directory is not null, "could not find Darling/Darling.Tests from the test binary.");

        foreach (var (key, reason) in QuiesceExceptions)
        {
            Assert.False(string.IsNullOrWhiteSpace(reason), $"{key} is allowed without a reason.");
            var parts = key.Split("::");
            var path = Path.Combine(directory!, parts[0]);
            Assert.True(File.Exists(path), $"{parts[0]} is in QuiesceExceptions but is not in the test project; remove the entry.");
            Assert.False(Exceptions.ContainsKey(parts[0]), $"{parts[0]} is already skipped by Exceptions; remove the entry from QuiesceExceptions.");
            var text = File.ReadAllText(path);
            Assert.True(text.Contains("DROP DATABASE", StringComparison.Ordinal),
                $"{parts[0]} no longer holds DROP DATABASE; remove {key} from QuiesceExceptions.");
            if (parts.Length > 1)
            {
                Assert.True(text.Contains(parts[1] + "(", StringComparison.Ordinal),
                    $"{parts[0]} no longer declares {parts[1]}; remove {key} from QuiesceExceptions.");
                Assert.True(MethodStillSkipsQuiesce(parts[1], text),
                    $"{parts[1]} in {parts[0]} no longer holds a DROP DATABASE that skips the quiesce; remove {key} from QuiesceExceptions.");
            }
        }
    }

    /// <summary>True when <paramref name="method"/> in <paramref name="source"/> holds a drop and no quiesce call, so its allow-list entry is still needed.</summary>
    private static bool MethodStillSkipsQuiesce(string method, string source)
    {
        var lines = ScratchPostgresQuiesceCensusTests.BlankComments(source).Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            if (!DropStatement.IsMatch(lines[i]))
            {
                continue;
            }

            var (start, end) = MethodAround(lines, i);
            var declared = MethodName.Match(lines[start]);
            if (declared.Success && declared.Groups[1].Value == method
                && !string.Join('\n', lines.Skip(start).Take(end - start + 1)).Contains(QuiesceHelper, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    [Fact]
    public void ADropWithoutTheQuiesce_IsReported_AndOneWithItOrAnAllowListedOneIsNot()
    {
        const string quiesced =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await ScratchPostgres.QuiesceTimescaleJobsAsync(cs, n);\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";
        const string plainBare =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";
        const string forceBare =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n} WITH (FORCE)\", admin);\n    }\n}\n";
        /* A quiesce in another method of the file does not excuse this one. */
        const string quiesceElsewhere =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n\n"
            + "    public async Task Other()\n    {\n        await ScratchPostgres.QuiesceTimescaleJobsAsync(cs, n);\n    }\n}\n";
        const string commentOnlyQuiesce =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        // QuiesceTimescaleJobsAsync(cs, n) would go here\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";
        const string commentOnly = "public class A\n{\n    // DROP DATABASE x\n    /// DROP DATABASE y\n}\n";
        var allowListed = QuiesceExceptions.Keys.First();
        var allowListedFile = allowListed.Split("::")[0];
        var allowListedMethod = allowListed.Split("::")[1];
        var allowListedSource =
            "public class A\n{\n    public async Task " + allowListedMethod + "()\n    {\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";

        Assert.Empty(ScanForQuiesce([("Q.cs", quiesced)]).Unquiesced);
        Assert.Equal(["P.cs:5"], ScanForQuiesce([("P.cs", plainBare)]).Unquiesced);
        Assert.Equal(["F.cs:5"], ScanForQuiesce([("F.cs", forceBare)]).Unquiesced);
        Assert.Equal(["E.cs:5"], ScanForQuiesce([("E.cs", quiesceElsewhere)]).Unquiesced);
        Assert.Equal(["M.cs:6"], ScanForQuiesce([("M.cs", commentOnlyQuiesce)]).Unquiesced);
        Assert.Equal(0, ScanForQuiesce([("C.cs", commentOnly)]).Sites);
        /* The allow-list names a file and a method: that method in that file is excused, the same method in another file is not. */
        Assert.Empty(ScanForQuiesce([(allowListedFile, allowListedSource)]).Unquiesced);
        Assert.Equal(["Other.cs:5"], ScanForQuiesce([("Other.cs", allowListedSource)]).Unquiesced);
        /* A SQL Server or fixture file is outside the rule altogether (Exceptions). */
        Assert.Equal(0, ScanForQuiesce([("DarlingCollectorRunnerTests.cs", plainBare)]).Sites);
    }

    [Fact]
    public void TheExceptions_NameRealFiles_AndEachOneStillHoldsTheDropText()
    {
        var directory = FindTestProjectDirectory();
        Assert.True(directory is not null, "could not find Darling/Darling.Tests from the test binary.");

        foreach (var (file, reason) in Exceptions)
        {
            Assert.False(string.IsNullOrWhiteSpace(reason), $"{file} is allowed without a reason.");
            var path = Path.Combine(directory!, file);
            Assert.True(File.Exists(path), $"{file} is in Exceptions but is not in the test project; remove the entry.");
            Assert.True(File.ReadAllText(path).Contains("DROP DATABASE", StringComparison.Ordinal),
                $"{file} no longer holds DROP DATABASE; remove it from Exceptions.");
        }
    }

    [Fact]
    public void APooledDrop_IsReported_AndAnUnpooledOrHandedInOneIsNot()
    {
        const string pooledOwn =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await using var admin = new NpgsqlConnection(cs);\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";
        const string unpooledOwn =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await using var admin = new NpgsqlConnection(ScratchPostgres.UnpooledAdminConnectionString(cs));\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";
        const string explicitOff =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        var b = new NpgsqlConnectionStringBuilder(cs) { Pooling = false };\n"
            + "        await using var admin = new NpgsqlConnection(b.ConnectionString);\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";
        const string unpooledElsewhereInFile =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await using var admin = new NpgsqlConnection(cs);\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n\n"
            + "    public string Other() => ScratchPostgres.UnpooledAdminConnectionString(cs);\n}\n";
        const string handedIn =
            "public class A\n{\n    private static async Task Drop(NpgsqlConnection c)\n    {\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", c);\n    }\n\n"
            + "    public string Other() => ScratchPostgres.UnpooledAdminConnectionString(cs);\n}\n";
        const string handedInNoEvidence =
            "public class A\n{\n    private static async Task Drop(NpgsqlConnection c)\n    {\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", c);\n    }\n}\n";
        const string commentOnly = "public class A\n{\n    // DROP DATABASE x\n    /// DROP DATABASE y\n}\n";

        Assert.Equal(["P.cs:6"], Scan([("P.cs", pooledOwn)]).Pooled);
        Assert.Empty(Scan([("U.cs", unpooledOwn)]).Pooled);
        Assert.Empty(Scan([("E.cs", explicitOff)]).Pooled);
        /* An unrelated unpooled helper elsewhere in the file does not excuse a drop that opens its own pooled connection. */
        Assert.Equal(["F.cs:6"], Scan([("F.cs", unpooledElsewhereInFile)]).Pooled);
        Assert.Empty(Scan([("H.cs", handedIn)]).Pooled);
        Assert.Equal(["N.cs:5"], Scan([("N.cs", handedInNoEvidence)]).Pooled);
        Assert.Equal(0, Scan([("C.cs", commentOnly)]).Sites);
        Assert.Equal(0, Scan([("DarlingCollectorRunnerTests.cs", pooledOwn)]).Sites);
    }

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
