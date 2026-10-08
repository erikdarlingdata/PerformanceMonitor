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
/// A test that runs <c>DROP DATABASE</c> does it on an unpooled connection (#5549).
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

    private static readonly Regex DropStatement = new("DROP DATABASE", RegexOptions.Compiled);

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
