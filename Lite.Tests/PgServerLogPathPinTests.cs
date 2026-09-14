/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Darling.Tests;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The collectors that read the server log take the log DIRECTORY from the server rather than assuming it.
///
/// <para><c>pg_ls_logdir()</c> returns bare file names relative to <c>log_directory</c>, so a literal
/// <c>'log/'</c> prefix is right exactly where that setting holds its default. Point it at an absolute path
/// — a separate volume for logs is an ordinary thing for an installer to want — and the read fails
/// <c>58P01</c> on the file the same statement just listed, reported as a failed collector next to the
/// <c>42501</c> that means the unrelated thing (#3410).</para>
///
/// <para><c>pg_catalog.current_setting('log_directory')</c> covers both regimes with no branch, which is why
/// no site joins <c>data_directory</c> onto a relative setting: <c>pg_read_file</c> resolves a relative path
/// against the data directory itself, so a default setting concatenates to the path the literal produced,
/// and an absolute setting resolves as itself — readable without a wider grant, because an absolute path
/// under <c>log_directory</c> is admitted even when <c>log_directory</c> sits outside the data directory.
/// <c>StoreLogSweep.ReadFileSql</c> builds the store's own log path the same way, so one idiom serves every
/// site.</para>
///
/// <para>Asserted PER SITE and never as a total, because reverting one of two identical lines satisfies any
/// count that averages them — and the source sweep below holds the shape for a site that does not exist
/// yet, so a third log reader cannot arrive unpinned.</para>
/// </summary>
public sealed class PgServerLogPathPinTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext()
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 9, 14, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
                PostgresVersionNum = 170007,
            },
            ExcludedDatabases = Array.Empty<string>(),
        };

    /// <summary>The call that asks the server, schema-qualified: a monitoring login's <c>search_path</c> is
    /// not a collector's to assume.</summary>
    private const string AsksTheServer = "pg_catalog.current_setting('log_directory')";

    /// <summary>The prefix that assumes the answer.</summary>
    private const string AssumesTheDefault = "'log/'";

    /// <summary>The marker that makes a SQL literal one of the ones this pin is about.</summary>
    private const string ListsTheLogDirectory = "pg_ls_logdir()";

    /// <summary>
    /// What the pin requires of one piece of SQL that lists the log directory, returned as the reasons it
    /// fell short so a failure names the site rather than a count.
    /// </summary>
    private static IReadOnlyList<string> DirectoryFaults(string sql)
    {
        var faults = new List<string>();

        if (!sql.Contains(AsksTheServer, StringComparison.Ordinal))
        {
            faults.Add($"does not read {AsksTheServer}");
        }

        if (sql.Contains(AssumesTheDefault, StringComparison.Ordinal))
        {
            faults.Add($"still prefixes the assumed {AssumesTheDefault}");
        }

        return faults;
    }

    /// <summary>
    /// The check the two facts below run, exercised in BOTH directions on strings this class owns. A matcher
    /// that accepted everything would pass them while proving nothing, so the exact shape #3410 reported is
    /// written out here and required to fail — and to fail on both counts, since that line neither asks for
    /// the directory nor omits the assumption.
    /// </summary>
    [Fact]
    public void TheDirectoryCheckDiscriminates()
    {
        const string asks =
            "SELECT pg_catalog.pg_read_file(pg_catalog.current_setting('log_directory') || '/' || n.name, 0, 1)";
        const string assumes =
            "SELECT pg_catalog.pg_read_file('log/' || n.name, 0, 1)";
        const string neither =
            "SELECT pg_catalog.pg_read_file(n.name, 0, 1)";

        Assert.Empty(DirectoryFaults(asks));
        Assert.Equal(2, DirectoryFaults(assumes).Count);
        Assert.Single(DirectoryFaults(neither));
    }

    /// <summary>
    /// The two collectors' SHIPPED SQL — what <c>BuildQuery</c> hands the driver, not what the file says
    /// about itself.
    /// </summary>
    [Fact]
    public void BothServerLogCollectorsAskWhereTheLogDirectoryIs()
    {
        var sites = new[]
        {
            (Collector: PgDeadlocksCollector.Instance.Name,
             Sql: PgDeadlocksCollector.Instance.BuildQuery(MakeContext()).Text),
            (Collector: PgPlanCaptureCollector.Instance.Name,
             Sql: PgPlanCaptureCollector.Instance.BuildQuery(MakeContext()).Text),
        };

        var offenders = new List<string>();

        foreach (var site in sites)
        {
            /* The marker, asserted per site: SQL that stopped listing the directory is not a site this pin
               can speak for, and treating it as clean would be the quiet pass. */
            Assert.Contains(ListsTheLogDirectory, site.Sql, StringComparison.Ordinal);

            offenders.AddRange(
                DirectoryFaults(site.Sql).Select(fault => $"{site.Collector} {fault}"));
        }

        Assert.True(
            offenders.Count == 0,
            "The log directory is assumed rather than asked for: " + string.Join("; ", offenders));
    }

    /// <summary>
    /// Every SQL literal in the collectors project that lists the log directory, whether or not a definition
    /// test reaches it.
    ///
    /// <para>Read through <see cref="CSharpSourceWalker.StringLiteralBodies"/> rather than off the raw text,
    /// so the paragraphs above each query — which name both the call and the prefix, because the reason is
    /// the durable part — are not mistaken for the SQL itself.</para>
    ///
    /// <para>The contributing FILES are compared for equality, not counted. A walk that went blind
    /// contributes nothing and reds here rather than passing vacuously, and a third log reader is forced
    /// into the list instead of arriving unwatched.</para>
    /// </summary>
    [Fact]
    public void EverySqlLiteralThatListsTheLogDirectoryAsksWhereItIs()
    {
        var collectors = Path.Combine(RepoRoot(), "PerformanceMonitor.Collectors");

        var sources = Directory.EnumerateFiles(collectors, "*.cs", SearchOption.TopDirectoryOnly)
            .Select(path => (Name: Path.GetFileName(path), Text: File.ReadAllText(path)))
            .OrderBy(s => s.Name, StringComparer.Ordinal)
            .ToArray();

        Assert.NotEmpty(sources);

        var (files, faults) = Sweep(sources);

        Assert.Equal(
            new[] { "PgDeadlocksCollector.cs", "PgPlanCaptureCollector.cs" },
            files);

        Assert.True(
            faults.Count == 0,
            "SQL that lists the log directory but does not ask where it is: " + string.Join("; ", faults));
    }

    /// <summary>
    /// The sweep above, run over a counterfeit this class owns. Crippling the instrument is the only way to
    /// learn that it discriminates: the second source carries the reported shape and has to be reported,
    /// the third mentions the prefix in PROSE only and must not be, and the fourth ships SQL that never
    /// lists the directory and is none of this pin's business.
    /// </summary>
    [Fact]
    public void TheSweepReportsACounterfeitAndSparesAComment()
    {
        var (files, faults) = Sweep(new[]
        {
            (Name: "Clean.cs",
             Text: "const string Q = @\"SELECT pg_catalog.pg_read_file("
                 + "pg_catalog.current_setting('log_directory') || '/' || n.name) FROM pg_ls_logdir()\";"),
            (Name: "Assuming.cs",
             Text: "const string Q = @\"SELECT pg_catalog.pg_read_file('log/' || n.name)"
                 + " FROM pg_ls_logdir()\";"),
            (Name: "ProseOnly.cs",
             Text: "/* pg_ls_logdir() returns names relative to log_directory, so 'log/' is an assumption. */"),
            (Name: "NoLogRead.cs",
             Text: "const string Q = @\"SELECT 'log/' AS assumed_prefix\";"),
        });

        Assert.Equal(new[] { "Assuming.cs", "Clean.cs" }, files);
        Assert.Equal(2, faults.Count);
        Assert.All(faults, fault => Assert.StartsWith("Assuming.cs", fault, StringComparison.Ordinal));
    }

    /// <summary>
    /// Which of the given sources carry a log-directory listing, and every way their literals fall short.
    /// </summary>
    private static (string[] Files, IReadOnlyList<string> Faults) Sweep(
        IEnumerable<(string Name, string Text)> sources)
    {
        var files = new List<string>();
        var faults = new List<string>();

        foreach (var source in sources)
        {
            var listing = CSharpSourceWalker.StringLiteralBodies(source.Text)
                .Where(body => body.Text.Contains(ListsTheLogDirectory, StringComparison.Ordinal))
                .ToArray();

            if (listing.Length == 0)
            {
                continue;
            }

            files.Add(source.Name);

            foreach (var body in listing)
            {
                faults.AddRange(
                    DirectoryFaults(body.Text)
                        .Select(fault => $"{source.Name} (literal at offset {body.Start}) {fault}"));
            }
        }

        return (files.OrderBy(f => f, StringComparer.Ordinal).ToArray(), faults);
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName
            ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }
}
