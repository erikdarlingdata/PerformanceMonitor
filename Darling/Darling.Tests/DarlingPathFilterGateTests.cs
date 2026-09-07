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
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The two Darling jobs in <c>build.yml</c> share ONE path gate, and that gate covers every shared library
/// Darling compiles against (#3116).
///
/// <para><b>What went wrong.</b> Each Darling job carried its own list of paths. The Linux job's named three
/// shared libraries; the PostgreSQL job's named none, only <c>Darling/**</c> and the workflow files. So a
/// change to <c>PerformanceMonitor.Collectors</c> — referenced by four Darling projects, and the class
/// <c>DarlingWorker</c> registers and runs — built the Linux image in 1m47s and ran the TimescaleDB suite on
/// none of it, reporting success in 17s. On the same commit the same job took 4m5s for a sibling pull request
/// whose only change was a comment. <b>The gate keyed on where the change lived rather than on what the tests
/// covered</b>, and the job it gated is the only place the <c>*_AgainstDevPostgres</c> classes run before a
/// merge.</para>
///
/// <para><b>Why a test rather than a corrected list.</b> The correction is one entry per shared library, and
/// the same entries were already written down twice elsewhere in the same file. Two hand-maintained copies of
/// one dependency fact is what came apart here — the copies are not compared by anything, so the one nobody
/// edits is the one that goes wrong, and it goes wrong silently because its symptom is a green check. Both
/// jobs now read <c>.github/darling-paths-filter.yml</c> through dorny/paths-filter's <c>filters: path</c>
/// form, so there is one copy for the two jobs; and the requirement on that copy is DERIVED here from the
/// ProjectReference closure of <c>Darling.Tests</c> rather than restated as a list, so adding a shared
/// library to a Darling project makes the gate entry mandatory instead of remembered.</para>
///
/// <para><b>What this cannot see.</b> Whether the patterns match the files they are meant to is
/// dorny/paths-filter's own glob semantics, which this reads as text and does not evaluate. What it holds is
/// the FORM — <c>dir/**/!(*.md)</c>, the extglob carve-out — because the alternative spelling is the one that
/// silently broke these filters before: dorny v4 evaluates each pattern as an independent predicate under
/// the default quantifier, so a bare <c>!**/*.md</c> exclude is not a subtraction but its own rule meaning
/// "any non-markdown file anywhere", which made the filters true for every change in the repository.</para>
/// </summary>
public sealed class DarlingPathFilterGateTests
{
    /// <summary>
    /// The shared gate both Darling jobs read, repository-relative.
    /// </summary>
    private const string GatePath = ".github/darling-paths-filter.yml";

    /// <summary>
    /// One <c>&lt;ProjectReference Include="..." /&gt;</c>, which is how the closure below is walked. MSBuild
    /// spells these with backslashes regardless of platform, so the path is normalised at the use site.
    /// </summary>
    private static readonly Regex ProjectReference =
        new(@"<ProjectReference\s+Include=""(?<path>[^""]+)""", RegexOptions.Compiled);

    /// <summary>
    /// One quoted pattern entry of a dorny/paths-filter block.
    /// </summary>
    private static readonly Regex PatternEntry =
        new(@"^\s*-\s*'(?<pattern>[^']+)'\s*$", RegexOptions.Compiled | RegexOptions.Multiline);

    /// <summary>
    /// Every shared library reachable from <c>Darling.Tests</c> is in the gate, in the extglob form.
    ///
    /// <para>The closure is the requirement, not a judgement about how far the dependency graph usefully
    /// reaches: if a Darling project references it, the suite the PostgreSQL job runs compiles it, so a change
    /// to it can change what that suite reports. That is the question the gate has to answer.</para>
    /// </summary>
    [Fact]
    public void TheSharedGate_CoversEverySharedLibraryDarlingCompiles()
    {
        var repo = RepoRoot();
        var libraries = SharedLibraryClosure(repo);

        /* Non-vacuity, and specifically that the walk is TRANSITIVE. Darling.Tests references exactly one
           shared library directly (PerformanceMonitor.Common); PerformanceMonitor.Collectors is two hops
           away, through Darling.Storage, and is the library #3116 was found on. A walk that stopped at
           direct references would satisfy an Assert.NotEmpty and prove nothing. */
        Assert.Contains("PerformanceMonitor.Common", libraries);
        Assert.Contains("PerformanceMonitor.Collectors", libraries);

        var missing = MissingFromGate(ReadRepoFileLf(repo, GatePath), libraries);

        Assert.True(
            missing.Count == 0,
            $"Shared libraries Darling compiles against that {GatePath} does not cover, so a change to one "
            + "runs the Darling PostgreSQL job and the Darling Linux job on none of it while both report "
            + "success — add 'NAME/**/!(*.md)' to the darling filter for each:\n  "
            + string.Join("\n  ", missing));
    }

    /// <summary>
    /// The coverage check reports an injected gap. Without this, a parser that matched nothing would report
    /// every gate as complete and the assertion above would be vacuous — which is the failure mode being
    /// guarded one layer out, so it would be a poor one to reproduce here.
    /// </summary>
    [Fact]
    public void TheCoverageCheck_ReportsAnInjectedGap()
    {
        var repo = RepoRoot();
        var real = ReadRepoFileLf(repo, GatePath);

        Assert.Empty(MissingFromGate(real, SharedLibraryClosure(repo)));

        /* Delete the entry #3116 was about. */
        var mutated = real.Replace(
            "  - 'PerformanceMonitor.Collectors/**/!(*.md)'\n",
            string.Empty,
            StringComparison.Ordinal);
        Assert.NotEqual(real, mutated);
        Assert.Contains("PerformanceMonitor.Collectors", MissingFromGate(mutated, SharedLibraryClosure(repo)));

        /* And the extglob form is what is required, not the directory. A bare 'dir/**' include paired with a
           negation elsewhere is the spelling that made these filters unconditionally true. */
        var bareInclude = real.Replace(
            "  - 'PerformanceMonitor.Collectors/**/!(*.md)'",
            "  - 'PerformanceMonitor.Collectors/**'",
            StringComparison.Ordinal);
        Assert.NotEqual(real, bareInclude);
        Assert.Contains("PerformanceMonitor.Collectors", MissingFromGate(bareInclude, SharedLibraryClosure(repo)));
    }

    /// <summary>
    /// Both Darling jobs read the one gate, and neither carries a list of its own.
    ///
    /// <para>The second half is the part that matters: a job that inlines its filters again has a private copy
    /// of the dependency fact, and a private copy is what nothing compares.</para>
    /// </summary>
    [Theory]
    [InlineData("darling-pg")]
    [InlineData("darling-linux")]
    public void EachDarlingJob_ReadsTheSharedGate_AndCarriesNoListOfItsOwn(string jobId)
    {
        var job = JobBlock(ReadRepoFileLf(RepoRoot(), ".github/workflows/build.yml"), jobId);

        Assert.Contains($"filters: {GatePath}", job, StringComparison.Ordinal);

        Assert.DoesNotContain("filters: |", job, StringComparison.Ordinal);
    }

    /// <summary>
    /// Each Darling job's gate-decision step reports the decision from the filter step's own outputs.
    ///
    /// <para>A job reporting success having run nothing is indistinguishable from one that ran everything,
    /// which is why the notice exists at all — so what it prints has to be answerable from the run. The skip
    /// notice named a CAUSE, "documentation-only", as the only explanation for a false gate; on the pull
    /// request that exposed #3116 the changed files were a PostgreSQL collector and its test, so the claim was
    /// false in both of its clauses and read as authoritative because the notice was the only log there was.
    /// Requiring the message to interpolate the changed-file count leaves it reporting what the gate saw.</para>
    /// </summary>
    [Theory]
    [InlineData("darling-pg", "Report the Darling PG gate decision")]
    [InlineData("darling-linux", "Report the Linux gate decision")]
    public void EachGateDecisionNotice_ReportsTheDecisionRatherThanACause(string jobId, string stepName)
    {
        var job = JobBlock(ReadRepoFileLf(RepoRoot(), ".github/workflows/build.yml"), jobId);

        /* The file lists the notice names have to come from. */
        Assert.Contains("list-files: shell", job, StringComparison.Ordinal);

        var step = StepBlock(job, stepName);

        Assert.Contains("ALL_COUNT: ${{ steps.filter.outputs.all_count }}", step, StringComparison.Ordinal);
        Assert.Contains("ALL_FILES: ${{ steps.filter.outputs.all_files }}", step, StringComparison.Ordinal);

        /* Every branch that decides against running says how many changed files it classified, so the number
           is the run's own rather than a sentence about it. */
        var skipNotices = step
            .Split('\n')
            .Where(line => line.Contains("::notice title=", StringComparison.Ordinal)
                        && line.Contains("skipped::", StringComparison.Ordinal)
                        && !line.Contains("Release event", StringComparison.Ordinal))
            .ToList();

        Assert.NotEmpty(skipNotices);
        Assert.All(skipNotices, line => Assert.Contains("${ALL_COUNT", line, StringComparison.Ordinal));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The shared libraries in the ProjectReference closure of <c>Darling.Tests</c> — the projects whose
    /// directory sits at the repository root and is named <c>PerformanceMonitor.*</c>. The <c>Darling.*</c>
    /// projects are excluded because they live under <c>Darling/</c>, which the gate's <c>Darling/**</c>
    /// entry already covers.
    /// </summary>
    private static SortedSet<string> SharedLibraryClosure(string repo)
    {
        var root = Path.GetFullPath(repo).TrimEnd(Path.DirectorySeparatorChar);
        var libraries = new SortedSet<string>(StringComparer.Ordinal);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var pending = new Queue<string>();

        pending.Enqueue(Path.GetFullPath(Path.Combine(root, "Darling", "Darling.Tests", "Darling.Tests.csproj")));

        while (pending.Count > 0)
        {
            var project = pending.Dequeue();
            if (!seen.Add(project))
            {
                continue;
            }

            /* A ProjectReference that does not resolve must fail loudly rather than silently shrinking the
               closure to the projects that still exist — a short closure is how this guard would start
               reporting a complete gate. */
            Assert.True(File.Exists(project), $"a ProjectReference points at a file that is not there: {project}");

            var directory = Path.GetDirectoryName(project)!;
            foreach (Match match in ProjectReference.Matches(File.ReadAllText(project)))
            {
                var include = match.Groups["path"].Value.Replace('\\', Path.DirectorySeparatorChar);
                var referenced = Path.GetFullPath(Path.Combine(directory, include));
                pending.Enqueue(referenced);

                var referencedDirectory = Path.GetDirectoryName(referenced)!;
                var name = Path.GetFileName(referencedDirectory);

                if (name.StartsWith("PerformanceMonitor.", StringComparison.Ordinal)
                    && string.Equals(Path.GetDirectoryName(referencedDirectory), root, StringComparison.OrdinalIgnoreCase))
                {
                    libraries.Add(name);
                }
            }
        }

        return libraries;
    }

    /// <summary>
    /// The libraries with no <c>NAME/**/!(*.md)</c> entry in the gate's <c>darling</c> filter.
    /// </summary>
    private static List<string> MissingFromGate(string gateYaml, IEnumerable<string> libraries)
    {
        var patterns = FilterPatterns(gateYaml, "darling");
        Assert.True(patterns.Count > 0, $"{GatePath}'s 'darling' filter is gone — find where it moved before editing this test");

        return libraries
            .Where(library => !patterns.Contains($"{library}/**/!(*.md)", StringComparer.Ordinal))
            .ToList();
    }

    /// <summary>
    /// The quoted pattern entries of one filter block in the shared gate, whose keys sit at column zero.
    /// </summary>
    private static List<string> FilterPatterns(string gateYaml, string filterName)
    {
        var at = gateYaml.IndexOf($"\n{filterName}:\n", StringComparison.Ordinal);
        if (at < 0)
        {
            return new List<string>();
        }

        var rest = gateYaml[(at + 1)..];
        var next = Regex.Match(rest, "\n[a-z_]+:\n");
        var block = next.Success ? rest[..next.Index] : rest;

        return PatternEntry.Matches(block)
            .Select(match => match.Groups["pattern"].Value)
            .ToList();
    }

    /// <summary>
    /// One job's block of <c>build.yml</c>, from its two-space-indented key to the next one.
    /// </summary>
    private static string JobBlock(string yaml, string jobId)
    {
        var at = yaml.IndexOf($"\n  {jobId}:\n", StringComparison.Ordinal);
        Assert.True(at > 0, $"build.yml has no '{jobId}' job — find where it moved before editing this test");

        var rest = yaml[(at + 1)..];
        var next = Regex.Match(rest, "\n  [a-z][a-z0-9-]*:\n");
        return next.Success ? rest[..next.Index] : rest;
    }

    /// <summary>
    /// One step's block of a job, from its name to the next step's.
    /// </summary>
    private static string StepBlock(string job, string stepName)
    {
        var at = job.IndexOf($"- name: {stepName}\n", StringComparison.Ordinal);
        Assert.True(at > 0, $"the '{stepName}' step is gone — find where it moved before editing this test");

        var rest = job[at..];
        var next = Regex.Match(rest, "\n      - name: ");
        return next.Success ? rest[..next.Index] : rest;
    }

    /// <summary>
    /// A repository file, read with the line endings normalised, so the assertions here do not depend on
    /// whether the checkout landed CRLF or LF.
    /// </summary>
    private static string ReadRepoFileLf(string repo, string relative) =>
        File.ReadAllText(Path.Combine(repo, relative.Replace('/', Path.DirectorySeparatorChar)))
            .Replace("\r\n", "\n", StringComparison.Ordinal);

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
