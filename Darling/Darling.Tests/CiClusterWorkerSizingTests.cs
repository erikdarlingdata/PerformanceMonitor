/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Drift guard between the gated-live CI cluster's hand-written <c>postgresql.conf</c> settings and the
/// product's own worker sizing, <see cref="PerformanceMonitor.Darling.Service.DarlingManagedPostgres"/>'s
/// <c>BuildWorkerSizingConfAppend</c> (#1888).
///
/// <para><b>Why this exists.</b> The throwaway cluster in <c>build.yml</c>'s <c>darling-pg</c> job and its
/// <c>nightly.yml</c> mirror is stood up by shell, not by the product, so it got PostgreSQL's default
/// <c>max_worker_processes = 8</c> — the exact under-provisioned shape the product exists to avoid. Every
/// TimescaleDB background policy (compression, retention, continuous-aggregate refresh) is launched by a
/// background worker, and at 8 slots those launches routinely fail outright ("failed to start a background
/// worker"), so a large part of what the product does in the field was exercised only by the tests' explicit
/// foreground <c>run_job</c> calls. Measured on this repo's pg-runtime, same cluster, only that setting
/// changed: <c>add_compression_policy</c> over three eligible chunks compressed NOTHING in six seconds at 8
/// (the job never launched, <c>total_runs</c> NULL) and all three at 64.</para>
///
/// <para><b>Why a parse rather than a constant.</b> A workflow cannot call into the product, so the two
/// numbers have to be literals in YAML — and literals derived from the collector catalog go stale the moment a
/// collector is added. That is not hypothetical: <see cref="DarlingManagedPostgresTests"/> carried a hard "40"
/// pin from the 27-hypertable era and it went stale exactly that way, which is why the product's own test
/// derives instead. This reads the REAL workflow files (copied beside the test binary by the csproj, so there
/// is no second copy to go stale) and requires both numbers to equal what
/// <see cref="TimescaleSupport.HypertableCount"/> produces today. Add a collector and this fails on the pull
/// request that adds it, naming the file and the number to change.</para>
///
/// <para><b>The guard is itself guarded.</b> <see cref="ParsedSettings_Comparison_FailsOnAnInjectedDrift"/>
/// runs the identical parse over a mutated copy and asserts it reports the difference, so a regex that
/// silently matched nothing — a reformatted <c>Add-Content</c>, a renamed step — can never pass as "no
/// drift". A guard whose parser has stopped matching is indistinguishable from a clean one by its result
/// alone, and that is precisely the failure mode this whole family of source-parsing tests exists to catch.</para>
/// </summary>
public sealed class CiClusterWorkerSizingTests
{
    /// <summary>
    /// The workflows that stand up a throwaway cluster. All of them, not just the nightly: the nightly leg and the UI-smoke legs (#5701, and the WPF smoke's viewer walk, #5745) are separate copies of the same steps, and a fix applied to one of
    /// them is the ordinary way these drift apart.
    /// </summary>
    public static TheoryData<string> ClusterWorkflows() => new() { "nightly.yml", "darling-ui-smoke.yml", "wpf-smoke.yml" };

    /// <summary>
    /// <c>Add-Content -Path "$dataDir\postgresql.conf" -Value "setting = value"</c> — how both workflows append
    /// a cluster setting. Keyed on the appended VALUE rather than on the step, because that is the thing whose
    /// correctness is at stake; a setting moved to a different step still has to carry the right number.
    /// </summary>
    private static readonly Regex ConfAppend = new(
        @"Add-Content\s+-Path\s+""\$dataDir\\postgresql\.conf""\s+-Value\s+""(?<setting>[a-z_.]+)\s*=\s*(?<value>[^""]+)""",
        RegexOptions.Compiled);

    /* The product's formula, restated ONCE. Both are re-derived from the live catalog count on every run, so
       neither this test nor the workflows can be right about a stale hypertable count. */
    private static int ExpectedBackgroundWorkers => TimescaleSupport.HypertableCount + 2;

    private static int ExpectedWorkerProcesses => 3 + ExpectedBackgroundWorkers + 8;

    [Theory]
    [MemberData(nameof(ClusterWorkflows))]
    public void ClusterWorkflow_SizesWorkersFromTheProductsFormula(string workflowFileName)
    {
        var settings = ParseConfAppends(ReadWorkflow(workflowFileName));

        /* Named explicitly rather than asserted through a dictionary lookup that could throw a bare
           KeyNotFoundException: the whole point of the failure message is to say WHICH file and WHICH line to
           change, to someone who just added a collector and has no idea CI's cluster is sized off the count. */
        Assert.True(settings.ContainsKey("timescaledb.max_background_workers"),
            $"{workflowFileName} no longer appends timescaledb.max_background_workers to the throwaway "
            + "cluster's postgresql.conf. Without it the gated-live suite runs on TimescaleDB's default, which "
            + "the product overrides on every managed start (#1888).");
        Assert.True(settings.ContainsKey("max_worker_processes"),
            $"{workflowFileName} no longer appends max_worker_processes to the throwaway cluster's "
            + "postgresql.conf. PostgreSQL's default of 8 cannot launch the per-hypertable policy jobs, so the "
            + "suite would silently go back to testing a configuration no customer runs (#1888).");

        Assert.Equal(
            ExpectedBackgroundWorkers.ToString(CultureInfo.InvariantCulture),
            settings["timescaledb.max_background_workers"]);

        Assert.Equal(
            ExpectedWorkerProcesses.ToString(CultureInfo.InvariantCulture),
            settings["max_worker_processes"]);
    }

    /// <summary>
    /// The parser must actually be reading the file. Mutating the appended value has to be REPORTED — if this
    /// passes, the regex above matched nothing and every assertion in this class was vacuous.
    /// </summary>
    [Fact]
    public void ParsedSettings_Comparison_FailsOnAnInjectedDrift()
    {
        var real = ReadWorkflow("nightly.yml");
        var parsed = ParseConfAppends(real);
        Assert.NotEmpty(parsed);
        Assert.Equal(ExpectedWorkerProcesses.ToString(CultureInfo.InvariantCulture), parsed["max_worker_processes"]);

        var mutated = real.Replace(
            $@"-Value ""max_worker_processes = {ExpectedWorkerProcesses}""",
            @"-Value ""max_worker_processes = 8""",
            StringComparison.Ordinal);
        Assert.NotEqual(real, mutated);

        Assert.Equal("8", ParseConfAppends(mutated)["max_worker_processes"]);
    }

    private static Dictionary<string, string> ParseConfAppends(string workflowText)
    {
        var settings = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (Match match in ConfAppend.Matches(workflowText))
        {
            /* Last assignment wins, exactly as postgresql.conf itself resolves duplicates — so a workflow that
               appends a setting twice is compared on the value the server would actually run with. */
            settings[match.Groups["setting"].Value] = match.Groups["value"].Value.Trim();
        }

        return settings;
    }

    private static string ReadWorkflow(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", fileName);
        Assert.True(File.Exists(path),
            $"{fileName} was not copied beside the test binary. Darling.Tests.csproj links the workflow files "
            + "into Fixtures\\ so this guard parses the real ones; restore that item rather than pointing the "
            + "test at a copy that can go stale.");
        return File.ReadAllText(path);
    }
}
