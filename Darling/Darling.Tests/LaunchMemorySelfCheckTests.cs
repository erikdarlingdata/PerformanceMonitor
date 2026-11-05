/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6995: the hidden <c>--self-check-launch-memory</c> verb. The file reads run on Linux only, in the container smoke, so
/// what is pinned here is the decision (which readings pass and which fail), the argument parsing, and that the verb is
/// reachable and stays out of the usage text.
/// </summary>
public sealed class LaunchMemorySelfCheckTests
{
    private const long Mib = 1024L * 1024L;

    private static LaunchMemoryReading Reading(string metric = "resident memory", string source = "cgroup memory limit", long bytes = 80 * Mib, long limit = 512 * Mib)
        => new(bytes, limit, metric, source);

    [Fact]
    public void A_resident_reading_against_the_cgroup_limit_passes()
        => Assert.Empty(DarlingLaunchMemorySelfCheck.Evaluate(Reading(), 512 * Mib, 512 * Mib));

    [Fact]
    public void A_private_bytes_metric_fails()
    {
        var failures = DarlingLaunchMemorySelfCheck.Evaluate(Reading(metric: "private bytes"), 512 * Mib, null);
        Assert.Contains(failures, f => f.Contains("private bytes", System.StringComparison.Ordinal));
    }

    [Fact]
    public void A_fallback_to_the_gc_budget_fails_even_without_a_cgroup_limit()
    {
        var failures = DarlingLaunchMemorySelfCheck.Evaluate(Reading(source: "the GC memory budget"), null, null);
        Assert.Contains(failures, f => f.Contains("fell back", System.StringComparison.Ordinal));
    }

    [Fact]
    public void A_cgroup_limit_the_guard_did_not_use_fails()
    {
        // The sampler reports total RAM (8 GiB) although the container is capped at 512 MiB.
        var failures = DarlingLaunchMemorySelfCheck.Evaluate(Reading(source: "total RAM", limit: 8192 * Mib), 512 * Mib, null);
        Assert.Contains(failures, f => f.Contains("cgroup memory limit of", System.StringComparison.Ordinal));
    }

    [Fact]
    public void A_cgroup_limit_above_total_ram_leaves_total_ram_a_pass()
        => Assert.Empty(DarlingLaunchMemorySelfCheck.Evaluate(Reading(source: "total RAM", limit: 8192 * Mib), 16384 * Mib, null));

    [Fact]
    public void No_cgroup_limit_and_total_ram_passes()
        => Assert.Empty(DarlingLaunchMemorySelfCheck.Evaluate(Reading(source: "total RAM", limit: 8192 * Mib), null, null));

    [Fact]
    public void An_expected_limit_the_guard_does_not_read_fails()
    {
        var failures = DarlingLaunchMemorySelfCheck.Evaluate(Reading(limit: 1024 * Mib), 1024 * Mib, 512 * Mib);
        Assert.Contains(failures, f => f.Contains("expected a limit of about 512 MiB", System.StringComparison.Ordinal));
    }

    [Fact]
    public void An_expected_limit_with_total_ram_as_the_source_fails()
    {
        var failures = DarlingLaunchMemorySelfCheck.Evaluate(Reading(source: "total RAM", limit: 512 * Mib), null, 512 * Mib);
        Assert.Contains(failures, f => f.Contains("expected the cgroup limit", System.StringComparison.Ordinal));
    }

    [Fact]
    public void A_zero_figure_fails()
        => Assert.NotEmpty(DarlingLaunchMemorySelfCheck.Evaluate(Reading(bytes: 0), 512 * Mib, null));

    [Theory]
    [InlineData("512", true, 512L * Mib)]
    [InlineData("1", true, Mib)]
    [InlineData("0", false, 0L)]
    [InlineData("-5", false, 0L)]
    [InlineData("512m", false, 0L)]
    [InlineData("", false, 0L)]
    public void The_expected_limit_is_whole_mebibytes_above_zero(string text, bool ok, long bytes)
    {
        Assert.Equal(ok, DarlingLaunchMemorySelfCheck.TryParseExpectedLimitBytes(text, out var parsed));
        Assert.Equal(bytes, parsed);
    }

    [Fact]
    public void Extra_or_unreadable_arguments_print_the_usage_line_and_exit_one()
    {
        foreach (var args in new[] { new[] { "512", "extra" }, new[] { "abc" } })
        {
            var error = new StringWriter();
            Assert.Equal(1, DarlingLaunchMemorySelfCheck.Run(args, new StringWriter(), error));
            Assert.Contains(DarlingLaunchMemorySelfCheck.UsageLine, error.ToString(), System.StringComparison.Ordinal);
        }
    }

    [Fact]
    public void The_verb_is_recognized_and_stays_out_of_the_usage_text()
    {
        Assert.True(DarlingCliCommands.IsSelfCheckLaunchMemoryVerb("--SELF-CHECK-LAUNCH-MEMORY"));
        Assert.False(DarlingCliCommands.IsSelfCheckLaunchMemoryVerb("--self-check-launch"));
        Assert.True(DarlingCliCommands.IsKnownVerb("--self-check-launch-memory"));
        Assert.Equal(StartupAction.RunKnownVerb, DarlingCliCommands.ClassifyStartupArgs(new[] { "--self-check-launch-memory" }));
        Assert.DoesNotContain("self-check", DarlingCliCommands.UsageText(), System.StringComparison.Ordinal);
    }
}
