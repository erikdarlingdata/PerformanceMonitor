/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A collector query may use SQL Server's LOCAL clock only where the other side of the expression shares
/// that frame. A local clock reaching a STORED timestamp column is a data-correctness defect: every naive
/// timestamp in the store is UTC, and nothing downstream can recover the frame from the value, because the
/// offset belongs to the deployment rather than to the row and it moves with DST.
///
/// <para><b>Measured, not theoretical.</b> <c>CpuUtilizationCollector</c>'s ring-buffer arm computed
/// <c>sample_time</c> from <c>SYSDATETIME()</c>. On the us-east-2 production store that put it
/// <b>exactly 4 h behind the <c>collection_time</c> written by the same collector run</b>, on <b>42 of 42</b>
/// servers, while that collector reported SUCCESS 12,526 times in six hours with zero failures. Sibling
/// tables were current to 5-8 seconds, so collection health showed nothing. It also disagreed with the SAME
/// collector's Azure arm, which takes <c>sys.dm_db_resource_stats.end_time</c> and is already UTC — one
/// column arriving in two frames, decided by target type.</para>
///
/// <para><b>Why an allowlist rather than a ban.</b> Sixteen <c>GETDATE()</c> uses here are correct and must
/// stay: durations (<c>DATEDIFF(SECOND, qs.creation_time, GETDATE())</c>), comparisons against DMV columns
/// that are themselves local wall clock (<c>last_execution_time</c>, <c>msdb</c>'s <c>run_date</c>), and the
/// deliberate <c>DATEDIFF(MINUTE, GETUTCDATE(), GETDATE())</c> that PRODUCES
/// <c>ServerPropertiesCollector</c>'s <c>utc_offset_minutes</c>. Rewriting those to UTC would break them by
/// shifting each threshold by the deployment's offset. So the rule is not "no local clock" but "local clock
/// only where the other side is local too" — a judgement no regex can make. This pin fixes the FILE SET and
/// makes a person state a reason for any addition.</para>
/// </summary>
public sealed class CollectorTimestampFrameTests
{
    /// <summary>
    /// Files permitted a local clock, each with the reason it is correct there. None stores an absolute
    /// local timestamp.
    /// </summary>
    private static readonly Dictionary<string, string> s_localClockAllowed = new(System.StringComparer.Ordinal)
    {
        ["JobHistoryCollector.cs"] =
            "msdb run_date/run_datetime are the server's local wall clock; the window is compared in that frame",
        ["ProcedureStatsCollector.cs"] =
            "dm_exec_procedure_stats.last_execution_time is local wall clock",
        ["QueryStatsCollector.cs"] =
            "dm_exec_query_stats.last_execution_time is local; creation_time feeds a DATEDIFF duration",
        ["RunningJobsCollector.cs"] =
            "DATEDIFF against start_execution_date is a duration, and the msdb window is local",
        ["ServerPropertiesCollector.cs"] =
            "DATEDIFF(MINUTE, GETUTCDATE(), GETDATE()) IS the utc_offset_minutes measurement",
        ["SessionSummaryStatsCollector.cs"] =
            "des.last_request_end_time is local wall clock; only a boolean is stored",
    };

    private static readonly Regex s_localClock = new(
        @"\b(?:GETDATE|SYSDATETIME)\s*\(\s*\)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The two ring-buffer collectors whose <c>sample_time</c> the measurement above indicted. Named
    /// because they are the regression this pin exists for.
    /// </summary>
    private static readonly string[] s_mustBeUtc =
    {
        "CpuUtilizationCollector.cs",
        "MemoryPressureEventsCollector.cs",
    };

    [Fact]
    public void OnlyTheAllowlistedCollectors_UseSqlServersLocalClock()
    {
        var offenders = new List<string>();
        var users = new SortedSet<string>(System.StringComparer.Ordinal);

        foreach (var path in CollectorSources())
        {
            var sql = WithoutComments(File.ReadAllText(path));
            var match = s_localClock.Match(sql);

            if (!match.Success)
            {
                continue;
            }

            var file = Path.GetFileName(path);
            users.Add(file);

            if (!s_localClockAllowed.ContainsKey(file))
            {
                offenders.Add($"{file}:{LineOf(sql, match.Index)}");
            }
        }

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} collector(s) use SQL Server's LOCAL clock with no recorded reason. If the "
            + "value is STORED, use SYSUTCDATETIME()/GETUTCDATE(): a local absolute timestamp in the store is "
            + "unrecoverable, since the offset is the deployment's and moves with DST. If it is compared "
            + "against a DMV column that is itself local wall clock, or feeds a DATEDIFF duration, add the "
            + $"file to s_localClockAllowed with that reason: {string.Join(", ", offenders)}");

        /* Allowlist rot in the other direction: an entry whose file no longer uses a local clock is stale
           permission that would wave through a future reintroduction. Dropping a local-clock use is a good
           change, so this asks for the LIST to be trimmed with it. */
        var stale = s_localClockAllowed.Keys.Where(f => !users.Contains(f)).ToArray();

        Assert.True(
            stale.Length == 0,
            $"{stale.Length} allowlist entr(ies) no longer use a local clock and should be removed, or they "
            + $"stand as unearned permission for a future one: {string.Join(", ", stale)}");
    }

    /// <summary>
    /// The indicted collectors must carry the UTC clock and no local one. Asserted on the shipped source
    /// rather than on a transcription, and stated both ways round: the UTC call must be present AND no local
    /// call may remain, so replacing one of two ring-buffer arms cannot pass.
    /// </summary>
    [Fact]
    public void TheRingBufferCollectors_StampSampleTimeInUtc()
    {
        foreach (var file in s_mustBeUtc)
        {
            var sql = WithoutComments(File.ReadAllText(Path.Combine(CollectorsDirectory(), file)));

            Assert.Contains("SYSUTCDATETIME()", sql, System.StringComparison.Ordinal);

            Assert.False(
                s_localClock.IsMatch(sql),
                $"{file} computes a STORED sample_time from the ring buffer, so it must use "
                + "SYSUTCDATETIME(). A local clock here sat exactly one UTC offset behind the "
                + "collection_time written by the same run - 4h on 42 of 42 us-east-2 servers, with the "
                + "collector reporting SUCCESS throughout.");
        }
    }

    /// <summary>
    /// Comment spans blanked, string LITERALS preserved — the opposite of
    /// <c>CSharpSourceWalker.StripCommentsAndStrings</c>, and deliberately so: the collector queries ARE
    /// verbatim literals, so blanking literals would leave nothing to scan. Both C# and embedded T-SQL
    /// comment forms are removed, because this codebase's SQL carries its reasoning in
    /// <c>/* ... */</c> blocks that routinely name the very functions being matched.
    /// </summary>
    private static string WithoutComments(string text)
    {
        var withoutBlocks = Regex.Replace(text, @"/\*.*?\*/", " ", RegexOptions.Singleline);
        var withoutSlashSlash = Regex.Replace(withoutBlocks, @"//[^\r\n]*", " ");

        return Regex.Replace(withoutSlashSlash, @"--[^\r\n]*", " ");
    }

    private static int LineOf(string text, int index) => text.Take(index).Count(c => c == '\n') + 1;

    private static string CollectorsDirectory() => Path.Combine(RepoRoot(), "PerformanceMonitor.Collectors");

    private static IEnumerable<string> CollectorSources()
    {
        var dir = CollectorsDirectory();

        Assert.True(Directory.Exists(dir), $"collectors project not found: {dir}");

        var paths = Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories)
            .Where(p => !p.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", System.StringComparison.Ordinal)
                        && !p.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", System.StringComparison.Ordinal))
            .OrderBy(p => p, System.StringComparer.Ordinal)
            .ToArray();

        Assert.True(paths.Length >= 50, $"the collector sweep found only {paths.Length} files - the project has moved");

        return paths;
    }

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
