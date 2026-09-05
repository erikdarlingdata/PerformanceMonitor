/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Runtime.CompilerServices;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2993, Darling half: a log stamped in a non-UTC zone records as <c>PERMISSIONS</c>, not <c>ERROR</c>
/// and not a successful empty read.
///
/// <para>The refusal itself and every zone decision are pinned in Lite.Tests
/// (<c>PgDeadlockLogParserTests</c>) against the shared parser both transports use. What is only pinnable
/// here is the CLASSIFICATION, which is try/catch glue around a live collection sweep — the same shape and
/// the same reason as <see cref="DarlingLockTimeoutYieldTests"/>: source-level, because there is no way to
/// reach the arm without a monitored PostgreSQL target.</para>
///
/// <para>The status is the load-bearing part. <c>ERROR</c> would be a monitoring fault, which this is not,
/// and collection health would carry it forever for as long as the parameter group says what it says;
/// <c>PERMISSIONS</c> is the store's non-fatal degradation bucket, and
/// <c>CollectorRuntimePrecondition</c>'s arm for it already tells a reader the condition can be satisfied
/// on the monitored server and is re-derived every cycle. Both are true here.</para>
/// </summary>
public sealed class PgDeadlockLogTimezoneTests
{
    [Fact]
    public void Worker_Records_A_NonUtc_Log_As_Permissions_Not_Error()
    {
        var source = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));

        var armIndex = source.IndexOf("catch (PgLogTimezoneUnsupportedException ex)", System.StringComparison.Ordinal);
        Assert.True(armIndex >= 0, "The #2993 classification arm is gone — a non-UTC log would fall to the general handler and record ERROR every cycle.");

        /* Bounded to the arm's own body. An unscoped Contains would pass on any of the other PERMISSIONS
           arms in this file and prove nothing about this one. */
        var body = source[armIndex..];
        var close = body.IndexOf("\n            return 0;", System.StringComparison.Ordinal);
        body = close > 0 ? body[..close] : body;

        Assert.Contains("\"PERMISSIONS\"", body, System.StringComparison.Ordinal);
        Assert.DoesNotContain("\"ERROR\"", body, System.StringComparison.Ordinal);

        /* The message is the exception's own, which is where log_timezone is named. Re-authoring it in the
           arm would be a second copy of prose whose whole value is naming one setting correctly. */
        Assert.Contains("ex.Message", body, System.StringComparison.Ordinal);
    }

    /// <summary>
    /// The <c>pg_read_file</c> route's own half: the query has to RETURN the prefix zone, or the parser has
    /// nothing to check and the refusal is unreachable on the transport that has a database connection.
    /// The zone was matched and discarded here before #2993.
    /// </summary>
    [Fact]
    public void The_Collector_Query_Returns_The_Prefix_Zone()
    {
        var source = ReadRepoFile(Path.Combine("PerformanceMonitor.Collectors", "PgDeadlocksCollector.cs"));

        Assert.Contains("AS log_zone_text", source, System.StringComparison.Ordinal);

        /* [^ \n]+ rather than \w+: a numeric-offset zone matched no block at all under \w+, so the
           server reported no deadlocks instead of reporting a zone this cannot store. */
        Assert.Contains("([^ \\n]+) \\[(\\d+)\\]", source, System.StringComparison.Ordinal);
        Assert.DoesNotContain("\\w+ \\[(\\d+)\\]", source, System.StringComparison.Ordinal);
    }

    /* Locate the repo from this file — the DarlingLockTimeoutYieldTests idiom; no build-output copying. */
    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
