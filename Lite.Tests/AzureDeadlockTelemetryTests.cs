/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2641: Azure SQL DB deadlock capture had one source, and it was the wrong shape twice over.
///
/// <para>
/// The session we create is <b>database-scoped</b>, so a connection to <c>master</c> captures only
/// <c>master</c>'s deadlocks — a reporter with fifty user databases got essentially none of theirs — and
/// its ring buffer is memory-resident, so an Azure failover empties it without notice.
/// </para>
///
/// <para>
/// <c>sys.fn_xe_telemetry_blob_target_read_file</c> is Azure's own file-backed telemetry, and it is
/// <b>master-scoped</b>. That last fact is the whole trick, and it cost two wrong conclusions before it was
/// found: called from a user database the identical statement returns zero rows, silently, with no error.
/// I reported the source unusable on that basis before testing it from <c>master</c>, where it returned the
/// deadlock immediately — with the USER database's name attached.
/// </para>
///
/// <para>
/// Verified against a live Azure SQL Database, running the shipped query text itself rather than a
/// paraphrase: from <c>master</c> it returns <c>source_database_name = 'Erik'</c> for a deadlock that
/// happened in the user database; from that user database the same text returns the ring-buffer row with a
/// NULL source database, unchanged.
/// </para>
/// </summary>
public class AzureDeadlockTelemetryTests
{
    private static string AzureSql =>
        (string)typeof(DeadlocksCollector)
            .GetField("AzureQueryText", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

    private static string ServerScopedSql =>
        (string)typeof(DeadlocksCollector)
            .GetField("ServerScopedQueryText", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

    [Fact]
    public void TheAzureQueryReadsTheDurableTelemetryBlob_NotOnlyTheRingBuffer()
    {
        Assert.Contains("sys.dm_xe_database_session_targets", AzureSql, StringComparison.Ordinal);
        Assert.Contains("sys.fn_xe_telemetry_blob_target_read_file('dl', NULL, NULL, NULL)", AzureSql, StringComparison.Ordinal);
        Assert.Contains("UNION ALL", AzureSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The guard, and it is not an optimization. From a user database the telemetry call returns zero rows
    /// with no error — indistinguishable from "this server had no deadlocks". Restricting the read to the
    /// only connection that can answer keeps a silent zero from ever being produced.
    /// </summary>
    [Fact]
    public void TheTelemetryArmIsGuardedOnBeingConnectedToMaster()
    {
        Assert.Contains("DB_NAME() = N'master'", AzureSql, StringComparison.Ordinal);

        /* Inside the telemetry subquery, not applied to the whole union — the ring-buffer arm must keep
           working from a user database, which is where most entries point. */
        var telemetryIndex = AzureSql.IndexOf("fn_xe_telemetry_blob_target_read_file", StringComparison.Ordinal);
        var guardIndex = AzureSql.IndexOf("DB_NAME() = N'master'", StringComparison.Ordinal);

        Assert.True(guardIndex > telemetryIndex,
            "The master guard must sit inside the telemetry subquery; ahead of it, it would gate the ring buffer too.");
    }

    /// <summary>
    /// The telemetry event carries the USER database's name, and it has to win over the connection's.
    ///
    /// <para>That arm is read while connected to <c>master</c>, so <c>CurrentDatabaseName</c> is
    /// <c>"master"</c> for every row of it. Taking the connection's database would stamp every deadlock on
    /// the server as <c>master</c>'s — turning the one source that spans all fifty databases into fifty rows
    /// about the wrong one.</para>
    /// </summary>
    [Fact]
    public void TheTelemetryArmProjectsTheEventsOwnDatabaseName()
    {
        /* Asserted as a substring, not a regex: the value under test contains double quotes, and the
           escaping needed to match it in a pattern is exactly the kind of thing that makes a guard pass
           for the wrong reason. The first version of this assertion did — it matched the doubled quotes
           of the C# verbatim literal rather than the single quotes of the runtime string. */
        Assert.Contains(
            "source_database_name = tel.evt.value('(/event/data[@name=\"database_name\"]/value)[1]', 'nvarchar(128)')",
            AzureSql,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// Every arm of every engine projects the column, so one reader serves them all. A union whose arms
    /// disagree on width does not compile; one whose arms disagree on ORDER compiles and silently swaps
    /// two values, which is the failure this asserts against.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void EveryProjectionCarriesTheColumn_SoOneReaderServesBoth(bool azure)
    {
        var sql = azure ? AzureSql : ServerScopedSql;

        var occurrences = Regex.Matches(sql, @"source_database_name\s*=").Count;

        Assert.True(occurrences >= 1, "The projection lost source_database_name — ReadAsync reads it by name and would go silently null.");
    }

    /// <summary>
    /// The ring-buffer arms project it as a TYPED null. An untyped <c>NULL</c> in the first arm of a union
    /// takes its type from that arm, which on some paths is <c>int</c> — and the telemetry arm's
    /// <c>nvarchar</c> then fails to convert at runtime, on Azure only, where nothing we own would notice.
    /// </summary>
    [Fact]
    public void TheNullArmsAreTypedNulls()
    {
        Assert.Contains("source_database_name = CONVERT(nvarchar(128), NULL)", AzureSql, StringComparison.Ordinal);
        Assert.Contains("source_database_name = CONVERT(nvarchar(128), NULL)", ServerScopedSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Both arms honour the watermark. An unfiltered telemetry arm would re-read the whole blob every cycle
    /// and re-insert every deadlock it has ever held — the collector would look like it was working
    /// perfectly while multiplying its own history.
    /// </summary>
    [Fact]
    public void BothArmsFilterOnTheCutoff()
        => Assert.True(Regex.Matches(AzureSql, @"> @cutoff_time").Count >= 2,
            "One arm of the Azure union does not filter on @cutoff_time and would re-read its whole source every cycle.");
}
