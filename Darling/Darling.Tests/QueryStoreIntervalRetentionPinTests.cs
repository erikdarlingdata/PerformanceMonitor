/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Source pins for the retention of the day-partitioned Query Store interval tables (#5571). A <c>ctid</c> repeats across
/// partitions, so a <c>DELETE ... WHERE ctid IN (SELECT ctid ...)</c> against the partitioned PARENT can delete live
/// rows of another day. The row-capped purge must therefore name the legacy table, and the DEFAULT delete must name the
/// DEFAULT partition. The live check that proves the behavior is in <see cref="QueryStoreIntervalRetentionLiveTests"/>;
/// these catch the rewrite before it reaches a database.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class QueryStoreIntervalRetentionPinTests
{
    private const string PurgeStart = "internal static async Task<IntervalPartitionPurge> PurgeIntervalTableAsync(";
    private const string PurgeEnd = "/// <summary>Whether a schema-qualified relation exists";

    private static string ReadSource(string project, string file, [CallerFilePath] string callerFile = "")
    {
        var relative = Path.Combine("Darling", project, file);
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(callerFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate);
            }
        }

        throw new FileNotFoundException(file + " not found above " + callerFile);
    }

    private static string PurgeBody([CallerFilePath] string callerFile = "")
    {
        var source = ReadSource("PerformanceMonitor.Darling.Service", "DarlingRetention.cs", callerFile);
        var start = source.IndexOf(PurgeStart, StringComparison.Ordinal);
        Assert.True(start >= 0, "PurgeIntervalTableAsync moved or was renamed");
        var end = source.IndexOf(PurgeEnd, start, StringComparison.Ordinal);
        Assert.True(end > start, "the end of PurgeIntervalTableAsync could not be found");
        return source.Substring(start, end - start);
    }

    [Fact]
    public void ThePurgeNamesTheLegacyTable_NeverTheParent()
    {
        var body = PurgeBody();

        Assert.Contains("RowCappedDeleteSql(table.Legacy", body, StringComparison.Ordinal);
        Assert.DoesNotContain("RowCappedDeleteSql(table.Parent", body, StringComparison.Ordinal);
        Assert.DoesNotContain("RowCappedDeleteSql(table.Name", body, StringComparison.Ordinal);

        /* Every PurgeOneAsync call in the purge names the legacy table or the DEFAULT partition as its table: the second argument. */
        var calls = Regex.Matches(body, @"PurgeOneAsync\(\s*postgres,\s*(?<table>[^,\s]+),").Select(m => m.Groups["table"].Value).ToList();
        Assert.Equal(2, calls.Count);
        Assert.All(calls, c => Assert.True(c is "table.Legacy" or "table.Default", $"PurgeOneAsync names {c}"));
    }

    [Fact]
    public void TheDefaultDeleteNamesTheDefaultPartition_AndIsAPlainCutoffDelete()
    {
        var sql = DarlingRetention.DefaultPartitionDeleteSql(QueryStoreIntervalPartitions.Wide);

        Assert.Equal("DELETE FROM collect.query_store_interval_wide_default WHERE first_execution_time < $1", sql);
        Assert.DoesNotContain("ctid", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            "DELETE FROM collect.query_store_interval_latest_default WHERE first_execution_time < $1",
            DarlingRetention.DefaultPartitionDeleteSql(QueryStoreIntervalPartitions.Latest));
    }

    [Fact]
    public void NoPurgeStatementInRetentionNamesAnIntervalParentByLiteral()
    {
        var source = ReadSource("PerformanceMonitor.Darling.Service", "DarlingRetention.cs");

        /* The parents are reachable only through the table descriptors (and only for logging); a literal parent name in a
           purge call is the pre-partitioning shape. */
        var literal = new Regex(@"(RowCappedDeleteSql|PurgeOneAsync)\([^;]*""(collect\.)?query_store_interval_(wide|latest)""");
        Assert.DoesNotMatch(literal, source);
    }

    [Fact]
    public void TheLegacyDropIsGatedOnPromotion_AndTheLegacyTableCanNeverBeSelectedWhileUnbounded()
    {
        var source = ReadSource("PerformanceMonitor.Darling.Storage", "QueryStoreIntervalPartitions.cs");
        var start = source.IndexOf("public static async Task<StepResult> DropExpiredAsync(", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = source.IndexOf("public static async Task<StepResult> RunMaintenanceAsync(", start, StringComparison.Ordinal);
        var body = source.Substring(start, end - start);

        /* The first statement after reading the state: not promoted means not ready, and nothing is dropped. */
        var gate = body.IndexOf("if (!state.Promoted)", StringComparison.Ordinal);
        var drop = body.IndexOf("DROP TABLE", StringComparison.Ordinal);
        Assert.True(gate >= 0 && drop > gate, "DropExpiredAsync must check Promoted before any DROP");
        Assert.Contains("StepOutcome.NotReady", body.Substring(gate, drop - gate), StringComparison.Ordinal);
        Assert.Contains("SET LOCAL lock_timeout", source, StringComparison.Ordinal);
        Assert.Contains("IsLockTimeout(ex)", body, StringComparison.Ordinal);

        /* An unbounded legacy table and DEFAULT are never in the expired set. */
        var expired = source.IndexOf("public static IReadOnlyList<PartitionInfo> ExpiredPartitions(", StringComparison.Ordinal);
        var expiredBody = source.Substring(expired, 400);
        Assert.Contains("!p.IsDefault", expiredBody, StringComparison.Ordinal);
        Assert.Contains("!p.UpperUnbounded", expiredBody, StringComparison.Ordinal);
    }
}
