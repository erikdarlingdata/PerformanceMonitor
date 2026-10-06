// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348 (statement filter, PR B): a census over every SQL Server collector definition's payload columns. A column
/// whose name looks like it can hold statement text, a plan, an event document or a script (the
/// <see cref="PayloadPattern"/>) must be listed below as one of three things, so a new column cannot reach the
/// store without somebody deciding what happens to it:
/// <list type="bullet">
/// <item><b>Hooked</b>: the collector passes the value through the statement filter before the write. Each entry
/// names its row in the plan's collection-write-path table and the lane that lands the hook. A row whose hook has
/// not landed yet is "pending: Rn"; the integration branch flips <see cref="Pending"/> to false per row.</item>
/// <item><b>NullByDesign</b>: the collector writes NULL there on purpose.</item>
/// <item><b>Exempt</b>: the name matches but the value is not statement content, with the reason spelled out.</item>
/// </list>
/// A new matching column fails <see cref="EveryMatchingPayloadColumn_IsListed"/> until it is listed. A listed
/// column that no longer exists or no longer matches fails <see cref="EveryListedColumn_StillMatchesADefinition"/>,
/// so the list cannot rot. PostgreSQL definitions are out of scope: they are a separate engine with their own
/// filter (<c>PgSensitiveStatementFilter</c>).
/// </summary>
public sealed class StatementColumnCensusTests
{
    private static readonly Regex PayloadPattern = new(
        "sql_text$|query_text$|statement_text$|text_data$|_xml$|query_plan|event_xml|implementation_script|^message$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private enum Kind
    {
        Hooked,
        NullByDesign,
        Exempt,
    }

    private sealed record Entry(Kind Kind, string Detail, string? Lane = null, bool Pending = false);

    private static Entry Hooked(string row, string lane, bool pending = true) =>
        new(Kind.Hooked, "6.7 row " + row, lane, pending);

    private static Entry Null(string reason) => new(Kind.NullByDesign, reason);

    private static Entry Exempt(string reason) => new(Kind.Exempt, reason);

    /// <summary>Keyed "definition.column" (the definition's Name, which is what schedules and logs call it).</summary>
    private static readonly Dictionary<string, Entry> Listed = new(StringComparer.Ordinal)
    {
        /* Rows 1-4: query_stats and procedure_stats text and plans, inline and deferred fetch (R2). */
        ["query_stats.query_text"] = Hooked("1", "R2"),
        ["query_stats.query_plan_xml"] = Hooked("2 and 3 (deferred plan fetch)", "R2"),
        ["procedure_stats.query_plan_xml"] = Hooked("3 and 4 (deferred plan fetch, inline)", "R2"),
        ["query_stats.query_plan_hash"] = Exempt("a hash of the plan, not plan text; the filter has nothing to read"),
        ["query_stats.query_plan_xml_bytes"] = Exempt("the byte length of the stored plan, a number"),
        ["procedure_stats.query_plan_xml_bytes"] = Exempt("the byte length of the stored plan, a number"),

        /* Row 5: Query Store. Lite stores it live; Darling stores it live and from backfill (R3). */
        ["query_store.query_text"] = Hooked("5", "R3"),
        ["query_store.query_plan_hash"] = Exempt("a hash of the plan, not plan text; the filter has nothing to read"),
        ["query_store.query_plan_text"] = Null("the main Query Store query writes a typed NULL here (QueryStoreCollector.cs:738); plan text reaches the store only through the separate by-ids fetch and the plan writer (6.7 row 7, R3)"),

        /* Row 8: the live snapshot of running requests (R6). */
        ["query_snapshots.query_text"] = Hooked("8", "R6"),
        ["query_snapshots.query_plan"] = Hooked("8", "R6"),
        ["query_snapshots.live_query_plan"] = Hooked("8", "R6"),

        /* Rows 9-11: blocked process reports (R4). */
        ["blocked_process_report.blocked_sql_text"] = Hooked("9", "R4"),
        ["blocked_process_report.blocking_sql_text"] = Hooked("9", "R4"),
        ["blocked_process_report.blocked_process_report_xml"] = Hooked("10", "R4"),
        ["blocked_process_report.blocked_query_plan_xml"] = Hooked("11", "R4"),
        ["blocked_process_report.blocking_query_plan_xml"] = Hooked("11", "R4"),

        /* Rows 12-14: deadlocks (R4). */
        ["deadlocks.victim_sql_text"] = Hooked("12", "R4"),
        ["deadlocks.deadlock_graph_xml"] = Hooked("13", "R4"),
        ["deadlocks.victim_query_plan_xml"] = Hooked("14", "R4"),

        /* Row 15: the DMV blocking snapshot (R4). */
        ["dmv_blocking_snapshot.blocked_sql_text"] = Hooked("15", "R4"),
        ["dmv_blocking_snapshot.blocking_sql_text"] = Hooked("15", "R4"),

        /* Rows 16-18 and 21: the event and history collectors (R5). */
        ["long_query_completions.statement_text"] = Hooked("16", "R5"),
        ["default_trace_events.text_data"] = Hooked("17", "R5"),
        ["system_health_events.event_xml"] = Hooked("18", "R5"),
        ["job_history.message"] = Hooked("21", "R5"),

        /* Row 19: plan correction (R6). */
        ["plan_correction.query_text"] = Hooked("19", "R6"),
        ["plan_correction.implementation_script"] = Hooked("19", "R6"),
    };

    /// <summary>
    /// Columns the pattern does not match but the plan names as exempt, so a later rename into the pattern, or a
    /// removal, is noticed. Each must exist in its definition and must NOT match the pattern (a match belongs in
    /// <see cref="Listed"/>).
    /// </summary>
    private static readonly Dictionary<string, string> WatchedExempt = new(StringComparer.Ordinal)
    {
        ["index_object_stats.filter_definition"] = "an index filter predicate over bracketed column names (IndexObjectStatsCollector.cs), not a statement",
        ["waiting_tasks.resource_description"] = "the collector writes a literal NULL there (WaitingTasksCollector.cs)",
    };

    private static IEnumerable<(string Definition, string Column)> SqlServerPayloadColumns() =>
        CollectorCatalog.All
            .Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer)
            .SelectMany(d => d.PayloadColumns.Select(c => (d.Name, c.Name)));

    /// <summary>The census itself, over any set of (definition, column) pairs; the plant test feeds it a fake one.</summary>
    private static string[] Unlisted(IEnumerable<(string Definition, string Column)> columns) =>
        columns
            .Where(c => PayloadPattern.IsMatch(c.Column))
            .Select(c => c.Definition + "." + c.Column)
            .Where(key => !Listed.ContainsKey(key))
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToArray();

    [Fact]
    public void EveryMatchingPayloadColumn_IsListed()
    {
        var unlisted = Unlisted(SqlServerPayloadColumns());

        Assert.True(
            unlisted.Length == 0,
            "These SQL Server payload columns look like statement, plan or event content but are not in " +
            "StatementColumnCensusTests.Listed. Hook each one through the statement filter (and list it as Hooked " +
            "with its plan row and lane), or list it as NullByDesign or Exempt with the reason: " +
            string.Join(", ", unlisted));
    }

    [Fact]
    public void ANewMatchingColumn_IsReportedUntilListed()
    {
        var planted = SqlServerPayloadColumns()
            .Append(("query_stats", "extra_statement_text"))
            .Append(("wait_stats", "wait_type"))
            .ToArray();

        var unlisted = Unlisted(planted);

        Assert.Equal(new[] { "query_stats.extra_statement_text" }, unlisted);
    }

    [Fact]
    public void EveryListedColumn_StillMatchesADefinition()
    {
        var actual = SqlServerPayloadColumns()
            .Where(c => PayloadPattern.IsMatch(c.Column))
            .Select(c => c.Definition + "." + c.Column)
            .ToHashSet(StringComparer.Ordinal);

        var stale = Listed.Keys.Where(k => !actual.Contains(k)).OrderBy(k => k, StringComparer.Ordinal).ToArray();

        Assert.True(
            stale.Length == 0,
            "These census entries name a column that no longer exists in a SQL Server definition or no longer " +
            "matches the payload pattern; remove or fix them: " + string.Join(", ", stale));
    }

    [Fact]
    public void EveryWatchedExemptColumn_ExistsAndStaysOutsideThePattern()
    {
        var all = SqlServerPayloadColumns().Select(c => c.Definition + "." + c.Column).ToHashSet(StringComparer.Ordinal);

        foreach (var (key, reason) in WatchedExempt)
        {
            Assert.False(string.IsNullOrWhiteSpace(reason), key + " needs a reason");
            Assert.True(all.Contains(key), key + " is no longer a SQL Server payload column; remove it from WatchedExempt");

            var column = key[(key.IndexOf('.', StringComparison.Ordinal) + 1)..];
            Assert.False(PayloadPattern.IsMatch(column), key + " now matches the payload pattern; move it into Listed");
        }
    }

    [Fact]
    public void EveryEntry_CarriesItsReasonRowAndLane()
    {
        foreach (var (key, entry) in Listed)
        {
            Assert.False(string.IsNullOrWhiteSpace(entry.Detail), key + " needs a plan row or a reason");

            if (entry.Kind == Kind.Hooked)
            {
                Assert.Matches("^R[0-9]+$", entry.Lane ?? string.Empty);
                Assert.StartsWith("6.7 row ", entry.Detail, StringComparison.Ordinal);
            }
            else
            {
                Assert.Null(entry.Lane);
                Assert.False(entry.Pending, key + ": only a Hooked entry can be pending");
            }
        }
    }

    [Fact]
    public void TheCensusCountsWhatItReports()
    {
        var matched = SqlServerPayloadColumns().Count(c => PayloadPattern.IsMatch(c.Column));
        Assert.Equal(matched, Listed.Count);

        Assert.Equal(matched, Listed.Values.Count(e => e.Kind == Kind.Hooked)
            + Listed.Values.Count(e => e.Kind == Kind.NullByDesign)
            + Listed.Values.Count(e => e.Kind == Kind.Exempt));
    }
}
