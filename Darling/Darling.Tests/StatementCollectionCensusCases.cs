// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Collections.Generic;

namespace Darling.Tests;

/// <summary>
/// #4348 (statement filter): the registry of collection-time census cases. A case is a test that plants the canary
/// in a collector's input, runs the read and the write, and asserts that no written value holds a secret needle
/// (<c>StatementScrubRecordingWriter</c> is the harness). Each lane that hooks a column through the statement filter
/// adds one line here, naming the test that proves it: the key is "definition.column", exactly as
/// <c>StatementColumnCensusTests.Listed</c> spells it, and the value is "StatementCollectionCensusTests.TestMethod": a
/// <c>[Fact]</c> or <c>[Theory]</c> with no skip, in the class <c>StatementCollectionCensusTests</c> (partial, so each
/// lane can add its file), whose source drives <c>StatementScrubRecordingWriter</c> and names the column.
/// <c>StatementColumnCensusTests.EveryNonPendingHookedColumn_HasACollectionCase</c> reads this registry: a Hooked
/// entry cannot be flipped to non-pending without a case here, and a case here that names a missing test or a column
/// that is not a Hooked entry fails too.
/// </summary>
internal static class StatementCollectionCensusCases
{
    public static readonly IReadOnlyDictionary<string, string> ByColumn =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            // Filled by the lanes that hook a column, one line each, for example:
            // ["query_stats.query_text"] = "StatementCollectionCensusTests.QueryStats_QueryText_IsWithheld",
            // R4 (blocking and deadlocks), StatementScrubBlockingTests.cs:
            ["blocked_process_report.blocked_sql_text"] = "StatementCollectionCensusTests.BlockedProcessReport_blocked_sql_text_blocking_sql_text_blocked_process_report_xml_and_both_plans_AreWithheld",
            ["blocked_process_report.blocking_sql_text"] = "StatementCollectionCensusTests.BlockedProcessReport_blocked_sql_text_blocking_sql_text_blocked_process_report_xml_and_both_plans_AreWithheld",
            ["blocked_process_report.blocked_process_report_xml"] = "StatementCollectionCensusTests.BlockedProcessReport_blocked_sql_text_blocking_sql_text_blocked_process_report_xml_and_both_plans_AreWithheld",
            ["blocked_process_report.blocked_query_plan_xml"] = "StatementCollectionCensusTests.BlockedProcessReport_blocked_sql_text_blocking_sql_text_blocked_process_report_xml_and_both_plans_AreWithheld",
            ["blocked_process_report.blocking_query_plan_xml"] = "StatementCollectionCensusTests.BlockedProcessReport_blocked_sql_text_blocking_sql_text_blocked_process_report_xml_and_both_plans_AreWithheld",
            ["deadlocks.victim_sql_text"] = "StatementCollectionCensusTests.Deadlocks_victim_sql_text_deadlock_graph_xml_and_victim_query_plan_xml_AreWithheld",
            ["deadlocks.deadlock_graph_xml"] = "StatementCollectionCensusTests.Deadlocks_victim_sql_text_deadlock_graph_xml_and_victim_query_plan_xml_AreWithheld",
            ["deadlocks.victim_query_plan_xml"] = "StatementCollectionCensusTests.Deadlocks_victim_sql_text_deadlock_graph_xml_and_victim_query_plan_xml_AreWithheld",
            ["dmv_blocking_snapshot.blocked_sql_text"] = "StatementCollectionCensusTests.DmvBlockingSnapshot_blocked_sql_text_and_blocking_sql_text_AreWithheld",
            ["dmv_blocking_snapshot.blocking_sql_text"] = "StatementCollectionCensusTests.DmvBlockingSnapshot_blocked_sql_text_and_blocking_sql_text_AreWithheld",
        };
}
