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
            ["long_query_completions.statement_text"] = "StatementCollectionCensusTests.LongQueryCompletions_StatementText_IsWithheldAndThePlainStatementIsUntouched",
            ["default_trace_events.text_data"] = "StatementCollectionCensusTests.DefaultTraceEvents_TextData_IsWithheldAndThePlainStatementIsUntouched",
            ["system_health_events.event_xml"] = "StatementCollectionCensusTests.SystemHealthEvents_EventXml_WithholdsTheSqlTextActionAndKeepsTheRestOfTheEvent",
            ["job_history.message"] = "StatementCollectionCensusTests.JobHistory_Message_IsWithheldWhenItEchoesAStatement_AndAPlainMessageIsUntouched",
            ["query_store.query_text"] = "StatementCollectionCensusTests.QueryStore_QueryText_IsWithheld",
        };
}
