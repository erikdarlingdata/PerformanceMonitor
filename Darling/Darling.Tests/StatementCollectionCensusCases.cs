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
/// <c>StatementColumnCensusTests.Listed</c> spells it, and the value is "TestClass.TestMethod" (a <c>[Fact]</c> or
/// <c>[Theory]</c> in this assembly).
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
            // ["query_stats.query_text"] = "QueryStatsStatementFilterCensusTests.QueryText_IsWithheld",
        };
}
