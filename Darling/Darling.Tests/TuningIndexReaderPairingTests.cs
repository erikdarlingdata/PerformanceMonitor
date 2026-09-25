/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4247: every remaining <c>CREATE INDEX</c> in <see cref="PgTableTuning.Statements"/> is paired with a named,
/// real reader constant that actually references the index's LEADING key column — so an index added here
/// without a reader that could use it fails the build, the same discipline the #4247 measurement pass applied
/// retroactively to the five that got dropped for having none. This checks the access PATH (a real reader
/// touches the column that makes the index selective), not the full column set — a covering index's INCLUDE
/// list is pinned against its own read elsewhere (e.g. <see cref="ForcePlanFailuresAccessPathTests"/>).
/// </summary>
public sealed class TuningIndexReaderPairingTests
{
    /// <summary>index name -> (leading key column, the real reader SQL constant that must reference it).
    /// <see cref="DarlingStoreMetricsReader.StoreMetricsLatestSql"/> is a skip-scan (no bound value for
    /// object_kind — it walks every distinct one via a recursive CTE), so the check below is column
    /// PRESENCE, not an equality predicate; the other three bind the column with a plain <c>= $n</c>.</summary>
    private static readonly IReadOnlyDictionary<string, (string LeadingColumn, string ReaderSql)> ReaderByIndex =
        new Dictionary<string, (string, string)>
        {
            ["idx_query_stats_server_hash_time"] = ("server_id", DarlingStoredPlanReader.QueryStatsPlanXmlByHashSql),
            ["idx_query_store_stats_server_db_query_plan_time"] = ("server_id", DarlingStoredPlanReader.QueryStorePlanTextSql),
            ["idx_query_store_stats_server_time_forcing"] = ("server_id", DarlingAlertReadAdapter.ForcePlanFailuresSql),
            ["idx_store_metrics_kind_name_time"] = ("object_kind", DarlingStoreMetricsReader.StoreMetricsLatestSql),
        };

    /// <summary>Every <c>CREATE INDEX IF NOT EXISTS &lt;name&gt; ON ... (&lt;leading&gt;, ...)</c> statement
    /// still in the list, with its name and leading (first key) column extracted from the statement itself —
    /// so a future addition is picked up automatically rather than needing a second hand-maintained list.</summary>
    private static IEnumerable<(string IndexName, string LeadingColumn)> CreatedIndexes()
    {
        var pattern = new Regex(
            @"CREATE INDEX IF NOT EXISTS (?<name>\w+) ON collect\.\w+ \((?<leading>\w+)",
            RegexOptions.Compiled);

        foreach (var statement in PgTableTuning.Statements)
        {
            var match = pattern.Match(statement);
            if (match.Success)
            {
                yield return (match.Groups["name"].Value, match.Groups["leading"].Value);
            }
        }
    }

    [Fact]
    public void EveryCreatedIndex_HasANamedReader_ThatReferencesItsLeadingColumn()
    {
        var created = CreatedIndexes().ToList();

        /* Sanity: this must actually see the four indexes the list ships today (#4247 dropped the other
           five), or the regex/list drifted and the pairing check below would be vacuously true. */
        Assert.Equal(4, created.Count);

        foreach (var (indexName, leadingColumn) in created)
        {
            Assert.True(ReaderByIndex.TryGetValue(indexName, out var reader),
                $"{indexName} has no paired reader in {nameof(ReaderByIndex)} — an index with no reader must fail this test (#4247).");

            Assert.Equal(leadingColumn, reader.LeadingColumn);

            var referencesColumn = new Regex($@"\b{Regex.Escape(leadingColumn)}\b", RegexOptions.IgnoreCase);
            Assert.True(referencesColumn.IsMatch(reader.ReaderSql),
                $"{indexName}'s paired reader never references its leading column '{leadingColumn}':\n{reader.ReaderSql}");
        }
    }
}
