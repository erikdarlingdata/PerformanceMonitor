// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Source pin: the "latest row per index" read must end its ORDER BY with <c>collection_id DESC</c>. Two rows for one
/// index can share a collection_time, and DISTINCT ON without a final tiebreak returns whichever row PostgreSQL
/// happens to see first, so the Index Analysis tab and get_finops index_analysis could show either one. The live
/// golden (<c>FinOpsIndexAnalysisGoldenLiveTests</c>) seeds such a tie; this pin fails on every rig, not only the
/// ones where the planner picks the wrong row.
/// </summary>
public sealed class FinOpsIndexAnalysisLatestTiebreakTests
{
    [Fact]
    public void IndexObjectStatsLatestSql_BreaksACollectionTimeTieWithCollectionIdDesc()
    {
        var sql = DarlingFinOpsIndexAnalysisReader.IndexObjectStatsLatestSql;
        Assert.EndsWith("s.collection_time DESC, s.collection_id DESC", sql.TrimEnd());
    }

    [Fact]
    public void ServerCompressionInfoSql_BreaksACollectionTimeTieWithCollectionIdDesc()
    {
        var sql = DarlingFinOpsIndexAnalysisReader.ServerCompressionInfoSql;
        Assert.Contains("ORDER BY collection_time DESC, collection_id DESC", sql);
    }
}
