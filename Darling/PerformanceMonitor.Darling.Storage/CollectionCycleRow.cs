using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #5735: the one spelling of the <c>collection_log</c> row that records the collection slots a server's loop stepped
/// over (Lite's twin is <c>CollectionCycleSkip</c>, #5643, PR #5730). The row is not a collector run: <c>collector_name</c> is
/// <see cref="CollectorName"/> and <c>status</c> is <see cref="Status"/>. It carries a real <c>server_id</c>, so
/// the data_retention sentinel trick (server_id 0, left out of every per-server read) does not hide it. Every read of
/// the log that counts runs, rates, streaks, "last collected" or "any row" leaves it out through
/// <see cref="NotCycleRowSql"/>; the reads that name one collector, or <c>status = 'SUCCESS'</c>, never see it; the raw
/// log reads show it on purpose. <c>CollectionCycleReaderCensusTests</c> holds the line for every later reader, and
/// no file but this one may spell the collector name as a literal.
/// </summary>
public static class CollectionCycleRow
{
    /// <summary>The <c>collection_log.collector_name</c> of a skipped-slots row.</summary>
    public const string CollectorName = "collection_cycle";

    /// <summary>The <c>collection_log.status</c> of a skipped-slots row.</summary>
    public const string Status = "SKIPPED";

    /// <summary>The predicate that leaves the rows that are not collector runs out of a read of <c>collection_log</c> or
    /// <c>v_collection_log</c>: the skipped-slots row and, since #5824, the scheduled analysis pass's row
    /// (<see cref="AnalysisPassRow"/>): <c>collector_name NOT IN ('collection_cycle', 'analysis_pass')</c>. A row's
    /// collector_name is NOT NULL.</summary>
    public const string NotCycleRowSql = "collector_name NOT IN ('" + CollectorName + "', '" + AnalysisPassRow.CollectorName + "')";
}

/// <summary>
/// #5824: the one spelling of the <c>collection_log</c> row each server's scheduled analysis pass writes. The pass runs inside
/// the server's sweep body, which holds a fleet collection slot, so its length is part of what a slot costs; this row is the
/// only record of it. It is not a collector run and is left out of every count of runs by
/// <see cref="CollectionCycleRow.NotCycleRowSql"/>. Its status is never <c>SUCCESS</c>: a pass that finished is
/// <see cref="Analyzed"/>, so a read that is safe only because it keeps <c>status = 'SUCCESS'</c> (which
/// <c>CollectionCycleReaderCensusTests</c> accepts unfiltered) never counts it either. The other statuses are the collectors'
/// own words: <c>ABANDONED</c> (a run past its deadline), <c>ERROR</c>, <c>SKIPPED</c>.
/// </summary>
public static class AnalysisPassRow
{
    /// <summary>The <c>collection_log.collector_name</c> of an analysis-pass row.</summary>
    public const string CollectorName = "analysis_pass";

    /// <summary>The status of a pass that finished (with findings, none, or too little data to analyze).</summary>
    public const string Analyzed = "ANALYZED";

    /// <summary>The status of a pass that ran past its budget: the collectors' word for a run past its deadline.</summary>
    public const string Abandoned = "ABANDONED";

    /// <summary>The status of a pass that failed.</summary>
    public const string Error = "ERROR";

    /// <summary>The status of a pass that did not run (the previous one still in flight, or the service stopping).</summary>
    public const string Skipped = "SKIPPED";

    /// <summary>
    /// Darling's <c>get_collection_log</c> status vocabulary: the shared one (<c>EnumeratedCollectorDriver.CollectionLogStatuses</c>,
    /// passed in because this assembly does not reference it) plus <see cref="Analyzed"/>, which only Darling writes. Lite's
    /// tool keeps the shared set, so it does not accept a word its log never carries.
    /// </summary>
    public static IReadOnlyList<string> WithAnalysisPassStatuses(IReadOnlyList<string> shared)
    {
        var all = new List<string>(shared);
        if (!all.Contains(Analyzed))
        {
            all.Insert(1, Analyzed);
        }

        return all;
    }
}
