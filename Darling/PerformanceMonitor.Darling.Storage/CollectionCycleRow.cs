namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #5735: the one spelling of the <c>collection_log</c> row that records the collection slots a server's loop stepped
/// over (Lite's twin is <c>CollectionCycleSkip</c>, #5643). The row is not a collector run: <c>collector_name</c> is
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

    /// <summary>The predicate that leaves skipped-slots rows out of a read of <c>collection_log</c> or
    /// <c>v_collection_log</c>: <c>collector_name &lt;&gt; 'collection_cycle'</c>. A row's collector_name is NOT NULL.</summary>
    public const string NotCycleRowSql = "collector_name <> '" + CollectorName + "'";
}
