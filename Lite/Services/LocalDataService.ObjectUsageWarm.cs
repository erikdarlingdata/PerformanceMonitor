/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.ObjectReferences;

namespace PerformanceMonitorLite.Services;

/// <summary>What one parse-cache warm did (#9885): the statement texts it read, the ones it parsed, how long it took, and whether the time budget ended it.</summary>
internal sealed record ObjectUsageWarmResult(int Texts, long Parsed, TimeSpan Elapsed, bool StoppedByBudget, bool StoppedByCharCap);

public partial class LocalDataService
{
    /// <summary>The most statement text characters one warm reads for a server (about 64 MB held), newest statements first, so a store with
    /// more text than that keeps the rest for the first reader instead of holding it all in memory here (#9885).</summary>
    internal const long ObjectUsageWarmMaxChars = 32_000_000;

    /// <summary>
    /// The statement texts of one server over the window, one per (database, query), newest first (#9885). The text is the one
    /// <see cref="ObjectUsageStatementsSql"/> reads for the same query, whichever database the reader asks about.
    /// $1 server_id, $2 the window start (naive UTC).
    /// </summary>
    internal const string ObjectUsageWarmTextsSql = @"
SELECT
    MAX(query_text)
FROM v_query_store_stats
WHERE server_id = $1
AND   database_name IS NOT NULL
AND   query_text IS NOT NULL
AND   COALESCE(interval_start_time_utc, collection_time) >= $2
AND   collection_time >= $2 - INTERVAL '1 day'
GROUP BY
    database_name,
    query_id
ORDER BY
    MAX(last_execution_time) DESC NULLS LAST";

    /// <summary>
    /// Parses the Query Store statement texts of one server into the object usage parse cache before the first <c>get_object_usage</c> reader needs
    /// them (#9885): the parse is the floor of the first call after a start, and the cache keeps every text for the life of the process. The texts
    /// are read first, with the store's read lock held for the read only, and parsed after the lock is released, so a long parse never holds a
    /// writer back. The parse stops at <paramref name="budget"/> (measured from the call's start) or when <paramref name="cancellationToken"/>
    /// fires, and what is parsed stays cached. A reader that arrives meanwhile is answered: <see cref="QueryTextScanCache"/> is thread-safe, and
    /// a text both parse at once is parsed twice at worst. A test passes its own <paramref name="scanCache"/> to count the parses.
    /// </summary>
    internal async Task<ObjectUsageWarmResult> WarmObjectUsageParseCacheAsync(
        int serverId, DateTime windowStartUtc, TimeSpan budget, QueryTextScanCache? scanCache = null, CancellationToken cancellationToken = default)
    {
        var cache = scanCache ?? ObjectUsageScanCache;
        var started = Stopwatch.GetTimestamp();
        var texts = new List<string>();
        long chars = 0;
        var charCap = false;

        using (var connection = await OpenConnectionAsync(timer: null, cancellationToken))
        using (var command = connection.CreateCommand())
        {
            command.CommandText = ObjectUsageWarmTextsSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(windowStartUtc, DateTimeKind.Unspecified) });
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var text = reader.GetString(0);
                if (string.IsNullOrWhiteSpace(text))
                {
                    continue;
                }

                if (chars + text.Length > ObjectUsageWarmMaxChars)
                {
                    charCap = true;
                    break;
                }

                chars += text.Length;
                texts.Add(text);
            }
        }

        var scansBefore = cache.Scans;
        var budgetEnded = false;
        foreach (var text in texts)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (Stopwatch.GetElapsedTime(started) >= budget)
            {
                budgetEnded = true;
                break;
            }

            cache.GetOrScan(text);
        }

        return new ObjectUsageWarmResult(texts.Count, cache.Scans - scansBefore, Stopwatch.GetElapsedTime(started), budgetEnded, charCap);
    }
}
