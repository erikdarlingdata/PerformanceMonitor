/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common.QueryTags;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /// <summary>
    /// #5990: the sightings of one written batch, as one upsert into <c>query_tag_sightings</c>. Each row's
    /// <c>DescribeQueryTagSightings</c> hook (#5980) names its keys; the batch is folded to one entry per key
    /// (the first sighting's caller fields and text sample, a count of how often the key was seen) and written
    /// with ONE statement, so a batch costs at most one extra write and a batch with no sightings costs none.
    /// A key already stored keeps its first_seen and text sample, gains the count and gets a new last_seen.
    /// The distinct keys of one batch are capped at <see cref="QueryTagLimits.MaxKeysPerServerPerPass"/>.
    /// <para>Failure-isolated: a throw here is logged and never fails the collection, whose rows are already
    /// written by the time this runs.</para>
    /// </summary>
    internal static void WriteQueryTagSightings<TRow>(
        DuckDBConnection duckConnection,
        ICollectorDefinition<TRow> definition,
        List<TRow> rows,
        int serverId)
    {
        try
        {
            var folded = new Dictionary<(QueryTagKeyKind Kind, string Key, string App, string Login, string Host), (QueryTagSighting First, long Count)>();
            foreach (var row in rows)
            {
                var sightings = definition.DescribeQueryTagSightings(row);
                if (sightings is null)
                {
                    continue;
                }

                foreach (var s in sightings)
                {
                    var id = (s.KeyKind, s.Key, s.Application ?? string.Empty, s.Login ?? string.Empty, s.Host ?? string.Empty);
                    if (folded.TryGetValue(id, out var seen))
                    {
                        folded[id] = (seen.First, seen.Count + 1);
                    }
                    else if (folded.Count < QueryTagLimits.MaxKeysPerServerPerPass)
                    {
                        folded[id] = (s, 1);
                    }
                }
            }

            if (folded.Count == 0)
            {
                return;
            }

            var kinds = new List<string>(folded.Count);
            var keys = new List<string>(folded.Count);
            var databases = new List<string>(folded.Count);
            var objects = new List<string>(folded.Count);
            var applications = new List<string>(folded.Count);
            var logins = new List<string>(folded.Count);
            var hosts = new List<string>(folded.Count);
            var samples = new List<string>(folded.Count);
            var counts = new List<long>(folded.Count);
            foreach (var ((kind, key, _, _, _), (first, count)) in folded)
            {
                kinds.Add(KindText(kind));
                keys.Add(key);
                databases.Add(first.Database ?? string.Empty);
                objects.Add(first.ObjectRef?.Qualified ?? string.Empty);
                applications.Add(first.Application ?? string.Empty);
                logins.Add(first.Login ?? string.Empty);
                hosts.Add(first.Host ?? string.Empty);
                samples.Add(QueryTagLimits.CutText(first.TextSample) ?? string.Empty);
                counts.Add(count);
            }

            using var command = duckConnection.CreateCommand();
            /* Empty text stands for NULL in the lists (a list parameter cannot be relied on to carry NULL
               elements), and NULLIF turns it back. The timestamps are explicit UTC, never now(), which is local time. */
            command.CommandText = @"
INSERT INTO query_tag_sightings
    (server_id, key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample,
     first_seen, last_seen, seen_count)
SELECT $1, k, c_key, NULLIF(d, ''), NULLIF(o, ''), a, l, h, NULLIF(t, ''), $11, $11, n
FROM (SELECT UNNEST($2) AS k, UNNEST($3) AS c_key, UNNEST($4) AS d, UNNEST($5) AS o, UNNEST($6) AS a,
             UNNEST($7) AS l, UNNEST($8) AS h, UNNEST($9) AS t, UNNEST($10) AS n)
ON CONFLICT (server_id, key_kind, key, application_name, login_name, host_name) DO UPDATE SET
    seen_count = query_tag_sightings.seen_count + excluded.seen_count,
    last_seen = excluded.last_seen";
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = kinds });
            command.Parameters.Add(new DuckDBParameter { Value = keys });
            command.Parameters.Add(new DuckDBParameter { Value = databases });
            command.Parameters.Add(new DuckDBParameter { Value = objects });
            command.Parameters.Add(new DuckDBParameter { Value = applications });
            command.Parameters.Add(new DuckDBParameter { Value = logins });
            command.Parameters.Add(new DuckDBParameter { Value = hosts });
            command.Parameters.Add(new DuckDBParameter { Value = samples });
            command.Parameters.Add(new DuckDBParameter { Value = counts });
            command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
            command.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            AppLogger.Warn("QueryTags", $"The query-tag sightings write for {definition.Name} failed; the collection is unaffected: {ex.Message}");
        }
    }

    internal static string KindText(QueryTagKeyKind kind) => kind switch
    {
        QueryTagKeyKind.Hash => "hash",
        QueryTagKeyKind.Object => "object",
        _ => "caller",
    };

    /// <summary>
    /// #5990: the map step after a collection round. Rebuilds the tag map when the rules are ahead of it, otherwise
    /// extends it for the keys sighted since the last pass. Failure-isolated like the sightings write.
    /// </summary>
    internal async Task RunQueryTagMapPassAsync(CancellationToken cancellationToken)
    {
        try
        {
            /* The cursors live in memory, per server, and only a pass that committed moves them (the pass updates the
               dictionary after its commit). A fresh process starts from the epoch. */
            var result = await new LocalDataService(_duckDb).BuildQueryTagMapAsync(_queryTagMapCursors);
            if (result.KeysResolved > 0)
            {
                AppLogger.Debug("QueryTags", $"Tag map {(result.Rebuilt ? "rebuilt" : "extended")}: {result.KeysResolved} keys resolved, {result.KeysTagged} tagged");
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            AppLogger.Warn("QueryTags", $"The query-tag map pass failed; the next pass retries: {ex.Message}");
        }
    }

    private readonly Dictionary<int, QueryTagMapCursor> _queryTagMapCursors = new();
}
