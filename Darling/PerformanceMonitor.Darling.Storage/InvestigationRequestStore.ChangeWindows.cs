/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>#9485: one declared change window, as <c>config.investigation_change_windows</c> holds it. A null <see cref="ServerIds"/> means every server.</summary>
public sealed record ChangeWindowRow(
    long WindowId, IReadOnlyList<int>? ServerIds, DateTime StartsAt, DateTime EndsAt, string Reason, string DeclaredBy, DateTime DeclaredAt,
    DateTime? ClearedAt, DateTime? SummarizedAt);

/// <summary>
/// #9485: a window with what it held. <see cref="State"/> is <see cref="InvestigationChangeWindows.StateUpcoming"/>, <see cref="InvestigationChangeWindows.StateOpen"/>,
/// <see cref="InvestigationChangeWindows.StateInTail"/> or <see cref="InvestigationChangeWindows.StateClosed"/>. <see cref="Held"/> counts every request the window held,
/// <see cref="Released"/> the ones a late join let go. <see cref="Servers"/> is the names, or null for every server.
/// </summary>
public sealed record ChangeWindowSummary(
    ChangeWindowRow Window, string State, DateTime TailEndsAt, IReadOnlyList<string>? Servers, int Held, int Released,
    IReadOnlyDictionary<string, int> ByFamily, IReadOnlyDictionary<string, int> ByServer);

/// <summary>#9485: one request a window held: its server label, family, open time, computed state and, when a late join released it, the release time.</summary>
public sealed record ChangeWindowHeldRequest(Guid RequestId, string ServerName, string Family, DateTime OpenedAt, string State, DateTime? ReleasedAt);

/// <summary>#9485: the answer of <see cref="InvestigationRequestStore.ReadChangeWindowsAsync"/>. <see cref="HeldRequests"/> is set only when one window was asked for.
/// <see cref="Truncated"/> says rows were left out, and <see cref="NextOffset"/> is where the next page starts.</summary>
public sealed record ChangeWindowsRead(
    IReadOnlyList<ChangeWindowSummary> Windows, IReadOnlyList<ChangeWindowHeldRequest>? HeldRequests, bool Truncated, int? NextOffset);

public sealed partial class InvestigationRequestStore
{
    /* #9485: the windows in a span ($1 is the span's start, ignored for one window), or one window ($2), newest end first. The span keeps a window
       from when its tail ends, so an upcoming or running window is always in it. $5 is the reader's server scope (NULL for every server): a window
       with no server list (every server) is always in scope. */
    internal static readonly string ChangeWindowsSql = @"
SELECT w.window_id, w.server_ids, w.starts_at, w.ends_at, w.reason, w.declared_by, w.declared_at, w.cleared_at, w.summarized_at
FROM config.investigation_change_windows AS w
WHERE ($2::bigint IS NULL OR w.window_id = $2)
  AND ($2::bigint IS NOT NULL OR w.ends_at + " + InvestigationChangeWindows.TailIntervalSql + @" >= $1)
  AND ($5::integer[] IS NULL OR w.server_ids IS NULL OR w.server_ids && $5)
ORDER BY w.ends_at DESC, w.window_id DESC
LIMIT $3 OFFSET $4";

    /* #9485: what each window held, by family and server label, from the request link. $2 is the reader's server scope. */
    internal const string ChangeWindowCountsSql = @"
SELECT r.held_window_id, r.family, r.server_name, count(*)::integer AS held, count(r.hold_released_at)::integer AS released
FROM config.investigation_requests AS r
WHERE r.held_window_id = ANY($1)
  AND ($2::integer[] IS NULL OR r.server_id = ANY($2))
GROUP BY r.held_window_id, r.family, r.server_name";

    /* #9485: the requests one window held, oldest first, with the state ListSql computes. $2 is now, $5 the reader's server scope. */
    internal const string ChangeWindowHeldSql = @"
SELECT r.request_id, r.server_name, r.family, r.opened_at,
       CASE
           WHEN r.state IN ('open', 'claimed') AND r.expires_at <= $2 THEN 'expired'
           WHEN r.state = 'claimed' AND r.lease_until < $2 AND r.claim_count >= 3 THEN 'expired'
           WHEN r.state = 'open' AND r.claim_count >= 3 THEN 'expired'
           WHEN r.state = 'claimed' AND r.lease_until < $2 THEN 'open'
           ELSE r.state
       END AS effective_state,
       r.hold_released_at
FROM config.investigation_requests AS r
WHERE r.held_window_id = $1
  AND ($5::integer[] IS NULL OR r.server_id = ANY($5))
ORDER BY r.opened_at, r.request_id
LIMIT $3 OFFSET $4";

    internal const string ChangeWindowServerNamesSql = @"
SELECT server_id, COALESCE(display_name, server_name) FROM collect.servers WHERE server_id = ANY($1)";

    /// <summary>
    /// The change windows (#9485): with <paramref name="windowId"/>, that one window and the requests it held (a page of <paramref name="limit"/>
    /// from <paramref name="offset"/>, oldest first); without it, the windows whose tail ended no earlier than <paramref name="hoursBack"/> ago,
    /// or that have not ended, newest end first, each with its counts only. A page that leaves rows out says so. Reads only.
    /// </summary>
    public async Task<ChangeWindowsRead> ReadChangeWindowsAsync(
        long? windowId, int hoursBack, int limit, int offset, CancellationToken cancellationToken = default)
    {
        var now = Now();
        var since = now - TimeSpan.FromHours(hoursBack);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);

        var rows = new List<ChangeWindowRow>();
        await using (var command = Command(connection, null, ChangeWindowsSql,
            P(since), new NpgsqlParameter<long?> { TypedValue = windowId, NpgsqlDbType = NpgsqlDbType.Bigint },
            P(windowId is null ? limit + 1 : 1), P(windowId is null ? offset : 0), ReadServerScopeParameter()))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                rows.Add(new ChangeWindowRow(
                    reader.GetInt64(0), reader.IsDBNull(1) ? null : reader.GetFieldValue<int[]>(1), reader.GetDateTime(2), reader.GetDateTime(3),
                    reader.GetString(4), reader.GetString(5), reader.GetDateTime(6),
                    reader.IsDBNull(7) ? null : reader.GetDateTime(7), reader.IsDBNull(8) ? null : reader.GetDateTime(8)));
            }
        }

        /* A login scoped to some servers sees a window that also names others, but not which: the list keeps only the ids in its scope, before
           the name lookup below, so another server's name is never read. A window with no list (every server) stays as it is. */
        if (ReadServerIds is { } scope)
        {
            var allowed = scope.ToHashSet();
            for (var i = 0; i < rows.Count; i++)
            {
                if (rows[i].ServerIds is { } listed)
                {
                    rows[i] = rows[i] with { ServerIds = listed.Where(allowed.Contains).ToArray() };
                }
            }
        }

        var windowsTruncated = windowId is null && rows.Count > limit;
        if (windowsTruncated)
        {
            rows.RemoveRange(limit, rows.Count - limit);
        }

        var counts = new Dictionary<long, (int Held, int Released, Dictionary<string, int> ByFamily, Dictionary<string, int> ByServer)>();
        if (rows.Count > 0)
        {
            await using var command = Command(connection, null, ChangeWindowCountsSql,
                new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = rows.Select(r => r.WindowId).ToArray() },
                ReadServerScopeParameter());
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var id = reader.GetInt64(0);
                var held = reader.GetInt32(3);
                var released = reader.GetInt32(4);
                if (!counts.TryGetValue(id, out var entry))
                {
                    entry = (0, 0, new Dictionary<string, int>(StringComparer.Ordinal), new Dictionary<string, int>(StringComparer.Ordinal));
                }

                var family = reader.GetString(1);
                var server = reader.GetString(2);
                entry.ByFamily[family] = entry.ByFamily.GetValueOrDefault(family) + held;
                entry.ByServer[server] = entry.ByServer.GetValueOrDefault(server) + held;
                counts[id] = (entry.Held + held, entry.Released + released, entry.ByFamily, entry.ByServer);
            }
        }

        var names = new Dictionary<int, string>();
        var wanted = rows.Where(r => r.ServerIds is not null).SelectMany(r => r.ServerIds!).Distinct().ToArray();
        if (wanted.Length > 0)
        {
            await using var command = Command(connection, null, ChangeWindowServerNamesSql,
                new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = wanted });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                names[reader.GetInt32(0)] = reader.GetString(1);
            }
        }

        var windows = rows.Select(r =>
        {
            counts.TryGetValue(r.WindowId, out var c);
            return new ChangeWindowSummary(
                r, InvestigationChangeWindows.StateOf(r.StartsAt, r.EndsAt, r.ClearedAt, now), InvestigationChangeWindows.TailEnd(r.EndsAt),
                r.ServerIds?.Select(id => names.TryGetValue(id, out var name) ? name : "server " + id.ToString(CultureInfo.InvariantCulture)).ToArray(),
                c.Held, c.Released, c.ByFamily ?? new Dictionary<string, int>(), c.ByServer ?? new Dictionary<string, int>());
        }).ToList();

        if (windowId is null)
        {
            return new ChangeWindowsRead(windows, null, windowsTruncated, windowsTruncated ? offset + limit : null);
        }

        var heldRequests = new List<ChangeWindowHeldRequest>();
        if (rows.Count > 0)
        {
            await using var command = Command(connection, null, ChangeWindowHeldSql,
                P(windowId.Value), P(now), P(limit + 1), P(offset), ReadServerScopeParameter());
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                heldRequests.Add(new ChangeWindowHeldRequest(
                    reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetDateTime(3), reader.GetString(4),
                    reader.IsDBNull(5) ? null : reader.GetDateTime(5)));
            }
        }

        var heldTruncated = heldRequests.Count > limit;
        if (heldTruncated)
        {
            heldRequests.RemoveRange(limit, heldRequests.Count - limit);
        }

        return new ChangeWindowsRead(windows, heldRequests, heldTruncated, heldTruncated ? offset + limit : null);
    }
}
