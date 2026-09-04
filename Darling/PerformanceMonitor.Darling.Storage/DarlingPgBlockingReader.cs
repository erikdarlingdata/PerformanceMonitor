/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads blocking chains from the stored edge list (<c>pg_blocking_edges</c>), assembled into one row per
/// captured chain with its root blocker attributed.
/// <para>This is where storing edges rather than a rendered tree pays for itself: the collector wrote
/// (blocked, blocking) pairs and knew nothing about chains, and the questions that actually matter — who is
/// at the root, how deep does it go, how many sessions are behind it, has this same backend been the root
/// all afternoon — are all answered here in SQL over those pairs.</para>
/// </summary>
public static class DarlingPgBlockingReader
{
    public sealed record PgBlockingChainRow(
        DateTime CapturedAt,
        long RootBackendId,
        int RootPid,
        string[] Databases,
        string? RootUsername,
        string? RootApplicationName,
        string? RootState,
        string? RootQuery,
        bool RootIsIdleInTransaction,
        long RootXactDurationMs,
        long RootQueryDurationMs,
        int TotalVictims,
        int DirectVictims,
        int MaxDepth,
        long WorstVictimWaitMs,
        string? WorstVictimQuery,
        /* NULL, not 0 or 1, when the root's own backend id did not resolve (the collector's
           vanished-blocker sentinel). Recurrence is genuinely UNKNOWN there, and "seen once" is a
           different claim from "cannot tell". */
        long? SamplesAsRoot,
        bool QueryTextMayBeTruncated,
        bool ChainMayBeTruncated);

    /// <summary>
    /// One row per (capture, root blocker), with the chain behind it measured and the root's own state
    /// attached.
    ///
    /// <para><b>Roots are found by absence.</b> A backend is a root when it blocks something and is not
    /// itself blocked in the same capture. That definition is why the collector had to store the whole edge
    /// set per capture rather than only the pairs someone asked about — a root cannot be recognised from one
    /// edge in isolation.</para>
    ///
    /// <para><b>The recursion is depth-capped at 32, and that guard is not decoration.</b> A cycle in the
    /// edge set would make an uncapped recursive CTE run until it exhausted memory. Cycles are rare but
    /// genuinely possible: PostgreSQL's deadlock detector resolves them, but only after
    /// <c>deadlock_timeout</c> (1s by default), so a capture can land inside that window and record a true
    /// cycle. No real chain approaches 32, so the cap costs nothing and removes the failure mode.</para>
    ///
    /// <para><b><c>samples_as_root</c> is keyed on the synthetic backend id, not the pid</b>, which is the
    /// whole reason that column exists. It answers "has this been the same stuck backend all along, or a
    /// succession of different ones that happened to reuse a pid" — and those two have different remedies.
    /// A pid-keyed count cannot tell them apart and would silently merge them on a busy instance.</para>
    ///
    /// <para><b>Ordered worst-first, not newest-first</b> (widest chain, then deepest, then most recent).
    /// The question this read serves is "what was the worst blocking in this window", and a newest-first
    /// ordering under a row limit would answer a different one — it would return the most recent captures
    /// and could omit the incident entirely.</para>
    ///
    /// <para><b><c>WITH RECURSIVE</c>, and the keyword goes on the FIRST CTE.</b> PostgreSQL scopes
    /// <c>RECURSIVE</c> to the whole <c>WITH</c> clause, not to the one CTE that needs it, so writing
    /// <c>WITH edges AS ... chain AS (... UNION ALL ... FROM chain ...)</c> fails outright with
    /// <c>relation "chain" does not exist</c> — a forward reference it will not resolve. It is a runtime
    /// error on the first call, not a compile-time one, which is why this was found by running the text
    /// against a real instance rather than by reading it.</para>
    ///
    /// <para>#2714: this is a complete SELECT statement (its own nested <c>WITH RECURSIVE</c>), factored out
    /// so <see cref="PgBlockingChainsSql"/> (raw severity-ordered top-N — the MCP tool and the Viewer grid
    /// both want to see an ongoing situation's repeated samples, not one row per root) and
    /// <see cref="PgBlockingChainsDedupedByRootSql"/> (the alert-only variant that dedupes by root BEFORE the
    /// row-count LIMIT) share ONE canonical copy of the CTE chain rather than two texts that can drift the
    /// way the ladder-generator's fresh-store/upgrade pair once did. Embedded elsewhere by wrapping it in one
    /// more set of parens as the body of an outer CTE.</para>
    /// </summary>
    private const string PgBlockingChainCandidatesSql = """
        WITH RECURSIVE edges AS (
            SELECT
                collection_id,
                collection_time,
                blocked_pid,
                blocking_pid,
                blocking_backend_id,
                blocked_query,
                blocked_query_duration_ms,
                blocking_username,
                blocking_application_name,
                blocking_state,
                blocking_query,
                blocking_is_idle_in_transaction,
                blocking_xact_duration_ms,
                blocking_query_duration_ms,
                database_name,
                query_text_may_be_truncated
            FROM pg_blocking_edges
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        ),
        roots AS (
            SELECT DISTINCT
                e.collection_id,
                e.collection_time,
                e.blocking_pid,
                e.blocking_backend_id
            FROM edges AS e
            WHERE NOT EXISTS (
                SELECT 1
                FROM edges AS upstream
                WHERE upstream.collection_id = e.collection_id
                AND   upstream.blocked_pid = e.blocking_pid
            )
        ),
        chain AS (
            SELECT
                r.collection_id,
                r.blocking_pid AS root_pid,
                e.blocked_pid,
                e.blocked_query,
                e.blocked_query_duration_ms,
                1 AS depth,
                ARRAY[r.blocking_pid, e.blocked_pid] AS visited
            FROM roots AS r
            JOIN edges AS e
              ON  e.collection_id = r.collection_id
              AND e.blocking_pid = r.blocking_pid

            UNION ALL

            SELECT
                c.collection_id,
                c.root_pid,
                e.blocked_pid,
                e.blocked_query,
                e.blocked_query_duration_ms,
                c.depth + 1,
                c.visited || e.blocked_pid
            FROM chain AS c
            JOIN edges AS e
              ON  e.collection_id = c.collection_id
              AND e.blocking_pid = c.blocked_pid
            WHERE c.depth < 32
            /* Never revisit a backend already on this walk. Without it a cycle hanging off an otherwise
               legitimate root is walked until the depth cap: root A blocks B while B/C/D form a cycle among
               themselves, B is correctly excluded from roots (it IS blocked) but A still qualifies, and the
               walk goes B -> C -> D -> B -> ... to 32. The cap stops the runaway but chain_stats then reports
               max_depth = 32 and a worst victim drawn from repeated revisits of the same three backends,
               which is indistinguishable from a genuine 32-deep chain. With the guard the counts are the
               DISTINCT set, and the cycle itself is reported by PgBlockingCyclesSql instead. */
            AND   e.blocked_pid <> ALL(c.visited)
        ),
        chain_stats AS (
            SELECT
                collection_id,
                root_pid,
                count(DISTINCT blocked_pid)::int AS total_victims,
                count(DISTINCT blocked_pid) FILTER (WHERE depth = 1)::int AS direct_victims,
                max(depth)::int AS max_depth,
                max(coalesce(blocked_query_duration_ms, -1)) AS worst_victim_wait_ms
            FROM chain
            GROUP BY collection_id, root_pid
        ),
        worst_victim AS (
            SELECT DISTINCT ON (collection_id, root_pid)
                collection_id,
                root_pid,
                blocked_query
            FROM chain
            ORDER BY collection_id, root_pid, coalesce(blocked_query_duration_ms, -1) DESC
        ),
        /* Split deliberately in two. DISTINCT ON picks ONE of the root's edges, which is correct only for
           columns that are constant per BACKEND — username, application, state, query, the durations all
           come from the same blocker row whichever edge is chosen. It is NOT correct for the two columns
           the collector computes per EDGE from both sides: database_name is
           coalesce(blocked.datname, blocker.datname) and query_text_may_be_truncated is an OR across both
           queries. Taking those from an arbitrary edge attributes a victim's truncated text, or a victim's
           database, to the root. They are aggregated over all the root's edges instead. */
        root_detail AS (
            SELECT DISTINCT ON (e.collection_id, e.blocking_pid)
                e.collection_id,
                e.collection_time,
                e.blocking_pid,
                e.blocking_backend_id,
                e.blocking_username,
                e.blocking_application_name,
                e.blocking_state,
                e.blocking_query,
                e.blocking_is_idle_in_transaction,
                e.blocking_xact_duration_ms,
                e.blocking_query_duration_ms
            FROM edges AS e
            JOIN roots AS r
              ON  r.collection_id = e.collection_id
              AND r.blocking_pid = e.blocking_pid
            ORDER BY e.collection_id, e.blocking_pid, e.blocked_pid
        ),
        root_edge_agg AS (
            SELECT
                e.collection_id,
                e.blocking_pid,
                array_agg(DISTINCT e.database_name) AS databases,
                bool_or(e.query_text_may_be_truncated) AS query_text_may_be_truncated
            FROM edges AS e
            JOIN roots AS r
              ON  r.collection_id = e.collection_id
              AND r.blocking_pid = e.blocking_pid
            GROUP BY e.collection_id, e.blocking_pid
        ),
        recurrence AS (
            SELECT
                blocking_backend_id,
                count(DISTINCT collection_id) AS samples_as_root
            FROM roots
            /* Exclude the vanished-blocker sentinel. The collector stores
               coalesce(blocker.backend_id, 0), so every root whose own row had already left
               pg_stat_activity lands on id 0 — and grouping those together counts unrelated one-off
               incidents in different captures as repeat appearances of one backend. That is precisely the
               conflation the synthetic backend id exists to prevent, arriving through the fallback instead
               of through pid reuse. Excluded rather than counted, so the final LEFT JOIN yields NULL and
               the read reports recurrence as UNKNOWN rather than inventing a number. */
            WHERE blocking_backend_id <> 0
            GROUP BY blocking_backend_id
        )
        /* Every output column is aliased, including the ones whose name looks obvious. The C# reader is
           positional so it does not care — but an unaliased coalesce() comes back named "coalesce", and
           three of them did, so a psql session debugging this query saw three identical column headings.
           A query this intricate has to be readable in the tool people will actually reach for. */
        SELECT
            d.collection_time                        AS captured_at,
            d.blocking_backend_id                    AS root_backend_id,
            d.blocking_pid                           AS root_pid,
            a.databases                              AS databases,
            d.blocking_username                      AS root_username,
            d.blocking_application_name              AS root_application_name,
            d.blocking_state                         AS root_state,
            d.blocking_query                         AS root_query,
            d.blocking_is_idle_in_transaction        AS root_is_idle_in_transaction,
            coalesce(d.blocking_xact_duration_ms, -1)  AS root_xact_duration_ms,
            coalesce(d.blocking_query_duration_ms, -1) AS root_query_duration_ms,
            s.total_victims                          AS total_victims,
            s.direct_victims                         AS direct_victims,
            s.max_depth                              AS max_depth,
            s.worst_victim_wait_ms                   AS worst_victim_wait_ms,
            v.blocked_query                          AS worst_victim_query,
            c.samples_as_root                        AS samples_as_root,
            a.query_text_may_be_truncated            AS query_text_may_be_truncated,
            /* #5: the depth cap must announce itself. With the revisit guard in place a max_depth of 32 is
               no longer a masked cycle — it means a genuinely 32-level walk that the cap stopped, so
               total_victims and the worst victim are computed over a TRUNCATED walk and read identically to
               a complete one. Implausible in practice; reported anyway, because this collector's premise is
               that a short answer must never pass for the whole picture. */
            (s.max_depth >= 32)                      AS chain_may_be_truncated
        FROM root_detail AS d
        JOIN chain_stats AS s
          ON  s.collection_id = d.collection_id
          AND s.root_pid = d.blocking_pid
        LEFT JOIN worst_victim AS v
          ON  v.collection_id = d.collection_id
          AND v.root_pid = d.blocking_pid
        JOIN root_edge_agg AS a
          ON  a.collection_id = d.collection_id
          AND a.blocking_pid = d.blocking_pid
        LEFT JOIN recurrence AS c
          ON  c.blocking_backend_id = d.blocking_backend_id
        """;

    /// <para>$1 server_id, $2/$3 window (naive UTC), $4 row limit.</para>
    public const string PgBlockingChainsSql = PgBlockingChainCandidatesSql +
        "\nORDER BY s.total_victims DESC, s.max_depth DESC, d.collection_time DESC\nLIMIT $4";

    /// <summary>
    /// #2714: the same candidate rows as <see cref="PgBlockingChainsSql"/>, but deduped to ONE row per
    /// distinct root — the same sentinel-aware identity <c>DarlingWorker.WorstPgBlockingChainPerRoot</c>
    /// already applies in C# (a vanished root's own pid, since its backend id fell back to the shared
    /// sentinel 0; the real backend id otherwise) — BEFORE the row-count LIMIT is applied, not after.
    ///
    /// <para><b>Why this needs to exist at all.</b> <see cref="PgBlockingChainsSql"/> orders candidate rows
    /// by severity (<c>total_victims DESC, max_depth DESC, collection_time DESC</c>) and applies LIMIT
    /// across the whole window BEFORE any caller dedupes by root. A single severe, persistent blocker
    /// sampled on most cycles of a rolling hour can occupy the entire LIMIT budget with repeat samples of
    /// itself, pushing every sample of a second, genuinely distinct — merely less severe — root out of the
    /// top N and out of the alert entirely. Raising the LIMIT only narrows the window this can happen in; it
    /// does not fix the "ordered by severity before dedup" shape. Deduping in SQL, before LIMIT, is the only
    /// way the row budget is actually spent on root diversity rather than repeat samples of one root.</para>
    ///
    /// <para>Kept as a query, not just a bigger raw LIMIT: dedupes by keeping each root's OWN worst sample
    /// (<c>DISTINCT ON</c> ordered the same worst-first way as <see cref="PgBlockingChainsSql"/>), so the
    /// alert's downstream per-root dedup in C# becomes a no-op safety net rather than the only thing standing
    /// between a real distinct root and an undercount.</para>
    ///
    /// <para>This is deliberately a SEPARATE query from <see cref="PgBlockingChainsSql"/> rather than a
    /// parameter that changes its shape — the MCP tool and the Viewer's blocking grid both call the raw
    /// severity-ordered form and want to see an ongoing situation's repeated samples across the window, not
    /// collapse them to one row per root. Changing the shared query's contract would have silently changed
    /// what those two already-shipped call sites display.</para>
    /// </summary>
    public const string PgBlockingChainsDedupedByRootSql =
        "WITH candidates AS (\n" +
        PgBlockingChainCandidatesSql +
        "\n),\n" +
        """
        keyed AS (
            SELECT
                candidates.*,
                CASE WHEN root_backend_id = 0
                     THEN 'pid:' || root_pid::text
                     ELSE 'bid:' || root_backend_id::text
                END AS root_key
            FROM candidates
        ),
        deduped AS (
            SELECT DISTINCT ON (root_key) *
            FROM keyed
            ORDER BY root_key, total_victims DESC, max_depth DESC, captured_at DESC
        )
        SELECT
            captured_at,
            root_backend_id,
            root_pid,
            databases,
            root_username,
            root_application_name,
            root_state,
            root_query,
            root_is_idle_in_transaction,
            root_xact_duration_ms,
            root_query_duration_ms,
            total_victims,
            direct_victims,
            max_depth,
            worst_victim_wait_ms,
            worst_victim_query,
            samples_as_root,
            query_text_may_be_truncated,
            chain_may_be_truncated
        FROM deduped
        ORDER BY total_victims DESC, max_depth DESC, captured_at DESC
        LIMIT $4
        """;

    public static Task<List<PgBlockingChainRow>> GetPgBlockingChainsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default) =>
        RunPgBlockingChainsQueryAsync(
            postgres, PgBlockingChainsSql, serverId, startUtc, endUtc, limit, cancellationToken);

    /// <summary>#2714: dedup-by-root-before-LIMIT variant — see <see cref="PgBlockingChainsDedupedByRootSql"/>
    /// for why this exists as a separate call rather than a flag on <see cref="GetPgBlockingChainsAsync"/>.
    /// </summary>
    public static Task<List<PgBlockingChainRow>> GetPgBlockingChainsDedupedByRootAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default) =>
        RunPgBlockingChainsQueryAsync(
            postgres, PgBlockingChainsDedupedByRootSql, serverId, startUtc, endUtc, limit, cancellationToken);

    private static async Task<List<PgBlockingChainRow>> RunPgBlockingChainsQueryAsync(
        NpgsqlDataSource postgres, string sql, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken)
    {
        var rows = new List<PgBlockingChainRow>();
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified), not the bare value. Npgsql does not reject Kind=Utc — it infers
           timestamptz, and PostgreSQL then zone-shifts the window against the store's NAIVE timestamp
           columns, so east of UTC the window silently slides off the data. Same convention as every
           other PostgreSQL read (DarlingPgXminReader, and the alert adapter's NaiveUtcNow). */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(MapChainRow(reader));
        }

        return rows;
    }

    /// <summary>
    /// Maps one chain row by ORDINAL, extracted so it can be tested against a fake reader.
    ///
    /// <para>Everything else about this read is pinned by asserting on the SQL TEXT, which cannot see the one
    /// defect that matters here: the projection's column order and this method's ordinals are two lists that
    /// must agree, and nothing makes them. Reordering <c>root_username</c> and <c>root_application_name</c> —
    /// both <c>string?</c>, adjacent, and semantically confusable — would silently transpose them and every
    /// text assertion would still pass. The same hazard on the probe/mapper pair got its own pin
    /// (<c>StoreSchemaProbe_ColumnCount_MatchesTheMapArity</c>) and on the collector side too
    /// (<c>WritesEveryDeclaredPayloadColumn</c>); the reader side had none, which review caught.</para>
    ///
    /// <para>The projection was edited three times while this PR was open — <c>databases</c> replaced a
    /// scalar, <c>samples_as_root</c> became nullable, <c>chain_may_be_truncated</c> was appended — so the
    /// risk was live rather than hypothetical.</para>
    /// </summary>
    internal static PgBlockingChainRow MapChainRow(DbDataReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);

        return new PgBlockingChainRow(
            reader.GetDateTime(0),
            reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
            reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
            reader.IsDBNull(3) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetString(7),
            !reader.IsDBNull(8) && reader.GetBoolean(8),
            reader.IsDBNull(9) ? -1 : reader.GetInt64(9),
            reader.IsDBNull(10) ? -1 : reader.GetInt64(10),
            reader.IsDBNull(11) ? 0 : reader.GetInt32(11),
            reader.IsDBNull(12) ? 0 : reader.GetInt32(12),
            reader.IsDBNull(13) ? 0 : reader.GetInt32(13),
            reader.IsDBNull(14) ? -1 : reader.GetInt64(14),
            reader.IsDBNull(15) ? null : reader.GetString(15),
            reader.IsDBNull(16) ? null : reader.GetInt64(16),
            !reader.IsDBNull(17) && reader.GetBoolean(17),
            !reader.IsDBNull(18) && reader.GetBoolean(18));
    }

    public sealed record PgBlockingCycleRow(
        DateTime CapturedAt,
        int ParticipantCount,
        int[] Pids,
        string? DatabaseName,
        string? ApplicationName,
        int BlockedBehindCount,
        int[] BlockedBehindPids);

    /// <summary>
    /// Backends caught in a lock CYCLE — each one reachable from itself through the edge list.
    ///
    /// <para><b>This exists because the chain read cannot report them, and finding that out was the point of
    /// probing it.</b> <c>chains</c> identifies a root by absence: a backend that blocks something and is not
    /// itself blocked. In a cycle every participant is blocked, so there is no root, so the entire cyclic
    /// component is silently dropped — 0 rows from a capture that recorded real blocking. For a collector
    /// whose whole design is about never letting an empty answer mean "nothing happened", that was the one
    /// place the read did exactly that.</para>
    ///
    /// <para>Rare but genuinely reachable: PostgreSQL's deadlock detector resolves cycles, but only after
    /// <c>deadlock_timeout</c> (1s by default), and a capture can land inside that window. When it does, this
    /// is the only evidence that will ever exist — the edges are stored, and the engine kills one of the
    /// participants a moment later.</para>
    ///
    /// <para>Detected by reachability rather than by "the collection has no root", which would miss a cycle
    /// sharing a capture with an ordinary chain. Recursion stops as soon as a walk returns to where it
    /// started (<c>at_pid &lt;&gt; start_pid</c>), refuses to wander into a foreign cycle, and is
    /// depth-capped besides.</para>
    ///
    /// <para><b>One row per CYCLE, not per capture, and the attributed names come from the cycle's own
    /// edges.</b> Both of those were wrong first time and both failed the same way — silently, with a
    /// plausible number. Grouping on <c>collection_id</c> alone merged two independent deadlocks that landed
    /// in one sample into a single bogus component; joining the edge rows on <c>collection_id</c> alone
    /// aggregated <c>database_name</c> over every edge in the capture, so a cycle sharing a sample with an
    /// unrelated chain reported whichever database sorted first. Each walk's <c>members</c> array is carried
    /// specifically so the component can be canonicalised (sorted, then DISTINCT collapses the rotations one
    /// per participant) and used to scope the join.</para>
    ///
    /// <para>$1 server_id, $2/$3 window (naive UTC), $4 row limit.</para>
    /// </summary>
    public const string PgBlockingCyclesSql = """
        WITH RECURSIVE edges AS (
            SELECT
                collection_id,
                collection_time,
                blocked_pid,
                blocking_pid,
                database_name,
                blocked_application_name
            FROM pg_blocking_edges
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        ),
        walk AS (
            SELECT
                collection_id,
                blocked_pid AS start_pid,
                blocking_pid AS at_pid,
                1 AS depth,
                ARRAY[blocked_pid] AS members
            FROM edges

            UNION ALL

            SELECT
                w.collection_id,
                w.start_pid,
                e.blocking_pid,
                w.depth + 1,
                w.members || e.blocked_pid
            FROM walk AS w
            JOIN edges AS e
              ON  e.collection_id = w.collection_id
              AND e.blocked_pid = w.at_pid
            WHERE w.depth < 32
            /* Stop the moment the walk closes on where it started — that IS the detection. */
            AND   w.at_pid <> w.start_pid
            /* And never wander into a FOREIGN cycle: without this, a walk that starts outside a cycle and
               reaches one loops inside it to the depth cap, doing 32 rounds of work per starting edge. */
            AND   (e.blocking_pid = w.start_pid OR e.blocking_pid <> ALL(w.members))
        ),
        closed AS (
            /* A walk that returned to its own start. Its `members` array is exactly that cycle's
               participants, which is why the array is carried at all. */
            SELECT collection_id, members
            FROM walk
            WHERE at_pid = start_pid
        ),
        components AS (
            /* Canonicalise: every participant of one cycle produces its own closed walk with the same
               member SET in a rotated order, so sorting collapses them to one row per actual cycle.
               DISTINCT then dedupes the rotations.

               Grouping by the component rather than by the capture is load-bearing: two independent
               deadlocks landing in the same one-minute capture are two findings, and grouping on
               collection_id alone merged their pids into one bogus connected component. */
            SELECT DISTINCT
                collection_id,
                (SELECT array_agg(m ORDER BY m) FROM unnest(members) AS m) AS members
            FROM closed
        ),
        behind AS (
            /* Backends stuck BEHIND the cycle: blocked by a member, transitively, without being a member.
               These were invisible to BOTH reads, which is the one outcome this collector's design forbids.
               chains cannot see them — every cycle member is itself blocked, so no member qualifies as a
               root and no root walk ever reaches their edges. And this query's own walk cannot see them
               either: a walk starting at such an edge extends into the cycle but is barred from closing on
               its own start, so it never lands in `closed`. A real, captured blocking relationship therefore
               appeared nowhere at all. Reported here, attached to the cycle that is causing it, because
               "this deadlock also has nine sessions queued behind it" is the part that decides urgency. */
            SELECT
                c.collection_id,
                c.members,
                e.blocked_pid,
                1 AS depth,
                ARRAY[e.blocked_pid] AS seen
            FROM components AS c
            JOIN edges AS e
              ON  e.collection_id = c.collection_id
              AND e.blocking_pid = ANY(c.members)
            WHERE e.blocked_pid <> ALL(c.members)

            UNION ALL

            SELECT
                b.collection_id,
                b.members,
                e.blocked_pid,
                b.depth + 1,
                b.seen || e.blocked_pid
            FROM behind AS b
            JOIN edges AS e
              ON  e.collection_id = b.collection_id
              AND e.blocking_pid = b.blocked_pid
            WHERE b.depth < 32
            AND   e.blocked_pid <> ALL(b.members)
            AND   e.blocked_pid <> ALL(b.seen)
        ),
        behind_stats AS (
            SELECT
                collection_id,
                members,
                count(DISTINCT blocked_pid)::int AS blocked_behind_count,
                array_agg(DISTINCT blocked_pid) AS blocked_behind_pids
            FROM behind
            GROUP BY collection_id, members
        )
        SELECT
            e.collection_time                        AS captured_at,
            cardinality(c.members)                   AS participant_count,
            c.members                                AS pids,
            min(e.database_name)                     AS database_name,
            min(e.blocked_application_name)          AS application_name,
            coalesce(max(b.blocked_behind_count), 0) AS blocked_behind_count,
            coalesce(max(b.blocked_behind_pids), ARRAY[]::int[]) AS blocked_behind_pids
        FROM components AS c
        LEFT JOIN behind_stats AS b
          ON  b.collection_id = c.collection_id
          AND b.members = c.members
        JOIN edges AS e
          ON  e.collection_id = c.collection_id
          /* Scoped to the cycle's OWN participants. Joining on collection_id alone aggregated
             database_name and application_name over every edge in the capture, so a cycle in one database
             sharing a sample with an ordinary chain in another reported whichever name sorted first —
             pointing the reader at a database the deadlock never touched. */
          AND e.blocked_pid = ANY(c.members)
        GROUP BY e.collection_id, e.collection_time, c.members
        ORDER BY e.collection_time DESC
        LIMIT $4
        """;

    public static async Task<List<PgBlockingCycleRow>> GetPgBlockingCyclesAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgBlockingCycleRow>();
        await using var command = postgres.CreateCommand(PgBlockingCyclesSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified), not the bare value. Npgsql does not reject Kind=Utc — it infers
           timestamptz, and PostgreSQL then zone-shifts the window against the store's NAIVE timestamp
           columns, so east of UTC the window silently slides off the data. Same convention as every
           other PostgreSQL read (DarlingPgXminReader, and the alert adapter's NaiveUtcNow). */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(MapCycleRow(reader));
        }

        return rows;
    }

    /// <summary>Maps one cycle row by ORDINAL — same seam, same reason, see <see cref="MapChainRow"/>.</summary>
    internal static PgBlockingCycleRow MapCycleRow(DbDataReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);

        return new PgBlockingCycleRow(
            reader.GetDateTime(0),
            reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
            reader.IsDBNull(2) ? Array.Empty<int>() : reader.GetFieldValue<int[]>(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
            reader.IsDBNull(6) ? Array.Empty<int>() : reader.GetFieldValue<int[]>(6));
    }

    /// <summary>
    /// How many captures in the window recorded any blocking at all, and how many recorded none.
    /// <para>Reported alongside the chains because the denominator is the honest part of a sampled signal.
    /// "Three chains" means something different in a window of 60 captures than in a window of 4, and the
    /// stored table cannot say which on its own — an absent capture and a capture that found nothing look
    /// identical in a table that only holds edges. The blocking-free count comes from
    /// <c>collection_log</c>, which records a SUCCESS with zero rows, so the two really are
    /// distinguishable — but only by looking there.</para>
    /// <para>$1 server_id, $2/$3 window (naive UTC).</para>
    /// </summary>
    public const string PgBlockingCaptureCountsSql = """
        SELECT
            count(*) FILTER (WHERE l.rows_collected > 0),
            count(*),
            min(l.collection_time),
            max(l.collection_time)
        FROM collection_log AS l
        WHERE l.server_id = $1
        AND   l.collector_name = 'pg_blocking'
        AND   l.status = 'SUCCESS'
        AND   l.collection_time >= $2
        AND   l.collection_time <= $3
        """;

    public sealed record PgBlockingCaptureCounts(
        long CapturesWithBlocking,
        long CapturesTotal,
        DateTime? FirstCaptureAt,
        DateTime? LastCaptureAt);

    public static async Task<PgBlockingCaptureCounts> GetPgBlockingCaptureCountsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(PgBlockingCaptureCountsSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified), not the bare value. Npgsql does not reject Kind=Utc — it infers
           timestamptz, and PostgreSQL then zone-shifts the window against the store's NAIVE timestamp
           columns, so east of UTC the window silently slides off the data. Same convention as every
           other PostgreSQL read (DarlingPgXminReader, and the alert adapter's NaiveUtcNow). */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new PgBlockingCaptureCounts(
                reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                reader.IsDBNull(3) ? null : reader.GetDateTime(3));
        }

        return new PgBlockingCaptureCounts(0, 0, null, null);
    }
}
