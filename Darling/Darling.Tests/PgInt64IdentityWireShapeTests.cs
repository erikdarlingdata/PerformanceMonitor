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
using System.Text.Json;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2548: the two PostgreSQL int8 IDENTITIES that reach JSON must reach it as STRINGS.
///
/// <para><b>The defect.</b> <c>get_pg_top_queries</c> serialized <c>queryid</c> and <c>get_pg_blocking</c>
/// serialized <c>root_backend_id</c> as JSON numbers. Both are signed <c>int8</c> whose real values sit past
/// 2^53, and every parser that decodes JSON numbers as IEEE-754 doubles — <c>JSON.parse</c>, <c>json.loads</c>,
/// most agent tooling — rounds one silently. The value on the wire was never wrong; it was unrecoverable after
/// parsing, which for an identity is the same thing.</para>
///
/// <para><b>Why an identity is a different case from a metric.</b> A metric that comes back 27 too high is
/// still approximately true. An identity exists only to be joined on, and every use of these two —
/// <c>SELECT … FROM pg_stat_statements WHERE queryid = …</c>, matching a row on our screen against one on the
/// operator's instance, comparing a root blocker across two captures — is an equality test. A rounded key
/// matches nothing, so the field silently stops doing the one thing it is for.</para>
///
/// <para><b>root_backend_id is the worse of the two, which is not obvious.</b> The collector builds it by
/// CONCATENATING the backend's start epoch with its zero-padded pid, so every value is a 17-digit integer
/// around 1.79e16 — roughly 2x past 2^53, in the range where adjacent doubles are 2 apart. Half of all backend
/// ids are therefore odd and have no representation at all, and an unrepresentable one does not round to
/// nothing: it rounds onto its even NEIGHBOUR, which is a DIFFERENT backend. queryid loses the join;
/// root_backend_id makes the wrong one.</para>
///
/// <para><b>What these tests assert, and what they deliberately do not.</b> The fix is scoped to those two
/// fields, so <c>database_id</c> (a PostgreSQL <c>oid</c>: unsigned 32-bit, structurally unable to reach the
/// rounding range) and <c>root_pid</c> (an <c>int</c>) are pinned as NUMBERS. A guard that only demanded
/// strings would be satisfied by turning the whole payload into text, which is a different and worse change.
/// </para>
///
/// <para>Every assertion runs against the SHIPPED serializers rather than a re-implementation of the
/// projection, which is what keeps the guard from passing while the code drifts underneath it.</para>
/// </summary>
public sealed class PgInt64IdentityWireShapeTests
{
    /* The value quoted on #2548, plus a positive sibling and long.MaxValue. pg_stat_statements derives
       queryid from a hash of the post-parse-analysis tree, so real ids are spread over the whole signed
       64-bit range and the great majority of them look like these. */
    private static readonly long[] OutOfRangeQueryIds =
    {
        -4185925123159566327L,
        8993582471124583231L,
        long.MaxValue,
    };

    /* Controls. A guard whose fixture happens to fit in a double proves nothing about a fixture that does
       not, so the premise is asserted rather than assumed — and long.MinValue belongs HERE, not above: it is
       exactly -2^63, a power of two, and therefore one of the rare large longs a double holds exactly. */
    private static readonly long[] InRangeQueryIds = { 4185925L, long.MinValue };

    /// <summary>
    /// What <c>JSON.parse</c> leaves a caller holding: every JSON number goes through an IEEE-754 double,
    /// and a string does not.
    /// <para><c>"F0"</c> rather than a <c>(long)</c> cast or a <c>decimal</c> conversion — the cast SATURATES
    /// at <c>long.MaxValue</c> and the decimal conversion keeps only 15 significant digits, so both would
    /// misreport the loss on exactly the values furthest out of range.</para>
    /// </summary>
    private static string DecodeLikeJsonParse(JsonElement element) =>
        element.ValueKind switch
        {
            JsonValueKind.String => element.GetString()!,
            JsonValueKind.Number => element.GetDouble().ToString("F0", CultureInfo.InvariantCulture),
            _ => "<" + element.ValueKind + ">",
        };

    /// <summary>Whether a long survives a round trip through an IEEE-754 double unchanged.</summary>
    private static bool SurvivesDouble(long value) =>
        string.Equals(
            ((double)value).ToString("F0", CultureInfo.InvariantCulture),
            value.ToString(CultureInfo.InvariantCulture),
            StringComparison.Ordinal);

    private static DarlingPgStatementReader.PgStatementRow StatementRow(long queryId) =>
        new(
            QueryId: queryId,
            DatabaseId: 16384,
            Calls: 100,
            TotalExecTimeMs: 5000,
            RowsReturned: 250,
            MaxExecTimeMs: 91.5,
            SharedBlocksHit: 10,
            SharedBlocksRead: 5,
            StorageBlocksRead: 3,
            OrcacheBlocksHit: 2,
            TempBlocksRead: 0,
            TempBlocksWritten: 0,
            WalBytes: 4096,
            MaxPeakMemBytes: 2097152,
            QueryText: "SELECT * FROM public.widget WHERE id = $1");

    private static JsonElement TopQueries(IEnumerable<long> queryIds, out JsonDocument document)
    {
        var rows = queryIds.Select(StatementRow).ToList();
        document = JsonDocument.Parse(
            DarlingMcpPgStatementTools.BuildTopQueriesJson("pg-target-01", 24, rows, 20));
        return document.RootElement.GetProperty("queries");
    }

    /// <summary>
    /// The premise. If these ids fit in a double, every other assertion in this class is vacuous, so the
    /// fixture's adversarial-ness is a claim to prove rather than a comment to write.
    /// </summary>
    [Fact]
    public void TheQueryIdFixtureIsGenuinelyOutOfDoubleRange()
    {
        Assert.All(OutOfRangeQueryIds, id => Assert.False(SurvivesDouble(id), $"{id} should NOT survive"));
        Assert.All(InRangeQueryIds, id => Assert.True(SurvivesDouble(id), $"{id} SHOULD survive"));
    }

    [Fact]
    public void QueryIdIsAStringOnTheWire()
    {
        var queries = TopQueries(OutOfRangeQueryIds, out var document);
        using (document)
        {
            for (var i = 0; i < OutOfRangeQueryIds.Length; i++)
            {
                var element = queries[i].GetProperty("queryid");
                Assert.Equal(JsonValueKind.String, element.ValueKind);
                Assert.Equal(
                    OutOfRangeQueryIds[i].ToString(CultureInfo.InvariantCulture),
                    element.GetString());
            }
        }
    }

    /// <summary>
    /// The regression this class exists for: <c>queryid</c> back as a number would still LOOK right in the
    /// serialized text and would still fail here, because the check is what a double-decoding parser ends up
    /// holding rather than what the bytes say.
    /// </summary>
    [Fact]
    public void QueryIdSurvivesADoubleDecodingParse()
    {
        var ids = OutOfRangeQueryIds.Concat(InRangeQueryIds).ToArray();
        var queries = TopQueries(ids, out var document);
        using (document)
        {
            for (var i = 0; i < ids.Length; i++)
            {
                Assert.Equal(
                    ids[i].ToString(CultureInfo.InvariantCulture),
                    DecodeLikeJsonParse(queries[i].GetProperty("queryid")));
            }
        }
    }

    /// <summary>
    /// Scope. database_id is a PostgreSQL oid — unsigned 32-bit, so it cannot reach the rounding range — and
    /// stays a number. Without this, "turn the whole payload into strings" would pass every other test here.
    /// </summary>
    [Fact]
    public void DatabaseIdStaysANumber()
    {
        var queries = TopQueries(OutOfRangeQueryIds, out var document);
        using (document)
        {
            Assert.Equal(JsonValueKind.Number, queries[0].GetProperty("database_id").ValueKind);
        }
    }

    /* Shaped exactly the way the collector builds backend_id: a plausible backend_start epoch in seconds
       concatenated with a 7-digit zero-padded pid. Four ADJACENT pids, deliberately, because the failure
       being guarded is one backend's id decoding onto another's — which a fixture of one row cannot see and
       a fixture of well-separated ids would not provoke. */
    private static long[] AdjacentBackendIds()
    {
        const long epochSeconds = 1787000000L;
        return Enumerable.Range(1000, 4)
            .Select(pid => long.Parse(
                epochSeconds.ToString(CultureInfo.InvariantCulture)
                    + pid.ToString("D7", CultureInfo.InvariantCulture),
                CultureInfo.InvariantCulture))
            .ToArray();
    }

    private static DarlingPgBlockingReader.PgBlockingChainRow ChainRow(long backendId) =>
        new(
            CapturedAt: new DateTime(2026, 8, 22, 12, 0, 0, DateTimeKind.Unspecified),
            RootBackendId: backendId,
            RootPid: (int)(backendId % 10000000L),
            Databases: new[] { "appdb" },
            RootUsername: "app_user",
            RootApplicationName: "orders-service",
            RootState: "idle in transaction",
            RootQuery: "UPDATE public.widget SET qty = $1 WHERE id = $2",
            RootIsIdleInTransaction: true,
            RootXactDurationMs: 42000,
            RootQueryDurationMs: 12,
            TotalVictims: 7,
            DirectVictims: 3,
            MaxDepth: 2,
            WorstVictimWaitMs: 30000,
            WorstVictimQuery: "SELECT qty FROM public.widget WHERE id = $1 FOR UPDATE",
            SamplesAsRoot: 4,
            QueryTextMayBeTruncated: false,
            ChainMayBeTruncated: false);

    /* One real cycle row, not an empty list. Splitting BuildCycleEntries out made cycles travel as
       List<object>, and System.Text.Json serializes an object-typed element by its RUNTIME type -- but
       "it does" is a thing to CHECK at a new seam, not to remember. An empty list would have proved
       nothing while the payload quietly became a list of "{}". */
    private static List<object> CycleEntries() =>
        DarlingMcpPgBlockingTools.BuildCycleEntries(
            new List<DarlingPgBlockingReader.PgBlockingCycleRow>
            {
                new(
                    CapturedAt: new DateTime(2026, 8, 22, 12, 0, 0, DateTimeKind.Unspecified),
                    ParticipantCount: 2,
                    Pids: new[] { 1001, 1002 },
                    DatabaseName: "appdb",
                    ApplicationName: "orders-service",
                    BlockedBehindCount: 3,
                    BlockedBehindPids: new[] { 1003, 1004, 1005 }),
            });

    private static JsonElement BlockingChains(IEnumerable<long> backendIds, out JsonDocument document)
    {
        var chains = backendIds.Select(ChainRow).ToList();
        var captures = new DarlingPgBlockingReader.PgBlockingCaptureCounts(
            CapturesWithBlocking: 5,
            CapturesTotal: 60,
            FirstCaptureAt: new DateTime(2026, 8, 22, 11, 0, 0, DateTimeKind.Unspecified),
            LastCaptureAt: new DateTime(2026, 8, 22, 12, 0, 0, DateTimeKind.Unspecified));

        document = JsonDocument.Parse(DarlingMcpPgBlockingTools.BuildBlockingChainsJson(
            "pg-target-01", 24, chains, CycleEntries(), captures));
        return document.RootElement.GetProperty("chains");
    }

    /// <summary>
    /// The premise, stated precisely rather than conveniently. "Every backend_id is corrupted" is the easy
    /// claim and it is FALSE: at ~1.79e16 the even ones are exact and only the odd ones are unrepresentable.
    /// The true statement is the worse one, and it is the next test.
    /// </summary>
    [Fact]
    public void OddBackendIdsHaveNoDoubleRepresentationAndEvenOnesDo()
    {
        foreach (var id in AdjacentBackendIds())
        {
            Assert.Equal(id % 2 == 0, SurvivesDouble(id));
        }
    }

    [Fact]
    public void RootBackendIdIsAStringOnTheWire()
    {
        var ids = AdjacentBackendIds();
        var chains = BlockingChains(ids, out var document);
        using (document)
        {
            for (var i = 0; i < ids.Length; i++)
            {
                var element = chains[i].GetProperty("root_backend_id");
                Assert.Equal(JsonValueKind.String, element.ValueKind);
                Assert.Equal(ids[i].ToString(CultureInfo.InvariantCulture), element.GetString());
            }
        }
    }

    /// <summary>
    /// The failure a JSON number cannot avoid here and a string cannot cause: four DISTINCT backends
    /// decoding to fewer than four distinct ids. This is the assertion that says why root_backend_id was in
    /// scope rather than merely noted — a rounded key that resolves to the WRONG backend is worse than one
    /// that resolves to none.
    /// </summary>
    [Fact]
    public void DistinctBackendsStayDistinctAfterADoubleDecodingParse()
    {
        var ids = AdjacentBackendIds();
        var chains = BlockingChains(ids, out var document);
        using (document)
        {
            var decoded = Enumerable.Range(0, ids.Length)
                .Select(i => DecodeLikeJsonParse(chains[i].GetProperty("root_backend_id")))
                .ToArray();

            Assert.Equal(
                ids.Select(id => id.ToString(CultureInfo.InvariantCulture)).ToArray(),
                decoded);
            Assert.Equal(ids.Length, new HashSet<string>(decoded, StringComparer.Ordinal).Count);
        }
    }

    /// <summary>
    /// The seam this change introduced, guarded rather than assumed: cycle entries now travel as
    /// <c>List&lt;object&gt;</c>, and if System.Text.Json ever serialized them by their DECLARED type the
    /// payload would become a list of <c>{}</c> -- valid JSON, right array length, no content.
    /// </summary>
    [Fact]
    public void CycleEntriesKeepTheirMembersThroughTheObjectBoxing()
    {
        BlockingChains(AdjacentBackendIds(), out var document);
        using (document)
        {
            var cycle = document.RootElement.GetProperty("cycles")[0];
            Assert.Equal(2, cycle.GetProperty("participant_count").GetInt32());
            Assert.Equal(3, cycle.GetProperty("blocked_behind_count").GetInt32());
            Assert.Equal("appdb", cycle.GetProperty("database").GetString());
        }
    }

    /// <summary>Scope, the blocking half: root_pid is an int32 and cannot round, so it stays a number.</summary>
    [Fact]
    public void RootPidStaysANumber()
    {
        var chains = BlockingChains(AdjacentBackendIds(), out var document);
        using (document)
        {
            Assert.Equal(JsonValueKind.Number, chains[0].GetProperty("root_pid").ValueKind);
        }
    }
}
