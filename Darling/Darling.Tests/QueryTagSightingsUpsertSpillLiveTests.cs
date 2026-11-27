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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// #9038: the sightings upsert spilled to temporary files on every large flush, because the multi-argument <c>unnest</c> held every
/// text sample in a tuple store. The statement now keeps the text samples out of every work area. The old statement is kept here as
/// the reference: both run the same flushes on two stores and must leave the same rows and the
/// new one must write no temporary blocks at 4MB of <c>work_mem</c>, the PostgreSQL default.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It reaches DARLING_TEST_PG only to create and drop its own databases. */
public sealed class QueryTagSightingsUpsertSpillLiveTests
{
    private const int Server = 7;

    private const string ReferenceUpsertSql = """
INSERT INTO collect.query_tag_sightings AS t
    (server_id, key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample, seen_count)
SELECT u.server_id, u.key_kind, u.key, u.database_name, u.object_name, u.application_name, u.login_name, u.host_name, u.text_sample, u.seen_count
FROM unnest($1::integer[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $9::text[], $10::bigint[])
    WITH ORDINALITY AS u(server_id, key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample, seen_count, ord)
WHERE NOT EXISTS (
    SELECT 1
    FROM collect.query_tag_sightings AS s
    WHERE s.server_id = u.server_id AND s.key_kind = u.key_kind AND s.key = u.key
      AND COALESCE(s.application_name, '') = COALESCE(u.application_name, '')
      AND COALESCE(s.login_name, '') = COALESCE(u.login_name, '')
      AND COALESCE(s.host_name, '') = COALESCE(u.host_name, '')
      AND COALESCE(s.database_name, '') = COALESCE(u.database_name, '')
      AND NOT ((s.object_name IS NULL AND u.object_name IS NOT NULL)
            OR (s.text_sample IS NULL AND u.text_sample IS NOT NULL)
            OR s.last_seen < now() - $11::interval - $12::interval))
ORDER BY u.ord
ON CONFLICT (server_id, key_kind, key, COALESCE(application_name, ''), COALESCE(login_name, ''), COALESCE(host_name, ''), COALESCE(database_name, ''))
DO UPDATE SET
    last_seen = CASE
        WHEN (t.object_name IS NULL AND EXCLUDED.object_name IS NOT NULL)
          OR (t.text_sample IS NULL AND EXCLUDED.text_sample IS NOT NULL)
        THEN EXCLUDED.last_seen
        WHEN t.last_seen < EXCLUDED.last_seen - $11::interval - $12::interval
        THEN EXCLUDED.last_seen - $11::interval
        ELSE t.last_seen END,
    seen_count = t.seen_count + EXCLUDED.seen_count,
    object_name = COALESCE(t.object_name, EXCLUDED.object_name),
    text_sample = COALESCE(t.text_sample, EXCLUDED.text_sample)
WHERE (t.object_name IS NULL AND EXCLUDED.object_name IS NOT NULL)
   OR (t.text_sample IS NULL AND EXCLUDED.text_sample IS NOT NULL)
   OR t.last_seen < EXCLUDED.last_seen - $11::interval - $12::interval
""";

    private static string KeyOf(int i) => "0x" + i.ToString("X8", CultureInfo.InvariantCulture);

    private static string SampleOf(int i, int length)
    {
        var sb = new StringBuilder(length + 40);
        while (sb.Length < length)
        {
            sb.Append(i.ToString(CultureInfo.InvariantCulture)).Append(" select * from t where c = ").Append(i).Append(' ');
        }

        return sb.ToString(0, length);
    }

    /// <summary>
    /// A flush shaped like a busy one. Keys 1..<paramref name="existing"/> are re-sightings and the rest are new. In stage 1 a third of
    /// the keys lack an object name and half lack text. In stage 2 some of those carry a value (a fill) and some never do. Some keys
    /// have a missing or empty caller field, which the unique index reads as one identity.
    /// </summary>
    private static List<QueryTagSighting> Flush(int existing, int added, int stage, int textLength)
    {
        var list = new List<QueryTagSighting>();
        for (int i = 1; i <= existing + added; i++)
        {
            bool isNew = i > existing;
            bool hasObject = isNew ? i % 5 != 0 : stage == 1 ? i % 3 != 0 : i % 3 != 0 || i % 6 == 3;
            bool hasText = isNew ? i % 2 == 0 : stage == 1 ? i % 2 == 0 : i % 4 != 3;
            list.Add(new QueryTagSighting(
                i % 10 < 7 ? QueryTagKeyKind.Hash : i % 10 < 9 ? QueryTagKeyKind.Object : QueryTagKeyKind.Caller,
                KeyOf(i),
                i % 7 == 0 ? null : "Db" + (i % 20),
                hasObject ? new QueryTagObject("Db" + (i % 20), "dbo", "Proc" + i) : null,
                i % 11 == 0 ? null : i % 13 == 0 ? string.Empty : "App" + (i % 50),
                "login" + (i % 30),
                i % 17 == 0 ? null : "HOST" + (i % 100),
                hasText ? SampleOf(i, textLength) : null));
        }

        return list;
    }

    private static List<QueryTagSightingWriter.CollapsedSighting> Rows(List<QueryTagSighting> flush)
    {
        var rows = QueryTagSightingWriter.Collapse(Server, flush);
        QueryTagSightingWriter.SortByIdentity(rows);
        return rows;
    }

    private static NpgsqlCommand Command(NpgsqlConnection connection, string sql, List<QueryTagSightingWriter.CollapsedSighting> rows)
    {
        static NpgsqlParameter TextArray(IEnumerable<string?> values) =>
            new() { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = values.ToArray() };

        var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = rows.Select(r => r.ServerId).ToArray() });
        command.Parameters.Add(TextArray(rows.Select(r => (string?)r.Kind)));
        command.Parameters.Add(TextArray(rows.Select(r => (string?)r.Key)));
        command.Parameters.Add(TextArray(rows.Select(r => r.Database)));
        command.Parameters.Add(TextArray(rows.Select(r => r.ObjectName)));
        command.Parameters.Add(TextArray(rows.Select(r => r.Application)));
        command.Parameters.Add(TextArray(rows.Select(r => r.Login)));
        command.Parameters.Add(TextArray(rows.Select(r => r.Host)));
        command.Parameters.Add(TextArray(rows.Select(r => r.TextSample)));
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = rows.Select(r => r.Count).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = QueryTagSightingWriter.ReSightingBackdate });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = QueryTagSightingWriter.ReSightingRefreshWidth });
        return command;
    }

    private static async Task RunAsync(NpgsqlConnection connection, string sql, List<QueryTagSightingWriter.CollapsedSighting> rows, CancellationToken ct)
    {
        await using var command = Command(connection, sql, rows);
        await command.ExecuteNonQueryAsync(ct);
    }

    private const string IdentityOrder =
        "key_kind COLLATE \"C\", key COLLATE \"C\", coalesce(application_name, '') COLLATE \"C\", coalesce(login_name, '') COLLATE \"C\", coalesce(host_name, '') COLLATE \"C\", coalesce(database_name, '') COLLATE \"C\"";

    private static async Task<List<string>> QueryStringsAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var list = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            list.Add(reader.GetString(0));
        }

        return list;
    }

    /// <summary>Every column the upsert owns, with last_seen as the kind of move it made (the clocks of two stores differ), in identity order.</summary>
    private static Task<List<string>> SnapshotAsync(NpgsqlConnection connection, CancellationToken ct) =>
        QueryStringsAsync(
            connection,
            $"""
SELECT concat_ws('|', server_id, key_kind, key, coalesce(database_name, '<null>'), coalesce(object_name, '<null>'), coalesce(application_name, '<null>'),
       coalesce(login_name, '<null>'), coalesce(host_name, '<null>'), coalesce(md5(text_sample), '<null>'), seen_count,
       CASE WHEN last_seen = first_seen THEN 'insert'
            WHEN last_seen > now() - interval '1 minute' THEN 'now'
            WHEN last_seen > now() - interval '8 minutes' THEN 'backdated'
            ELSE 'old' END)
FROM collect.query_tag_sightings ORDER BY {IdentityOrder}
""",
            ct);

    [Fact]
    public async Task TheNewStatement_LeavesTheSameRows_AsTheReference_ForNewUnchangedFillAndDueRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        var snapshots = new List<List<string>>();
        foreach (var sql in new[] { ReferenceUpsertSql, QueryTagSightingWriter.UpsertSql })
        {
            await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
            await using var store = await OpenStoreAsync(scratch, ct);

            /* Flush 1 stores 600 keys. */
            await RunAsync(store, sql, Rows(Flush(600, 0, 1, 300)), ct);
            Assert.Equal(600, await CountAsync(store, "SELECT count(*) FROM collect.query_tag_sightings", ct));

            /* Flush 2 re-sights all 600 (fills for some, the keys ending in A are due for the six-hourly move, the rest unchanged) and adds 400 new keys. */
            await ExecAsync(store, "UPDATE collect.query_tag_sightings SET last_seen = now() - interval '10 minutes'", ct);
            await ExecAsync(store, "UPDATE collect.query_tag_sightings SET last_seen = now() - interval '7 hours' WHERE key LIKE '%A'", ct);
            await RunAsync(store, sql, Rows(Flush(600, 400, 2, 300)), ct);
            var afterSecond = await SnapshotAsync(store, ct);

            /* Flush 3 is flush 2 again: nothing is due or filled, so no row changes. */
            await RunAsync(store, sql, Rows(Flush(600, 400, 2, 300)), ct);
            Assert.Equal(afterSecond, await SnapshotAsync(store, ct));
            snapshots.Add(afterSecond);
        }

        Assert.Equal(1000, snapshots[0].Count);
        Assert.Equal(snapshots[0], snapshots[1]);
        foreach (var kind in new[] { "|insert", "|now", "|backdated", "|old" })
        {
            Assert.True(snapshots[1].Any(row => row.EndsWith(kind, StringComparison.Ordinal)), kind + " missing: " + string.Join(",", snapshots[1].GroupBy(r => r[(r.LastIndexOf('|') + 1)..]).Select(g => g.Key + "=" + g.Count())));
        }
    }

    [Fact]
    public async Task ALargeFlush_WritesNoTemporaryBlocks_AtTheDefaultWorkMem_WhereTheReferenceSpills()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        var spilled = new Dictionary<string, bool>();
        foreach (var (name, sql) in new[] { ("reference", ReferenceUpsertSql), ("new", QueryTagSightingWriter.UpsertSql) })
        {
            await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
            await using var store = await OpenStoreAsync(scratch, ct);
            await RunAsync(store, sql, Rows(Flush(3000, 0, 1, 1000)), ct);
            await ExecAsync(store, "UPDATE collect.query_tag_sightings SET last_seen = now() - interval '10 minutes'", ct);
            await ExecAsync(store, "ANALYZE collect.query_tag_sightings", ct);
            await ExecAsync(store, "SET work_mem = '4MB'", ct);

            var rows = Rows(Flush(3000, 1500, 2, 4000));
            await using var transaction = await store.BeginTransactionAsync(ct);
            var plan = new StringBuilder();
            await using (var command = Command(store, "EXPLAIN (ANALYZE, BUFFERS) " + sql, rows))
            {
                command.Transaction = transaction;
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    plan.AppendLine(reader.GetString(0));
                }
            }

            await transaction.RollbackAsync(ct);
            spilled[name] = plan.ToString().Contains("temp read=", StringComparison.Ordinal);
        }

        Assert.True(spilled["reference"], "the reference statement is expected to spill at 4MB, or this test proves nothing");
        Assert.False(spilled["new"], "the new statement wrote temporary blocks");
    }
}
