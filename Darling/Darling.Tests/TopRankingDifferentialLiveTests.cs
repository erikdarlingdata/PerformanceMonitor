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
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5226 differential: the shipped TWO-PASS raw top-N reads against the ONE-PASS statements they replaced, on one seed,
/// row for row. The old statements are kept verbatim in <see cref="TopRankingOnePassBaseline"/> (with only the tie order
/// and NULL placement the two-pass work changed on purpose); the new ones are read through the shipped reader, so the
/// comparison covers the statement text, the parameter binding and the row mapping, and it keeps covering whatever the
/// reader's SQL becomes.
///
/// <para><b>What the seed is built to catch.</b> Ties on the ranking metric alone and on metric and CPU together (only the
/// group key orders them); groups whose reads are all NULL or partly NULL; NULL <c>database_name</c>, <c>query_hash</c> and
/// <c>host_object_name</c> key parts (a join on plain <c>=</c> drops a NULL-keyed winner); several snapshots per group (a
/// sum is not a max); a proc-hosted group whose statement fragments across four hashes (the roll-up variant); a zero-
/// execution group (the HAVING); WAITFOR shells that outrank everything (the over-fetch and the trim); parallel-only floors
/// (max_dop NULL, 1, 4, mixed); zero-interval rows (the honest-interval filter); rows just outside the window; a latest
/// text that changed between snapshots; and forty seeded random groups drawn from small value sets, so ties are the rule.
/// Each comparison is the full page, keys and totals and order.</para>
///
/// <para>Each (statement, ranking, top, database filter, parallelism floor) is one comparison, and a failure names it and
/// the first row that differs.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class TopRankingDifferentialLiveTests
{
    private const string ServerName = "darling-top-ranking-differential-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string Db1 = "DiffDb1";
    private const string Db2 = "DiffDb2";

    private static readonly TopRanking[] Rankings = [TopRanking.Cpu, TopRanking.Duration, TopRanking.Reads, TopRanking.Executions];

    /// <summary>The page sizes: smaller than the WAITFOR crowd (the +5 over-fetch bites), mid-size, and larger than the seed.</summary>
    private static readonly int[] Tops = [3, 12, 150];

    // ---------------------------------------------------------------- the seed

    /// <summary>One <c>query_stats</c> snapshot. <paramref name="At"/> is relative to now; null takes the next unique in-window instant.</summary>
    private sealed record QRow(
        string? Db, string? Hash, string? Host, string? Text, long Exec, long Cpu, long Elapsed, long? Reads,
        int? MaxDop = 1, int Interval = 3600, TimeSpan? At = null);

    /// <summary>One <c>procedure_stats</c> snapshot.</summary>
    private sealed record PRow(
        string? Db, string? Schema, string? Object, string? Type, long Exec, long Cpu, long Elapsed, long? Reads,
        int Interval = 3600, TimeSpan? At = null);

    private const string WaitforText = "WAITFOR DELAY '00:00:30'";

    private static List<QRow> QuerySeed()
    {
        var rows = new List<QRow>();

        /* One winner per ranking, three snapshots each. */
        foreach (var (name, cpu, elapsed, reads, exec) in new[]
        {
            ("HWCPU", 9_000L, 1_000L, 100L, 10L), ("HWDUR", 100L, 8_000L, 50L, 5L),
            ("HWREADS", 500L, 600L, 90_000L, 20L), ("HWEXEC", 300L, 400L, 200L, 5_000L),
        })
        {
            for (var i = 0; i < 3; i++)
            {
                rows.Add(new QRow(Db1, "0x" + name, null, "SELECT " + name, exec / 3, cpu / 3, elapsed / 3, reads / 3));
            }
        }

        /* Exact ties on all four metrics: only the group key can order TIE1..TIE4 (and TIE5/6 in the other database). */
        foreach (var (db, name) in new[] { (Db1, "TIE1"), (Db1, "TIE2"), (Db1, "TIE3"), (Db1, "TIE4"), (Db2, "TIE5"), (Db2, "TIE6") })
        {
            rows.Add(new QRow(db, "0x" + name, null, "SELECT " + name, 10, 400, 700, 55));
            rows.Add(new QRow(db, "0x" + name, null, "SELECT " + name, 10, 100, 300, 45));
        }

        /* Ties on the metric alone: same duration, reads and executions, different CPU (the CPU tie-break), and two with equal CPU too. */
        rows.Add(new QRow(Db1, "0xTMA", null, "SELECT TMA", 30, 900, 5_000, 777));
        rows.Add(new QRow(Db1, "0xTMB", null, "SELECT TMB", 30, 800, 5_000, 777));
        rows.Add(new QRow(Db1, "0xTMC", null, "SELECT TMC", 30, 800, 5_000, 777));
        rows.Add(new QRow(Db2, "0xTMD", null, "SELECT TMD", 30, 100, 5_000, 777));

        /* Reads that are NULL: all snapshots, and some snapshots (a SUM skips the NULLs). They must rank last by reads, never first. */
        rows.Add(new QRow(Db1, "0xNULLREADS", null, "SELECT NULLREADS", 50, 9_999, 9_999, null));
        rows.Add(new QRow(Db1, "0xNULLREADS", null, "SELECT NULLREADS", 50, 9_999, 9_999, null));
        rows.Add(new QRow(Db1, "0xPARTNULL", null, "SELECT PARTNULL", 8, 400, 400, null));
        rows.Add(new QRow(Db1, "0xPARTNULL", null, "SELECT PARTNULL", 8, 400, 400, 25));
        rows.Add(new QRow(Db2, "0xNULLREADS2", null, "SELECT NULLREADS2", 1, 1, 1, null));

        /* NULL key parts: a NULL database, a NULL hash, both. */
        rows.Add(new QRow(null, "0xNULLDB", null, "SELECT NULLDB", 20, 3_000, 2_000, 5_000));
        rows.Add(new QRow(null, "0xNULLDB", null, "SELECT NULLDB", 20, 3_000, 2_000, 5_000));
        rows.Add(new QRow(Db1, null, null, "SELECT NULLHASH", 20, 2_900, 2_100, 5_100));
        rows.Add(new QRow(Db1, null, null, "SELECT NULLHASH", 20, 2_900, 2_100, 5_100));
        rows.Add(new QRow(null, null, null, "SELECT NULLBOTH", 20, 2_800, 2_200, 5_200));
        rows.Add(new QRow(Db1, null, "dbo.usp_NullHashHosted", "SELECT NULLHASHHOST", 20, 2_700, 2_300, 5_300));

        /* Host objects: one hash that is ad hoc (NULL host) and proc-hosted by two procedures, and one proc whose dynamic SQL
           fragments across four hashes (four groups by hash, one group by host object). */
        rows.Add(new QRow(Db1, "0xHOSTH", null, "SELECT adhoc", 40, 1_500, 1_200, 800));
        rows.Add(new QRow(Db1, "0xHOSTH", "dbo.usp_HostA", "INSERT A EXEC", 40, 1_400, 1_300, 810));
        rows.Add(new QRow(Db1, "0xHOSTH", "dbo.usp_HostB", "INSERT B EXEC", 40, 1_300, 1_400, 820));
        foreach (var i in new[] { 1, 2, 3, 4 })
        {
            rows.Add(new QRow(Db1, "0xROLL" + i, "dbo.usp_Roll", "SELECT roll " + i, 10 * i, 300 * i, 200 * i, 100 * i));
            rows.Add(new QRow(Db1, "0xROLL" + i, "dbo.usp_Roll", "SELECT roll " + i + " newer", 10 * i, 300 * i, 200 * i, 100 * i));
        }

        /* Zero executions: out by the HAVING with zero elapsed, in with elapsed time. */
        rows.Add(new QRow(Db1, "0xZEROEXEC", null, "SELECT ZEROEXEC", 0, 50_000, 0, 60_000));
        rows.Add(new QRow(Db1, "0xZEROEXECELAPSED", null, "SELECT ZEROEXECELAPSED", 0, 1_000, 4_000, 10));

        /* WAITFOR shells that outrank every real group on every metric: seven of them, more than the +5 over-fetch. */
        for (var i = 1; i <= 7; i++)
        {
            rows.Add(new QRow(Db1, "0xWF" + i, null, WaitforText, 90_000 + i, 900_000 + i, 900_000 + i, 900_000 + i));
            rows.Add(new QRow(Db1, "0xWF" + i, null, WaitforText, 1, 1, 1, 1));
        }

        /* Parallelism floor: the lifetime max_dop of the group decides. */
        rows.Add(new QRow(Db1, "0xPARSERIAL", null, "SELECT serial", 25, 1_800, 1_100, 900, MaxDop: 1));
        rows.Add(new QRow(Db1, "0xPARWIDE", null, "SELECT wide", 25, 1_700, 1_000, 910, MaxDop: 4));
        rows.Add(new QRow(Db1, "0xPARNULL", null, "SELECT neverCaptured", 25, 1_600, 1_050, 920, MaxDop: null));
        rows.Add(new QRow(Db1, "0xPARMIX", null, "SELECT mixed", 12, 800, 500, 450, MaxDop: 1));
        rows.Add(new QRow(Db1, "0xPARMIX", null, "SELECT mixed", 13, 900, 550, 460, MaxDop: 8));

        /* Zero-interval rows: dropped by the honest-interval filter, so only the second snapshot counts. */
        rows.Add(new QRow(Db1, "0xZINT", null, "SELECT zint", 70, 50_000, 50_000, 50_000, Interval: 0));
        rows.Add(new QRow(Db1, "0xZINT", null, "SELECT zint", 7, 70, 70, 70));
        rows.Add(new QRow(Db1, "0xZINTONLY", null, "SELECT zintonly", 70, 60_000, 60_000, 60_000, Interval: 0));

        /* The window: a group wholly outside it, and one with an in-window snapshot beside huge ones before the start and after the end. */
        rows.Add(new QRow(Db1, "0xOUTSIDE", null, "SELECT outside", 99, 99_999, 99_999, 99_999, At: TimeSpan.FromHours(-30)));
        rows.Add(new QRow(Db1, "0xSTRADDLE", null, "SELECT straddle old", 99, 99_999, 99_999, 99_999, At: TimeSpan.FromHours(-29)));
        rows.Add(new QRow(Db1, "0xSTRADDLE", null, "SELECT straddle in", 3, 33, 33, 33));
        rows.Add(new QRow(Db1, "0xSTRADDLE", null, "SELECT straddle future", 99, 99_999, 99_999, 99_999, At: TimeSpan.FromHours(1)));

        /* Text: the newest non-NULL text wins, and a group with only NULL texts reads an empty one. */
        rows.Add(new QRow(Db1, "0xTEXTVAR", null, "SELECT old text", 6, 600, 600, 60));
        rows.Add(new QRow(Db1, "0xTEXTVAR", null, "SELECT newer text", 6, 600, 600, 60));
        rows.Add(new QRow(Db1, "0xTEXTVAR", null, null, 6, 600, 600, 60));
        rows.Add(new QRow(Db1, "0xNOTEXT", null, null, 6, 650, 650, 65));

        /* Seeded random groups from small value sets, so ties on the metric and on CPU come up on their own. */
        var rng = new Random(5299);
        for (var g = 0; g < 40; g++)
        {
            var db = rng.Next(4) == 0 ? Db2 : Db1;
            var hash = "0xR" + rng.Next(1, 13).ToString("00", CultureInfo.InvariantCulture);
            var host = rng.Next(5) switch { 0 => "dbo.usp_R1", 1 => "dbo.usp_R2", _ => null };
            var snapshots = rng.Next(1, 5);
            for (var s = 0; s < snapshots; s++)
            {
                rows.Add(new QRow(
                    db, hash, host,
                    rng.Next(6) == 0 ? null : "SELECT " + hash + " v" + rng.Next(3),
                    new long[] { 0, 0, 5, 10, 10, 20 }[rng.Next(6)],
                    new long[] { 0, 100, 200, 200, 500, 1_000 }[rng.Next(6)],
                    new long[] { 0, 300, 300, 900 }[rng.Next(4)],
                    rng.Next(5) == 0 ? null : new long[] { 0, 10, 10, 50 }[rng.Next(4)],
                    MaxDop: new int?[] { null, 1, 1, 4 }[rng.Next(4)]));
            }
        }

        return rows;
    }

    private static List<PRow> ProcedureSeed()
    {
        var rows = new List<PRow>();
        const string Proc = "SQL_STORED_PROCEDURE";

        foreach (var (name, cpu, elapsed, reads, exec) in new[]
        {
            ("HWCPU", 9_000L, 1_000L, 100L, 10L), ("HWDUR", 100L, 8_000L, 50L, 5L),
            ("HWREADS", 500L, 600L, 90_000L, 20L), ("HWEXEC", 300L, 400L, 200L, 5_000L),
        })
        {
            for (var i = 0; i < 3; i++)
            {
                rows.Add(new PRow(Db1, "dbo", "usp_" + name, Proc, exec / 3, cpu / 3, elapsed / 3, reads / 3));
            }
        }

        foreach (var (db, name) in new[] { (Db1, "TIE1"), (Db1, "TIE2"), (Db1, "TIE3"), (Db1, "TIE4"), (Db2, "TIE5"), (Db2, "TIE6") })
        {
            rows.Add(new PRow(db, "dbo", "usp_" + name, Proc, 10, 400, 700, 55));
            rows.Add(new PRow(db, "dbo", "usp_" + name, Proc, 10, 100, 300, 45));
        }

        /* The same name in two schemas and two object types: each is its own group. */
        rows.Add(new PRow(Db1, "dbo", "usp_Same", Proc, 10, 500, 500, 50));
        rows.Add(new PRow(Db1, "sales", "usp_Same", Proc, 10, 500, 500, 50));
        rows.Add(new PRow(Db1, "dbo", "usp_Same", "SQL_SCALAR_FUNCTION", 10, 500, 500, 50));

        rows.Add(new PRow(Db1, "dbo", "usp_TMA", Proc, 30, 900, 5_000, 777));
        rows.Add(new PRow(Db1, "dbo", "usp_TMB", Proc, 30, 800, 5_000, 777));
        rows.Add(new PRow(Db1, "dbo", "usp_TMC", Proc, 30, 800, 5_000, 777));

        rows.Add(new PRow(Db1, "dbo", "usp_NULLREADS", Proc, 50, 9_999, 9_999, null));
        rows.Add(new PRow(Db1, "dbo", "usp_NULLREADS", Proc, 50, 9_999, 9_999, null));
        rows.Add(new PRow(Db1, "dbo", "usp_PARTNULL", Proc, 8, 400, 400, null));
        rows.Add(new PRow(Db1, "dbo", "usp_PARTNULL", Proc, 8, 400, 400, 25));

        /* NULL key parts: database, schema, object name and object type. */
        rows.Add(new PRow(null, "dbo", "usp_NULLDB", Proc, 20, 3_000, 2_000, 5_000));
        rows.Add(new PRow(Db1, null, "usp_NULLSCHEMA", Proc, 20, 2_900, 2_100, 5_100));
        rows.Add(new PRow(Db1, "dbo", null, Proc, 20, 2_800, 2_200, 5_200));
        rows.Add(new PRow(Db1, "dbo", "usp_NULLTYPE", null, 20, 2_700, 2_300, 5_300));
        rows.Add(new PRow(Db1, "dbo", "usp_NULLTYPE", null, 20, 2_700, 2_300, 5_300));
        rows.Add(new PRow(null, null, null, null, 20, 2_600, 2_400, 5_400));

        rows.Add(new PRow(Db1, "dbo", "usp_ZEROEXEC", Proc, 0, 50_000, 0, 60_000));
        rows.Add(new PRow(Db1, "dbo", "usp_ZEROEXECELAPSED", Proc, 0, 1_000, 4_000, 10));

        rows.Add(new PRow(Db1, "dbo", "usp_ZINT", Proc, 70, 50_000, 50_000, 50_000, Interval: 0));
        rows.Add(new PRow(Db1, "dbo", "usp_ZINT", Proc, 7, 70, 70, 70));
        rows.Add(new PRow(Db1, "dbo", "usp_ZINTONLY", Proc, 70, 60_000, 60_000, 60_000, Interval: 0));

        rows.Add(new PRow(Db1, "dbo", "usp_OUTSIDE", Proc, 99, 99_999, 99_999, 99_999, At: TimeSpan.FromHours(-30)));
        rows.Add(new PRow(Db1, "dbo", "usp_STRADDLE", Proc, 99, 99_999, 99_999, 99_999, At: TimeSpan.FromHours(-29)));
        rows.Add(new PRow(Db1, "dbo", "usp_STRADDLE", Proc, 3, 33, 33, 33));
        rows.Add(new PRow(Db1, "dbo", "usp_STRADDLE", Proc, 99, 99_999, 99_999, 99_999, At: TimeSpan.FromHours(1)));

        var rng = new Random(5226);
        for (var g = 0; g < 30; g++)
        {
            var db = rng.Next(4) == 0 ? Db2 : Db1;
            var obj = "usp_R" + rng.Next(1, 11).ToString("00", CultureInfo.InvariantCulture);
            var schema = rng.Next(3) == 0 ? "sales" : "dbo";
            var type = rng.Next(5) == 0 ? "SQL_SCALAR_FUNCTION" : Proc;
            var snapshots = rng.Next(1, 5);
            for (var s = 0; s < snapshots; s++)
            {
                rows.Add(new PRow(
                    db, schema, obj, type,
                    new long[] { 0, 0, 5, 10, 10, 20 }[rng.Next(6)],
                    new long[] { 0, 100, 200, 200, 500, 1_000 }[rng.Next(6)],
                    new long[] { 0, 300, 300, 900 }[rng.Next(4)],
                    rng.Next(5) == 0 ? null : new long[] { 0, 10, 10, 50 }[rng.Next(4)]));
            }
        }

        return rows;
    }

    // ---------------------------------------------------------------- the differential

    /// <summary>
    /// Every ranking, both query statements (per hash and rolled up by host object), every page size, the database filter
    /// and the parallelism floor: the shipped read returns the same page as the one-pass statement, row for row.
    /// </summary>
    [Fact]
    public Task Queries_EveryRankingAndShape_ReturnTheRowsTheOnePassStatementsDid() =>
        WithSharedStoreAsync(async (connection, postgres, now, ct) =>
        {
            await PlantQueriesAsync(connection, ct, now);
            var (start, end) = (now.AddHours(-24), now.AddMinutes(5));
            var mismatches = new List<string>();
            var stats = new Stats();

            foreach (var ranking in Rankings)
            {
                foreach (var rollUp in new[] { false, true })
                {
                    foreach (var top in Tops)
                    {
                        foreach (var db in new string?[] { null, Db1 })
                        {
                            foreach (var minDop in new[] { 0, 2 })
                            {
                                var label = $"queries {(rollUp ? "host-object roll-up" : "per hash")} by {TopRankings.WireName(ranking)}, top {top}, db {db ?? "(all)"}, min_max_dop {minDop}";
                                var oldSql = rollUp ? TopRankingOnePassBaseline.QueriesByHostObject(ranking) : TopRankingOnePassBaseline.Queries(ranking);
                                var oldPage = await ReadOldQueriesAsync(postgres, oldSql, start, end, top, db, minDop, ct);
                                var result = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
                                    postgres, ServerId, start, end, top, db, rollUp, minDop, ranking, ct);
                                Assert.Equal(RetentionTier.Raw, result.Tier);
                                var newPage = result.Rows.Select(FormatQuery).ToList();
                                Compare(label, oldPage, newPage, mismatches);
                                stats.Note(ranking, oldPage);
                            }
                        }
                    }
                }
            }

            AssertNoMismatches(mismatches, stats, expectedComparisons: Rankings.Length * 2 * Tops.Length * 2 * 2);

            /* The traps the seed plants really are on the pages being compared (a join on plain = would lose the NULL host group). */
            var rolled = await DarlingDataReader.GetTopQueriesByCpuAsync(
                postgres, ServerId, start, end, top: 150, databaseName: null, rollUpByHostObject: true, ranking: TopRanking.Cpu, cancellationToken: ct);
            Assert.Contains(rolled, r => r.HostObjectName is null);
            Assert.Equal(4, Assert.Single(rolled, r => r.HostObjectName == "dbo.usp_Roll").DistinctQueryHashes);
            var perHash = await DarlingDataReader.GetTopQueriesByCpuAsync(
                postgres, ServerId, start, end, top: 150, databaseName: null, ranking: TopRanking.Reads, cancellationToken: ct);
            Assert.Equal(4, perHash.Count(r => r.HostObjectName == "dbo.usp_Roll"));
            Assert.Contains(perHash, r => r.DatabaseName == "" && r.QueryHash == "0xNULLDB");
            Assert.Contains(perHash, r => r.QueryHash == "" && r.DatabaseName == Db1);
            var readsOrder = perHash.Select(r => r.QueryHash).ToList();
            Assert.True(readsOrder.IndexOf("0xNULLREADS") > readsOrder.IndexOf("0xPARTNULL"),
                "a group whose reads are all NULL ranks after one that has some: " + string.Join(",", readsOrder));
        });

    /// <summary>The same comparison for the procedures grid.</summary>
    [Fact]
    public Task Procedures_EveryRanking_ReturnTheRowsTheOnePassStatementDid() =>
        WithSharedStoreAsync(async (connection, postgres, now, ct) =>
        {
            await PlantProceduresAsync(connection, ct, now);
            var (start, end) = (now.AddHours(-24), now.AddMinutes(5));
            var mismatches = new List<string>();
            var stats = new Stats();

            foreach (var ranking in Rankings)
            {
                foreach (var top in Tops)
                {
                    foreach (var db in new string?[] { null, Db1 })
                    {
                        var label = $"procedures by {TopRankings.WireName(ranking)}, top {top}, db {db ?? "(all)"}";
                        var oldPage = await ReadOldProceduresAsync(postgres, TopRankingOnePassBaseline.Procedures(ranking), start, end, top, db, ct);
                        var result = await DarlingDataReader.GetTopProceduresByCpuRoutedAsync(
                            postgres, ServerId, start, end, top, db, ranking, ct);
                        Assert.Equal(RetentionTier.Raw, result.Tier);
                        var newPage = result.Rows.Select(FormatProcedure).ToList();
                        Compare(label, oldPage, newPage, mismatches);
                        stats.Note(ranking, oldPage);
                    }
                }
            }

            AssertNoMismatches(mismatches, stats, expectedComparisons: Rankings.Length * Tops.Length * 2);

            var perName = await DarlingDataReader.GetTopProceduresByCpuAsync(
                postgres, ServerId, start, end, top: 150, databaseName: null, ranking: TopRanking.Reads, cancellationToken: ct);
            Assert.Contains(perName, r => r.DatabaseName == "" && r.ObjectName == "usp_NULLDB");
            Assert.Contains(perName, r => r.ObjectName == "usp_NULLTYPE");
            Assert.Equal(3, perName.Count(r => r.ObjectName == "usp_Same"));
            var readsOrder = perName.Select(r => r.ObjectName).ToList();
            Assert.True(readsOrder.IndexOf("usp_NULLREADS") > readsOrder.IndexOf("usp_PARTNULL"),
                "a procedure whose reads are all NULL ranks after one that has some: " + string.Join(",", readsOrder));
        });

    // ---------------------------------------------------------------- comparing

    /// <summary>What the comparisons covered, so a seed that stopped exercising ties or filled no page fails loudly.</summary>
    private sealed class Stats
    {
        public int Comparisons;
        public int EmptyPages;
        public readonly Dictionary<TopRanking, int> Rows = [];

        public void Note(TopRanking ranking, List<string> page)
        {
            Comparisons++;
            EmptyPages += page.Count == 0 ? 1 : 0;
            Rows[ranking] = Rows.GetValueOrDefault(ranking) + page.Count;
        }
    }

    private static void Compare(string label, List<string> oldPage, List<string> newPage, List<string> mismatches)
    {
        if (oldPage.SequenceEqual(newPage))
        {
            return;
        }

        var at = 0;
        while (at < oldPage.Count && at < newPage.Count && oldPage[at] == newPage[at])
        {
            at++;
        }

        mismatches.Add($"{label}: {oldPage.Count} old rows, {newPage.Count} new rows, first difference at row {at}:\n" +
                       $"    old: {(at < oldPage.Count ? oldPage[at] : "(none)")}\n    new: {(at < newPage.Count ? newPage[at] : "(none)")}");
    }

    private static void AssertNoMismatches(List<string> mismatches, Stats stats, int expectedComparisons)
    {
        Assert.True(mismatches.Count == 0,
            $"{mismatches.Count} of {stats.Comparisons} comparisons differ from the one-pass statements. First:\n" + string.Join("\n", mismatches.Take(5)));
        Assert.Equal(expectedComparisons, stats.Comparisons);
        Assert.True(stats.EmptyPages * 4 < stats.Comparisons, $"{stats.EmptyPages} of {stats.Comparisons} compared pages were empty; the seed no longer fills them");
        foreach (var ranking in Rankings)
        {
            Assert.True(stats.Rows.GetValueOrDefault(ranking) > 50, $"{TopRankings.WireName(ranking)}: only {stats.Rows.GetValueOrDefault(ranking)} rows compared");
        }
    }

    /// <summary>One page row as a comparable line: every column the reader maps from the statement's first block, in order.</summary>
    private static string FormatQuery(DarlingDataReader.TopQueryRow r) => string.Join("|", new object?[]
    {
        r.DatabaseName, r.QueryHash, r.HostObjectName ?? "<null>", r.QueryPlanHash, r.SqlHandle, r.PlanHandle,
        r.TotalExecutions, r.TotalCpuUs, r.TotalElapsedUs, r.TotalLogicalReads, r.TotalLogicalWrites, r.TotalPhysicalReads,
        r.TotalRows, r.TotalSpills, r.MinDop, r.MaxDop, r.MinCpuUs, r.MaxCpuUs, r.MinElapsedUs, r.MaxElapsedUs,
        r.QueryText ?? "", r.DistinctTexts, r.DistinctQueryHashes,
    }.Select(v => Convert.ToString(v, CultureInfo.InvariantCulture)));

    private static string FormatProcedure(DarlingDataReader.TopProcedureRow r) => string.Join("|", new object?[]
    {
        r.DatabaseName, r.SchemaName, r.ObjectName, r.ObjectType, r.SqlHandle, r.PlanHandle,
        r.TotalExecutions, r.TotalCpuUs, r.TotalElapsedUs, r.TotalLogicalReads, r.TotalLogicalWrites, r.TotalPhysicalReads,
        r.TotalSpills, r.MinCpuUs, r.MaxCpuUs, r.MinElapsedUs, r.MaxElapsedUs,
    }.Select(v => Convert.ToString(v, CultureInfo.InvariantCulture)));

    /// <summary>Runs an old one-pass queries statement and maps its columns the way the reader does (NULL text and strings read empty, NULL numbers read zero).</summary>
    private static async Task<List<string>> ReadOldQueriesAsync(
        NpgsqlDataSource postgres, string sql, DateTime start, DateTime end, int top, string? database, int minMaxDop, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(sql);
        AddCommonParameters(command, start, end, top, database);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = minMaxDop });
        await using var reader = await command.ExecuteReaderAsync(ct);
        var page = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            string Text(int i) => reader.IsDBNull(i) ? "" : reader.GetString(i);
            long Long(int i) => reader.IsDBNull(i) ? 0 : reader.GetInt64(i);
            int Int(int i) => reader.IsDBNull(i) ? 0 : Convert.ToInt32(reader.GetValue(i));
            page.Add(string.Join("|", new object?[]
            {
                Text(0), Text(1), reader.IsDBNull(2) ? "<null>" : reader.GetString(2), Text(3), Text(4), Text(5),
                Long(6), Long(7), Long(8), Long(9), Long(10), Long(11), Long(12), Long(13), Int(14), Int(15),
                Long(16), Long(17), Long(18), Long(19), Text(20), Long(21), reader.IsDBNull(22) ? 1L : reader.GetInt64(22),
            }.Select(v => Convert.ToString(v, CultureInfo.InvariantCulture))));
        }

        return page;
    }

    private static async Task<List<string>> ReadOldProceduresAsync(
        NpgsqlDataSource postgres, string sql, DateTime start, DateTime end, int top, string? database, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(sql);
        AddCommonParameters(command, start, end, top, database);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var page = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            string Text(int i) => reader.IsDBNull(i) ? "" : reader.GetString(i);
            long Long(int i) => reader.IsDBNull(i) ? 0 : reader.GetInt64(i);
            page.Add(string.Join("|", new object?[]
            {
                Text(0), Text(1), Text(2), Text(3), Text(4), Text(5),
                Long(6), Long(7), Long(8), Long(9), Long(10), Long(11), Long(12), Long(13), Long(14), Long(15), Long(16),
            }.Select(v => Convert.ToString(v, CultureInfo.InvariantCulture))));
        }

        return page;
    }

    /// <summary>$1 server, $2 and $3 the window, $4 top, $5 the database filter: the order both old statements and the shipped reader bind.</summary>
    private static void AddCommonParameters(NpgsqlCommand command, DateTime start, DateTime end, int top, string? database)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = end });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = top });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)database ?? DBNull.Value });
    }

    // ---------------------------------------------------------------- planting and plumbing

    /// <summary>Plants the query seed. Every in-window row gets its own second, so a group's latest text is never a tie.</summary>
    private static async Task PlantQueriesAsync(NpgsqlConnection connection, CancellationToken ct, DateTime now)
    {
        var tick = 0;
        foreach (var row in QuerySeed())
        {
            var at = DarlingMcpTestData.TruncateToSeconds(row.At is { } offset ? now + offset : now.AddHours(-3).AddSeconds(7 * tick++));
            byte[]? digest = row.Text is null ? null : SHA256.HashData(Encoding.UTF8.GetBytes(row.Text));
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_stats
                      (collection_id, collection_time, server_id, server_name, database_name, query_hash, host_object_name,
                       query_plan_hash, sql_handle, plan_handle, query_text, query_text_digest,
                       delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                       delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time,
                       min_elapsed_time, max_elapsed_time, min_dop, max_dop, sample_interval_seconds)
                  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, (object?)row.Db ?? DBNull.Value, (object?)row.Hash ?? DBNull.Value,
                (object?)row.Host ?? DBNull.Value, "0xPH" + row.Hash, "0xSH" + row.Hash, "0xPLH" + row.Hash,
                (object?)row.Text ?? DBNull.Value, (object?)digest ?? DBNull.Value,
                row.Exec, row.Cpu, row.Elapsed, (object?)row.Reads ?? DBNull.Value, row.Cpu / 10, (object?)(row.Reads / 100) ?? DBNull.Value,
                row.Cpu / 2, row.Cpu, row.Elapsed / 2, row.Elapsed,
                (object?)row.MaxDop ?? DBNull.Value, (object?)row.MaxDop ?? DBNull.Value, row.Interval);
        }
    }

    private static async Task PlantProceduresAsync(NpgsqlConnection connection, CancellationToken ct, DateTime now)
    {
        var tick = 0;
        foreach (var row in ProcedureSeed())
        {
            var at = DarlingMcpTestData.TruncateToSeconds(row.At is { } offset ? now + offset : now.AddHours(-3).AddSeconds(7 * tick++));
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO procedure_stats
                      (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
                       sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                       delta_logical_writes, delta_physical_reads, delta_spills, min_worker_time, max_worker_time,
                       min_elapsed_time, max_elapsed_time, sample_interval_seconds)
                  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, (object?)row.Db ?? DBNull.Value, (object?)row.Schema ?? DBNull.Value,
                (object?)row.Object ?? DBNull.Value, (object?)row.Type ?? DBNull.Value, "0xSH" + row.Object, "0xPLH" + row.Object,
                row.Exec, row.Cpu, row.Elapsed, (object?)row.Reads ?? DBNull.Value, row.Cpu / 10, (object?)(row.Reads / 100) ?? DBNull.Value,
                row.Exec / 5, row.Cpu / 2, row.Cpu, row.Elapsed / 2, row.Elapsed, row.Interval);
        }
    }

    /// <summary>Runs a body against the shared store with this class's server registered, and leaves nothing behind.</summary>
    private static async Task WithSharedStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, DateTime, CancellationToken, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live top-ranking differential tests.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await body(connection, postgres, DarlingMcpTestData.Naive(DateTime.UtcNow), ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM query_stats WHERE server_id = {ServerId}; DELETE FROM procedure_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId}");
}
