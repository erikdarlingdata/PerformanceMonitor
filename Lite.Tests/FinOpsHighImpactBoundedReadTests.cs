/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Reflection;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8117: Lite's high-impact read ships the scorer only the hashes it can keep (the top N on each of its six measures),
/// groups before it ranks, and looks up the sample text for the kept hashes alone. The scored result is the one scoring
/// every hash in the window gives.
/// </summary>
public sealed class FinOpsHighImpactBoundedReadTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly DuckDbInitializer _duckDb;

    public FinOpsHighImpactBoundedReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private const int ServerId = -8117;
    private const string ServerName = "lite-finops-high-impact-bounded";

    /// <summary>Hashes seeded with distinct figures: many more than six times the scorer's default keep count can hold.</summary>
    private const int HashCount = 80;

    /// <summary>The scorer's per-measure keep count for the ship-count fact: small, so the window holds many more hashes than six times it.</summary>
    private const int SmallTopN = 3;

    private static string Hash(int i) => "0xL" + i.ToString("D3");

    private static string BusiestText(string hash) => "SELECT " + hash + ";";

    private static string LaterText(string hash) => "SELECT /* later, fewer executions */ " + hash + ";";

    /// <summary>Six measures that each put a different hash on top; a third of the hashes wrote nothing.</summary>
    private static (long Exec, long CpuUs, long ElapsedUs, long Reads, long Writes, long GrantKb) Figures(int i) =>
        (Exec: (i * 17) % 47 + 1,
         CpuUs: ((i * 37) % 61 + 1) * 100_000L,
         ElapsedUs: ((i * 53) % 59 + 1) * 150_000L,
         Reads: ((i * 29) % 67 + 1) * 1_000L,
         Writes: Math.Max(0, (i * 41) % 71 - 30) * 50L,
         GrantKb: ((i * 23) % 53 + 1) * 2_048L);

    /// <summary>The later, smaller row every tenth hash also has in Db9; its grant is the one the hash reports (#7166) and its text is never the sample.</summary>
    private static (long Exec, long CpuUs, long ElapsedUs, long Reads, long Writes, long GrantKb) SecondRow(int i) =>
        (Exec: 2, CpuUs: 50_000L, ElapsedUs: 75_000L, Reads: 500L, Writes: 25L, GrantKb: (i % 7 + 1) * 4_096L);

    /// <summary>
    /// The window's per-hash aggregate as the seed wrote it, before any scoring: the rows the scorer saw when the
    /// statement shipped every hash. Hash i runs in Db(i % 4); every tenth hash also ran, later and for less CPU, in
    /// Db9, so its figures sum across two databases, its database is the one with the most CPU (#7695) and its memory
    /// is the newer row's grant. Ordered as the statement orders: CPU descending, then hash.
    /// </summary>
    private static List<HighImpactQueryRow> ExpectedAggregate()
    {
        var rows = new List<HighImpactQueryRow>();
        for (var i = 1; i <= HashCount; i++)
        {
            var f = Figures(i);
            var row = new HighImpactQueryRow
            {
                QueryHash = Hash(i),
                DatabaseName = "Db" + (i % 4),
                DatabaseCount = 1,
                TotalExecutions = f.Exec,
                TotalCpuMs = f.CpuUs / 1000.0m,
                TotalDurationMs = f.ElapsedUs / 1000.0m,
                TotalReads = f.Reads,
                TotalWrites = f.Writes,
                PeakMemoryMb = f.GrantKb / 1024.0m,
                SampleQueryText = BusiestText(Hash(i)),
                FullQueryText = BusiestText(Hash(i)),
            };
            if (i % 10 == 0)
            {
                var s = SecondRow(i);
                row.DatabaseCount = 2;
                row.TotalExecutions += s.Exec;
                row.TotalCpuMs += s.CpuUs / 1000.0m;
                row.TotalDurationMs += s.ElapsedUs / 1000.0m;
                row.TotalReads += s.Reads;
                row.TotalWrites += s.Writes;
                row.PeakMemoryMb = s.GrantKb / 1024.0m;
            }
            rows.Add(row);
        }

        return rows.OrderByDescending(r => r.TotalCpuMs).ThenBy(r => r.QueryHash, StringComparer.Ordinal).ToList();
    }

    private async Task SeedAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        var at = DateTime.UtcNow.AddHours(-2);
        var id = -8_117_000L;
        for (var i = 1; i <= HashCount; i++)
        {
            var f = Figures(i);
            await Plant(connection, id--, at, Hash(i), "Db" + (i % 4), BusiestText(Hash(i)), f.Exec, f.CpuUs, f.ElapsedUs, f.Reads, f.Writes, f.GrantKb);
            if (i % 10 == 0)
            {
                var s = SecondRow(i);
                await Plant(connection, id--, at.AddMinutes(30), Hash(i), "Db9", LaterText(Hash(i)), s.Exec, s.CpuUs, s.ElapsedUs, s.Reads, s.Writes, s.GrantKb);
            }
        }

        /* Outside the window: never aggregated, never a candidate. */
        await Plant(connection, id--, at.AddDays(-3), "0xLOLD", "Db0", "SELECT 'old';", 99, 900_000_000L, 900_000_000L, 9_000_000L, 9_000L, 99_999L);
    }

    private static async Task Plant(DuckDBConnection connection, long id, DateTime at, string hash, string db, string text,
        long exec, long cpuUs, long elapsedUs, long reads, long writes, long grantKb)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text, creation_time,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, max_grant_kb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)";
        foreach (var value in new object[] { id, at, ServerId, ServerName, db, hash, text, at.AddDays(-1), exec, cpuUs, elapsedUs, reads, writes, grantKb })
        {
            command.Parameters.Add(new DuckDBParameter { Value = value });
        }
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// The read's result over a window of many hashes is the scorer's result over every hash in the window: the same
    /// rows, in the same order, with the same figures, shares, scores and sample text, including the hash whose figures
    /// sum across two databases and whose later, quieter row must not supply the sample.
    /// </summary>
    [Fact]
    public async Task GetHighImpactQueriesAsync_ScoresAsIfEveryHashHadBeenShipped()
    {
        await SeedAsync();

        var expected = HighImpactScorer.Score(ExpectedAggregate());
        Assert.True(expected.Count < HashCount, $"the seed keeps {expected.Count} of {HashCount} hashes; it must keep fewer than it seeds");
        Assert.Contains(expected, r => r.DatabaseCount == 2);
        Assert.Contains(expected, r => r.TotalWrites == 0);

        var actual = await new LocalDataService(_duckDb).GetHighImpactQueriesAsync(ServerId, 24);

        Assert.Equal(expected.Select(r => r.QueryHash).ToList(), actual.Select(r => r.QueryHash).ToList());
        for (var i = 0; i < expected.Count; i++)
        {
            var e = expected[i];
            var a = actual[i];
            Assert.Equal(e.DatabaseName, a.DatabaseName);
            Assert.Equal(e.DatabaseCount, a.DatabaseCount);
            Assert.Equal(e.TotalExecutions, a.TotalExecutions);
            Assert.Equal(e.TotalCpuMs, a.TotalCpuMs);
            Assert.Equal(e.TotalDurationMs, a.TotalDurationMs);
            Assert.Equal(e.TotalReads, a.TotalReads);
            Assert.Equal(e.TotalWrites, a.TotalWrites);
            Assert.Equal(e.PeakMemoryMb, a.PeakMemoryMb);
            Assert.Equal(e.CpuShare, a.CpuShare);
            Assert.Equal(e.DurationShare, a.DurationShare);
            Assert.Equal(e.ReadsShare, a.ReadsShare);
            Assert.Equal(e.WritesShare, a.WritesShare);
            Assert.Equal(e.MemoryShare, a.MemoryShare);
            Assert.Equal(e.ExecutionsShare, a.ExecutionsShare);
            Assert.Equal(e.ImpactScore, a.ImpactScore);
            Assert.Equal(e.SampleQueryText, a.SampleQueryText);
            Assert.Equal(e.FullQueryText, a.FullQueryText);
        }
    }

    /// <summary>
    /// The statement ships the scorer at most six times its keep count, however many hashes the window holds, and what
    /// it ships scores to the same result as every hash would. The statement is <c>LocalDataService.HighImpactQueriesSql</c>
    /// ($1 server_id, $2 cutoff, $3 the keep count), reached by name so the fact states what is missing when it is.
    /// </summary>
    [Fact]
    public async Task Statement_ShipsAtMostSixTimesTopN_AndTheyScoreTheSame()
    {
        await SeedAsync();

        var member = typeof(LocalDataService).GetField("HighImpactQueriesSql", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);
        Assert.True(member is not null,
            "LocalDataService.HighImpactQueriesSql is missing: the high-impact statement has no per-measure cap, so every query_hash in the window is shipped to the scorer.");
        var sql = Assert.IsType<string>(member!.GetValue(null));

        var candidates = new List<HighImpactQueryRow>();
        using (var readLock = _duckDb.AcquireReadLock())
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync();
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.Parameters.Add(new DuckDBParameter { Value = ServerId });
            command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-24) });
            command.Parameters.Add(new DuckDBParameter { Value = SmallTopN });
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                candidates.Add(new HighImpactQueryRow
                {
                    QueryHash = reader.GetString(0),
                    DatabaseName = reader.GetString(1),
                    TotalExecutions = ToInt64(reader.GetValue(2)),
                    TotalCpuMs = Convert.ToDecimal(reader.GetValue(3)),
                    TotalDurationMs = Convert.ToDecimal(reader.GetValue(4)),
                    TotalReads = ToInt64(reader.GetValue(5)),
                    TotalWrites = ToInt64(reader.GetValue(6)),
                    PeakMemoryMb = Convert.ToDecimal(reader.GetValue(7)),
                    DatabaseCount = (int)ToInt64(reader.GetValue(8)),
                    SampleQueryText = reader.IsDBNull(9) ? "" : reader.GetString(9),
                    FullQueryText = reader.IsDBNull(10) ? "" : reader.GetString(10),
                });
            }
        }

        Assert.True(candidates.Count <= 6 * SmallTopN, $"{candidates.Count} candidates shipped for a keep count of {SmallTopN}; at most {6 * SmallTopN} can be kept");
        Assert.True(candidates.Count < HashCount, $"{candidates.Count} candidates shipped from a window of {HashCount} hashes");

        var expected = HighImpactScorer.Score(ExpectedAggregate(), SmallTopN);
        var scored = HighImpactScorer.Score(candidates, SmallTopN);
        Assert.Equal(expected.Select(r => r.QueryHash).ToList(), scored.Select(r => r.QueryHash).ToList());
        Assert.Equal(expected.Select(r => r.ImpactScore).ToList(), scored.Select(r => r.ImpactScore).ToList());
        Assert.Equal(expected.Select(r => r.CpuShare).ToList(), scored.Select(r => r.CpuShare).ToList());
        Assert.Equal(expected.Select(r => r.MemoryShare).ToList(), scored.Select(r => r.MemoryShare).ToList());
        Assert.Equal(expected.Select(r => r.SampleQueryText).ToList(), scored.Select(r => r.SampleQueryText).ToList());
    }

    /// <summary>A summed BIGINT comes back from DuckDB as a <see cref="BigInteger"/>.</summary>
    private static long ToInt64(object value) => value is BigInteger big ? (long)big : Convert.ToInt64(value);
}
