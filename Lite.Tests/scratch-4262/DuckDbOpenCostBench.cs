/*
 * THROWAWAY benchmark for #4262. Not part of the product or the permanent test suite.
 * Lives on branch scratch/4262-duckdb-open-cost only; never merged.
 *
 * Measures:
 *  A) cost of a brand-new DuckDBConnection(connStr).OpenAsync() (what OpenConnectionAsync does today)
 *  B) cost of DuckDBConnection.Duplicate() off one already-open connection (candidate cheap path)
 *  C) whether a Duplicate()'d connection survives the database file being deleted and recreated
 *     underneath it (what ResetDatabaseAsync / the storage-version migration path do)
 * at two database sizes.
 */

using System;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using Xunit;

namespace Lite.Tests.Scratch4262;

public class DuckDbOpenCostBench
{
    private const int Iterations = 30;

    [Fact]
    public async Task MeasureOpenCost()
    {
        var sb = new StringBuilder();
        void Log(string s)
        {
            sb.AppendLine(s);
        }

        foreach (var (label, seedRows) in new[] { ("empty-fresh-install", 0), ("busy-2M-rows", 2_000_000) })
        {
            var tempDir = Path.Combine(Path.GetTempPath(), "Bench4262_" + Guid.NewGuid().ToString("N")[..8]);
            Directory.CreateDirectory(tempDir);
            var dbPath = Path.Combine(tempDir, "bench.duckdb");
            var duckDb = new DuckDbInitializer(dbPath);
            await duckDb.InitializeAsync();

            var tableCount = 0;
            using (var schemaConn = duckDb.CreateConnection())
            {
                await schemaConn.OpenAsync();
                using var cmd = schemaConn.CreateCommand();
                cmd.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'";
                tableCount = Convert.ToInt32(await cmd.ExecuteScalarAsync());

                if (seedRows > 0)
                {
                    /* One synthetic wide-ish table on top of the real (empty) schema — approximates a
                       busy install's file size/page count without needing to reverse the production
                       collector schema for a throwaway benchmark. */
                    using (var create = schemaConn.CreateCommand())
                    {
                        create.CommandText = "CREATE TABLE bench_rows (id BIGINT, ts TIMESTAMP, txt VARCHAR, val DOUBLE)";
                        await create.ExecuteNonQueryAsync();
                    }
                    using (var seed = schemaConn.CreateCommand())
                    {
                        seed.CommandText = $@"
INSERT INTO bench_rows
SELECT i, now() - to_seconds(i), repeat('x', 40), i * 1.5
FROM range({seedRows}) AS t(i)";
                        await seed.ExecuteNonQueryAsync();
                    }
                    using (var checkpoint = schemaConn.CreateCommand())
                    {
                        checkpoint.CommandText = "CHECKPOINT";
                        await checkpoint.ExecuteNonQueryAsync();
                    }
                }
            }

            var fileSizeMb = new FileInfo(dbPath).Length / (1024.0 * 1024.0);
            Log($"=== {label}: {tableCount} tables, {seedRows:N0} seeded rows, file {fileSizeMb:F1} MB ===");

            /* A: N sequential brand-new connections — what OpenConnectionAsync does today. */
            var freshOpenMs = new double[Iterations];
            var freshTotalMs = new double[Iterations];
            for (int i = 0; i < Iterations; i++)
            {
                var swTotal = Stopwatch.StartNew();
                using var c = new DuckDBConnection(duckDb.ConnectionString);
                var swOpen = Stopwatch.StartNew();
                await c.OpenAsync();
                swOpen.Stop();
                swTotal.Stop();
                freshOpenMs[i] = swOpen.Elapsed.TotalMilliseconds;
                freshTotalMs[i] = swTotal.Elapsed.TotalMilliseconds;
                c.Close();
            }
            Report(Log, "fresh new DuckDBConnection + OpenAsync (ctor+open)", freshTotalMs);

            /* Duplicate() throws NotSupportedException for a file-backed connection (confirmed against
               DuckDB.NET 1.5.5 source) — "only supported for in-memory connections". So the candidate
               cheap path is NOT Duplicate(); it's DuckDBConnection.Open()'s own ConnectionManager, which
               keeps a refcounted reference per unique connection string and only pays the real
               duckdb_open (catalog load / WAL replay) on the 0-to-1 transition. A second, concurrent, or
               overlapping Open() on the SAME connection string should just attach to the live instance. */
            try
            {
                /* B: hold one sentinel open, then do N sequential open+close cycles while it lives —
                   each should be a refcount attach (warm), not a real catalog load (cold). */
                using (var sentinel = new DuckDBConnection(duckDb.ConnectionString))
                {
                    await sentinel.OpenAsync();

                    var warmMs = new double[Iterations];
                    for (int i = 0; i < Iterations; i++)
                    {
                        var sw = Stopwatch.StartNew();
                        using var c = new DuckDBConnection(duckDb.ConnectionString);
                        await c.OpenAsync();
                        sw.Stop();
                        warmMs[i] = sw.Elapsed.TotalMilliseconds;
                        c.Close();
                    }
                    Report(Log, "WARM open while sentinel holds the instance (refcount attach)", warmMs);

                    using (var cmd = sentinel.CreateCommand())
                    {
                        cmd.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'";
                        var seenTables = Convert.ToInt32(await cmd.ExecuteScalarAsync());
                        Log($"  sentinel sanity: sees {seenTables} tables (expected {tableCount})");
                    }

                    /* CHECKPOINT while the sentinel is open and idle (no open transaction) — must not fail
                       just because a long-lived reader handle exists alongside it. */
                    using (var cmd = sentinel.CreateCommand())
                    {
                        cmd.CommandText = "CHECKPOINT";
                        await cmd.ExecuteNonQueryAsync();
                        Log("  CHECKPOINT succeeded with the sentinel open and idle");
                    }
                }
            }
            catch (Exception ex)
            {
                Log($"  [B FAILED] {ex.GetType().Name}: {ex.Message}");
            }

            try
            {
                /* C: N connections opened CONCURRENTLY via Task.WhenAll, all held open simultaneously —
                   this is the actual shape of one Overview refresh (10 parallel _dataService.Get* calls).
                   If the ConnectionManager attaches concurrent opens to the same in-flight instance, only
                   the very first should pay the real cost; the rest should be cheap even with no
                   deliberate sentinel. */
                var concurrentTimings = new double[Iterations];
                var openTasks = new Task[Iterations];
                var conns = new DuckDBConnection[Iterations];
                var swAll = Stopwatch.StartNew();
                for (int i = 0; i < Iterations; i++)
                {
                    var idx = i;
                    conns[idx] = new DuckDBConnection(duckDb.ConnectionString);
                    openTasks[idx] = Task.Run(async () =>
                    {
                        var sw = Stopwatch.StartNew();
                        await conns[idx].OpenAsync();
                        sw.Stop();
                        concurrentTimings[idx] = sw.Elapsed.TotalMilliseconds;
                    });
                }
                await Task.WhenAll(openTasks);
                swAll.Stop();
                Report(Log, "CONCURRENT open, all N in flight together (Task.WhenAll, no sentinel)", concurrentTimings);
                Log($"  wall time for all {Iterations} concurrent opens together: {swAll.Elapsed.TotalMilliseconds:F2} ms");
                foreach (var c in conns) { c.Close(); c.Dispose(); }
            }
            catch (Exception ex)
            {
                Log($"  [C FAILED] {ex.GetType().Name}: {ex.Message}");
            }

            try
            {
                /* D: what a long-lived sentinel would have to survive — the file being deleted and
                   recreated underneath it, which is exactly what ResetDatabaseAsync and the
                   storage-version migration path do (both under the write lock today). */
                var sentinel2 = new DuckDBConnection(duckDb.ConnectionString);
                await sentinel2.OpenAsync();

                var walPath = dbPath + ".wal";
                try
                {
                    File.Delete(dbPath);
                    if (File.Exists(walPath)) File.Delete(walPath);
                    Log("  File.Delete(dbPath) SUCCEEDED while the sentinel was open (no OS-level share-lock stopped it)");

                    var freshAfterDelete = new DuckDbInitializer(dbPath);
                    await freshAfterDelete.InitializeAsync();

                    using (var cmd = sentinel2.CreateCommand())
                    {
                        cmd.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'";
                        var result = await cmd.ExecuteScalarAsync();
                        Log($"  sentinel opened BEFORE the delete still answers after delete+recreate: {result} tables (did not throw)");
                    }

                    /* The discriminating check: does a BRAND-NEW connection opened AFTER the reset — same
                       connection string, opened the way any ordinary caller would — see the truly-fresh
                       recreated file, or does it attach (via the ConnectionManager's refcount) to the SAME
                       stale in-memory instance the sentinel kept alive, silently neutering the reset for
                       every caller in the process until the sentinel finally closes? bench_rows only
                       exists in the OLD file; InitializeAsync never recreates it. */
                    using (var freshConn = new DuckDBConnection(duckDb.ConnectionString))
                    {
                        await freshConn.OpenAsync();
                        using var cmd = freshConn.CreateCommand();
                        cmd.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = 'bench_rows'";
                        var stillHasBenchRows = Convert.ToInt32(await cmd.ExecuteScalarAsync()) > 0;
                        Log(stillHasBenchRows
                            ? "  [RISK] a NEW connection opened after the reset still sees 'bench_rows' — it attached to the sentinel's stale cached instance, NOT the freshly recreated file. The reset was silently neutered while the sentinel lived."
                            : "  a NEW connection opened after the reset does NOT see 'bench_rows' — it correctly reached the freshly recreated file.");
                    }
                }
                catch (IOException ex)
                {
                    Log($"  File.Delete(dbPath) THREW while the sentinel was open: {ex.GetType().Name}: {ex.Message} — a live handle blocks the reset/migration file swap at the OS level");
                }
                finally
                {
                    try { sentinel2.Close(); } catch { /* best effort */ }
                    sentinel2.Dispose();
                }
            }
            catch (Exception ex)
            {
                Log($"  [D FAILED] {ex.GetType().Name}: {ex.Message}");
            }

            try { Directory.Delete(tempDir, recursive: true); } catch { /* best effort */ }

            /* Flush after every size so one later failure doesn't cost the earlier size's numbers. */
            var resultsDir = Path.Combine(AppContext.BaseDirectory, "scratch-4262-results");
            Directory.CreateDirectory(resultsDir);
            await File.WriteAllTextAsync(Path.Combine(resultsDir, "results.txt"), sb.ToString());
        }

        // Always-pass: this is a measurement harness, not a regression pin.
        Assert.True(true, "benchmark ran; see scratch-4262-results/results.txt next to the test binary");
    }

    private static void Report(Action<string> log, string label, double[] timingsMs)
    {
        var sorted = timingsMs.OrderBy(x => x).ToArray();
        var n = sorted.Length;
        log($"  {label}: n={n} min={sorted[0]:F2}ms p50={sorted[n / 2]:F2}ms p95={sorted[(int)(n * 0.95) < n ? (int)(n * 0.95) : n - 1]:F2}ms max={sorted[n - 1]:F2}ms avg={sorted.Average():F2}ms");
    }
}
