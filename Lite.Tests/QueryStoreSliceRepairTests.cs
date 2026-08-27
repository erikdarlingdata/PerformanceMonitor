/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #1912 for Lite: the pre-#1907 split slices are collapsed in the hot DuckDB table AND in the parquet
/// archive that unions into the read views.
///
/// <para>Lite gets a FULL repair where Darling gets a bounded one, because Lite's long tier IS the archive —
/// the same rows, rewritable — rather than a materialized rollup that cannot be rebuilt from raw that no
/// longer exists.</para>
///
/// <para><b>Every archive here is a scratch file this test wrote.</b> The measurements that justified this
/// work were taken against a real archive by COPYING it; the shipped code only ever rewrites an archive when
/// an operator asks, and the tests never point at one.</para>
/// </summary>
public sealed class QueryStoreSliceRepairTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 9121;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _archivePath;
    private DuckDBConnection? _seed;
    private long _nextId = 1;

    public QueryStoreSliceRepairTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _archivePath = Path.Combine(Path.GetTempPath(), $"pm-1912-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_archivePath);

        /* The repair marker is created on demand and is deliberately NOT part of the managed schema — it must
           survive normal operation, which is the whole point of it. That also means the shared fixture's
           ResetData does not know to clear it, so it would leak between tests in this class and make the
           second one see a store that had "already been repaired". Dropped here rather than taught to
           ResetData, because the production behavior being relied on is precisely that nothing routine
           removes it. */
        using var connection = _duckDb.CreateConnection();
        connection.Open();
        using var drop = connection.CreateCommand();
        drop.CommandText = $"DROP TABLE IF EXISTS {QueryStoreSliceRepairService.MarkerTable}";
        drop.ExecuteNonQuery();
    }

    public void Dispose()
    {
        _seed?.Dispose();
        try
        {
            if (Directory.Exists(_archivePath))
            {
                Directory.Delete(_archivePath, recursive: true);
            }
        }
        catch (IOException)
        {
            /* A scratch directory that outlives one test run is noise, not a failure. */
        }
    }

    /// <summary>
    /// The key must be built from the columns a file ACTUALLY has. Archive files are monthly and the schema
    /// moved underneath them — <c>runtime_stats_interval_id</c> arrived with #1841 tier 2,
    /// <c>replica_role</c> with #1844/#1872 — so a file written before those carries neither. A real one on
    /// the author's machine (June 2026) is missing both.
    ///
    /// <para>WATCHED (mutation): make <c>KeyColumnsFor</c> return the full key regardless of what is present
    /// and the old-era assertions here fail — which is the cheap version of what the mutation would do in the
    /// field, where naming an absent column fails the rewrite outright and, worse, assuming the newest schema
    /// silently groups on a key that is not that era's dedup key at all.</para>
    /// </summary>
    [Fact]
    public void KeyColumns_AdaptToTheFilesOwnEra_RatherThanAssumingTheNewestSchema()
    {
        var modern = QueryStoreSliceRepairService.KeyColumnsFor(
        [
            "server_id", "database_name", "query_id", "plan_id", "runtime_stats_interval_id",
            "first_execution_time", "execution_type_desc", "replica_role", "collection_time", "execution_count",
        ]);

        Assert.Contains("runtime_stats_interval_id", modern);
        Assert.Contains("replica_role", modern);
        Assert.Equal(9, modern.Count);

        /* The pre-tier-2, pre-replica era: the tier-1 proxy key is all there has ever been for these rows. */
        var legacy = QueryStoreSliceRepairService.KeyColumnsFor(
        [
            "server_id", "database_name", "query_id", "plan_id",
            "first_execution_time", "execution_type_desc", "collection_time", "execution_count",
        ]);

        Assert.DoesNotContain("runtime_stats_interval_id", legacy);
        Assert.DoesNotContain("replica_role", legacy);
        Assert.Contains("first_execution_time", legacy);
        Assert.Contains("collection_time", legacy);
        Assert.Equal(7, legacy.Count);
    }

    /// <summary>
    /// The combining rules, which are the part that is wrong-but-invisible if they drift: a plain average of
    /// slice averages weights a 25-execution sliver the same as a 100-execution flush, and nothing downstream
    /// can tell afterwards.
    /// </summary>
    [Fact]
    public void CombineRules_MirrorTheCollectorsOwnAggregation()
    {
        Assert.Equal("SUM(execution_count)", QueryStoreSliceRepairService.CombineExpression("execution_count"));
        Assert.Equal("MAX(last_execution_time)", QueryStoreSliceRepairService.CombineExpression("last_execution_time"));
        Assert.Equal("MIN(min_duration_us)", QueryStoreSliceRepairService.CombineExpression("min_duration_us"));
        Assert.Equal("MAX(max_duration_us)", QueryStoreSliceRepairService.CombineExpression("max_duration_us"));
        Assert.Equal("ANY_VALUE(query_text)", QueryStoreSliceRepairService.CombineExpression("query_text"));

        var weighted = QueryStoreSliceRepairService.CombineExpression("avg_duration_us");
        Assert.Contains("SUM(CAST(avg_duration_us AS DOUBLE) * execution_count)", weighted, StringComparison.Ordinal);
        Assert.Contains("NULLIF(SUM(execution_count), 0)", weighted, StringComparison.Ordinal);
        Assert.DoesNotContain("AVG(", weighted, StringComparison.Ordinal);
    }

    [Fact]
    public async Task HotStore_CollapsesSplitSlices_LeavesCorrectlyCollectedRowsAlone_AndIsIdempotent()
    {
        var t = new DateTime(2026, 6, 12, 10, 0, 0, DateTimeKind.Unspecified);

        /* One interval stored as its two tied slices — the pre-#1907 shape, with the live SQL 2022 repro's
           numbers: 100 flushed + 25 in memory, truth 125, weighted mean 1871. */
        await SeedAsync(t, queryId: 1, planId: 11, intervalId: 5001, executionCount: 100, avgDurationUs: 1778);
        await SeedAsync(t, queryId: 1, planId: 11, intervalId: 5001, executionCount: 25, avgDurationUs: 2245);

        /* A correctly-collected interval that must come through untouched. */
        await SeedAsync(t, queryId: 2, planId: 22, intervalId: 5002, executionCount: 55, avgDurationUs: 500);

        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath);

        var survey = await service.SurveyAsync();
        Assert.Equal(1, survey.HotGroups);
        Assert.Equal(2, survey.HotRows);
        Assert.Equal(1, survey.HotRowsRemoved);
        Assert.True(survey.HasWork);

        Assert.Equal(1, (await service.RepairAsync()).RowsRemoved);

        Assert.Equal(125L, await ScalarAsync("SELECT execution_count FROM query_store_stats WHERE query_id = 1"));
        Assert.Equal(1871L, await ScalarAsync("SELECT avg_duration_us FROM query_store_stats WHERE query_id = 1"));
        Assert.Equal(1L, await ScalarAsync("SELECT COUNT(*) FROM query_store_stats WHERE query_id = 1"));

        /* Untouched, values intact. */
        Assert.Equal(1L, await ScalarAsync("SELECT COUNT(*) FROM query_store_stats WHERE query_id = 2"));
        Assert.Equal(55L, await ScalarAsync("SELECT execution_count FROM query_store_stats WHERE query_id = 2"));

        /* Idempotent: the signature cannot match what the repair produced. */
        var second = await service.SurveyAsync();
        Assert.False(second.HasWork);
        Assert.Equal(0, (await service.RepairAsync()).RowsRemoved);
    }

    /// <summary>
    /// The archive rewrite, on an OLD-ERA file — no <c>runtime_stats_interval_id</c>, no <c>replica_role</c>,
    /// exactly the shape the real June 2026 archive has. The era-appropriate key has to engage or the rewrite
    /// either fails outright or groups on the wrong thing.
    /// </summary>
    [Fact]
    public async Task Archive_RewritesAnOldEraFile_OnTheKeyThatEraActuallyHad()
    {
        var file = Path.Combine(_archivePath, $"202605_query_store_stats.parquet");
        await WriteLegacyArchiveAsync(file);

        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath);

        var survey = await service.SurveyAsync();
        var archived = Assert.Single(survey.Archive);
        Assert.False(archived.HasIntervalId);
        Assert.Equal(3, archived.Rows);
        Assert.Equal(1, archived.Groups);
        Assert.Equal(2, archived.SplitRows);
        Assert.Equal(1, archived.RowsRemoved);

        Assert.Equal(1, (await service.RepairAsync()).RowsRemoved);

        /* The split pair became one row carrying the summed count and the weighted mean; the third row, a
           different interval, is still there untouched. */
        var rows = await QueryArchiveAsync(file, "SELECT query_id, execution_count, avg_duration_us FROM read_parquet('{0}') ORDER BY query_id");
        Assert.Equal(2, rows.Count);
        Assert.Equal([1L, 125L, 1871L], rows[0]);
        Assert.Equal([2L, 55L, 500L], rows[1]);

        /* Still readable as parquet, and the file survived the swap. */
        Assert.True(File.Exists(file));
        Assert.Empty(Directory.GetFiles(_archivePath, "*.repair-tmp"));
    }

    /// <summary>
    /// A file that fails verification must leave the ORIGINAL intact — no backup copy is kept, so
    /// verify-before-promote IS the safety.
    ///
    /// <para>The failure is induced honestly rather than by mocking: a second archive file is left holding a
    /// column set the projection cannot combine, so its rewrite throws mid-run. The assertions are that the
    /// good file was still repaired, the bad file is byte-identical to what it was, and no temp file is left
    /// behind. Startup logs the failure and retries next launch; until then the union_by_name view keeps
    /// reading the un-rewritten file with #1907's read-side tie-break resolving it deterministically, so a
    /// half-repaired archive is a delay rather than a corruption.</para>
    /// </summary>
    [Fact]
    public async Task ArchiveRewrite_ThatFailsVerification_LeavesTheOriginalUntouched()
    {
        var good = Path.Combine(_archivePath, "202605_query_store_stats.parquet");
        await WriteLegacyArchiveAsync(good);

        /* A file whose execution_count is TEXT: the weighted-mean expression cannot combine it, so the COPY
           throws — a real failure of the real code path, not a stubbed one. */
        var bad = Path.Combine(_archivePath, "202604_query_store_stats.parquet");
        await WriteUncombinableArchiveAsync(bad);

        var badBefore = await File.ReadAllBytesAsync(bad);
        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath);

        var result = await service.RepairAsync();

        /* Per-FILE failure, not a thrown run: the bad file is reported, the good one is still repaired. */
        Assert.False(result.FullyRepaired);
        var failure = Assert.Single(result.Failures);
        Assert.Contains("202604_query_store_stats.parquet", failure, StringComparison.Ordinal);

        /* The bad file is exactly as it was — same bytes, not merely same length. */
        Assert.Equal(badBefore, await File.ReadAllBytesAsync(bad));

        /* No temp file survives a failure. */
        Assert.Empty(Directory.GetFiles(_archivePath, "*.repair-tmp"));

        /* And the file that CAN be repaired was, since the run processes files independently. */
        var rows = await QueryArchiveAsync(good, "SELECT query_id, execution_count FROM read_parquet('{0}') ORDER BY query_id");
        Assert.Equal(2, rows.Count);
        Assert.Equal([1L, 125L], rows[0]);
    }

    /// <summary>
    /// The startup contract: repair once, record it, do not repeat — and on a PARTIAL repair withhold the
    /// marker so the next launch retries.
    ///
    /// <para>The marker is a table of its own rather than a schema-version bump, and the retry requirement is
    /// exactly why: <c>RunMigrationsAsync</c> drops and recreates tables per version step, so a data repair
    /// cannot live there, and the schema version cannot be withheld to force a retry without also re-running
    /// those drops.</para>
    /// </summary>
    [Fact]
    public async Task StartupRepair_RunsOnce_RecordsAMarker_AndWithholdsItWhenAFileCouldNotBeRepaired()
    {
        var t = new DateTime(2026, 6, 13, 11, 0, 0, DateTimeKind.Unspecified);
        await SeedAsync(t, queryId: 1, planId: 11, intervalId: 6001, executionCount: 100, avgDurationUs: 1778);
        await SeedAsync(t, queryId: 1, planId: 11, intervalId: 6001, executionCount: 25, avgDurationUs: 2245);

        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath);
        Assert.False(await service.AlreadyRepairedAsync());

        await service.RepairOnStartupAsync();

        Assert.True(await service.AlreadyRepairedAsync());
        Assert.Equal(125L, await ScalarAsync("SELECT execution_count FROM query_store_stats WHERE query_id = 1"));

        /* A second startup is a no-op — it does not even survey, because the marker short-circuits it. */
        await SeedAsync(t.AddMinutes(5), queryId: 3, planId: 33, intervalId: 6003, executionCount: 7, avgDurationUs: 100);
        await SeedAsync(t.AddMinutes(5), queryId: 3, planId: 33, intervalId: 6003, executionCount: 3, avgDurationUs: 200);
        await service.RepairOnStartupAsync();
        Assert.Equal(2L, await ScalarAsync("SELECT COUNT(*) FROM query_store_stats WHERE query_id = 3"));
    }

    /// <summary>A partial repair must NOT record the marker, so the next launch tries the failed file again.</summary>
    [Fact]
    public async Task StartupRepair_WithAnUnrepairableFile_LeavesNoMarker_SoItRetries()
    {
        await WriteUncombinableArchiveAsync(Path.Combine(_archivePath, "202604_query_store_stats.parquet"));

        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath);
        await service.RepairOnStartupAsync();

        Assert.False(await service.AlreadyRepairedAsync());
    }

    /// <summary>A store with nothing to repair records the marker anyway, so the survey stops running forever.</summary>
    [Fact]
    public async Task StartupRepair_OnACleanStore_RecordsTheMarkerSoItDoesNotSurveyEveryLaunch()
    {
        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath);

        Assert.False(await service.AlreadyRepairedAsync());
        await service.RepairOnStartupAsync();
        Assert.True(await service.AlreadyRepairedAsync());
    }

    /// <summary>
    /// Source guard: every phase of the repair that MUTATES must sit inside a write-lock scope.
    ///
    /// <para>The read lock permits concurrent readers by design — that is what it is for — and this service is
    /// fired un-awaited at startup precisely so the UI stays interactive, so a UI reader genuinely can be
    /// mid-<c>read_parquet</c> while the repair runs. Mutating under the READ lock therefore races it: DuckDB
    /// answers "Reached the end of the file" / "No magic bytes found at end of file" to a reader whose file
    /// moved underneath it. <c>ArchiveService</c> already documents this exact hazard on its own purge, which
    /// is the idiom mirrored here.</para>
    ///
    /// <para>A source guard rather than a behavioral test because the failure needs a reader racing a writer at
    /// the wrong instant — reproducible in the field, miserable to force deterministically in a test. What CAN
    /// be pinned is the structure: the hot-table collapse and the file promotion are inside
    /// <c>AcquireWriteLock</c>, the rewrite-to-temp is not, and nobody has quietly wrapped the whole run in one
    /// write lock (which would freeze the UI for a large archive repair when only the swap instants need it).</para>
    /// </summary>
    [Fact]
    public void EveryMutatingPhase_SitsInsideAWriteLockScope()
    {
        var source = File.ReadAllText(SourcePath("Lite", "Services", "QueryStoreSliceRepairService.cs"));

        /* Exactly two write-lock scopes: the hot-table collapse, and the per-file promotion. */
        var writeLocks = Regex.Matches(source, @"_duckDb\.AcquireWriteLock\(\)").Count;
        Assert.Equal(2, writeLocks);

        /* The hot collapse's DELETE/INSERT must follow a write-lock acquisition, not a read-lock one. */
        var hotLock = source.IndexOf("using var writeLock = _duckDb.AcquireWriteLock();", StringComparison.Ordinal);
        var hotDelete = source.IndexOf($"DELETE FROM {{Table}} AS t WHERE EXISTS", StringComparison.Ordinal);
        Assert.True(hotLock > 0, "the hot-table collapse must take the write lock");
        Assert.True(hotDelete > hotLock, "the hot-table DELETE must sit inside the write-lock scope");

        /* The promotion — the only place a live path's bytes are replaced — is called from inside a write
           lock, and the swap itself never appears outside that helper. */
        Assert.Contains("using (_duckDb.AcquireWriteLock())", source, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(source, @"await PromoteRewrittenFileAsync\("));
        Assert.Single(Regex.Matches(source, @"File\.Move\("));

        /* And the REWRITE stays under the read lock: writing a temp sibling nothing can see yet must not take
           the UI's exclusivity for the whole of a large archive rewrite. */
        Assert.Contains("using (_duckDb.AcquireReadLock(cancellationToken))", source, StringComparison.Ordinal);
        var readLock = source.IndexOf("using (_duckDb.AcquireReadLock(cancellationToken))", StringComparison.Ordinal);
        var copyToTemp = source.IndexOf("COMPRESSION ZSTD)\";", StringComparison.Ordinal);
        Assert.True(copyToTemp > readLock, "the rewrite-to-temp belongs under the read lock, not the write lock");
    }

    /// <summary>
    /// Source guard: no lock acquisition in this service is silent about cancellation (#2465).
    ///
    /// <para>The sibling test above pins WHICH lock each phase takes. This one pins whether the phase can be
    /// ABANDONED while it waits for it, which is a separate property and the one CA2016 was pointing at: every
    /// method here holds a token, and every one of them takes the lock BEFORE it opens its connection, so a
    /// no-arg <c>AcquireReadLock()</c> leaves the token stopped at the door — abandonable everywhere except
    /// where the pass is actually stuck, which is the exact defect #2454 closed for the analysis pass.</para>
    ///
    /// <para>A source guard rather than a behavioral one because what is being pinned is a DECISION, not a
    /// behavior: three sites forward the token and two decline it, and on the shipped caller
    /// (<c>MainWindow</c> fires the repair un-awaited with no token) both spellings run identically today.
    /// Nothing observable separates them, and that is precisely why they need pinning — a later edit that
    /// "tidied" the two declines into forwards, or blanket-suppressed the rule, would cost nothing at runtime
    /// and quietly convert five stated decisions back into an unknown.</para>
    ///
    /// <para>The declines are pinned WITH their reason, because a bare <c>CancellationToken.None</c> is the
    /// oversight the analyzer complained about with an extra token typed in.</para>
    /// </summary>
    [Fact]
    public void EveryLockAcquisition_SaysWhetherItCanBeAbandoned()
    {
        var source = File.ReadAllText(SourcePath("Lite", "Services", "QueryStoreSliceRepairService.cs"));

        /* Not one silent acquisition left. This is the assertion that goes red on the pre-#2465 file. */
        Assert.Empty(Regex.Matches(source, @"_duckDb\.AcquireReadLock\(\)"));

        /* Three FORWARD it: the marker read, the survey, and the archive rewrite-to-temp. Each abandons into
           a state the next launch reproduces for free, so there is nothing to protect by waiting. */
        Assert.Equal(3, Regex.Matches(source, @"_duckDb\.AcquireReadLock\(cancellationToken\)").Count);

        /* Two DECLINE it, and both are the marker write — the only thing here that records work already done
           and irreversible, where abandoning costs the next launch the whole survey to learn nothing. */
        var declined = Regex.Matches(source, @"_duckDb\.AcquireReadLock\(CancellationToken\.None\)");
        Assert.Equal(2, declined.Count);

        foreach (Match site in declined)
        {
            var reason = source[Math.Max(0, site.Index - 1500)..site.Index];
            Assert.Contains("#2465", reason, StringComparison.Ordinal);
            Assert.Contains("marker", reason, StringComparison.Ordinal);
        }

        /* And each decline is WHOLE. A lock that will not be abandoned in front of a write that will be is
           worse than either choice made consistently, so the open and both statements decline too. */
        Assert.Equal(2, Regex.Matches(source, @"await connection\.OpenAsync\(CancellationToken\.None\);").Count);
        Assert.Equal(2, Regex.Matches(source, @"await MarkRepairedAsync\(connection, [^;]*CancellationToken\.None\);").Count);

        /* The write locks stay out of this: AcquireWriteLock has no token-taking overload, so there is no
           decision to state at those two. Their timeout question is #2463's, not this test's. */
        Assert.Equal(2, Regex.Matches(source, @"_duckDb\.AcquireWriteLock\(\)").Count);
    }

    /* ─────────────────────────── helpers ─────────────────────────── */

    /// <summary>Walks up from the test binary to the repo root so the pin works from any run directory.</summary>
    private static string SourcePath(params string[] parts)
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.True(dir is not null, "could not locate the repository root from " + AppContext.BaseDirectory);
        return Path.Combine([dir!, .. parts]);
    }


    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seed is null)
        {
            _seed = _duckDb.CreateConnection();
            await _seed.OpenAsync();
        }
        return _seed;
    }

    private async Task SeedAsync(
        DateTime collectionTime, long queryId, long planId, long? intervalId, long executionCount, long avgDurationUs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_duration_us, min_duration_us, max_duration_us,
     query_plan_hash, is_forced_plan, force_failure_count, runtime_stats_interval_id)
VALUES ($1, $2, $3, 'SRV', 'DB', $4, $5, 'Regular', $2, $2, 'SELECT 1', '0xH', $6, $7, 998, 4708, '0xP', false, 0, $8)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = planId });
        cmd.Parameters.Add(new DuckDBParameter { Value = executionCount });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgDurationUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)intervalId ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Writes a parquet file in the PRE-tier-2 shape: no runtime_stats_interval_id, no replica_role. Built by
    /// listing the columns explicitly rather than by copying the current table, so the fixture cannot silently
    /// acquire whatever columns the schema grows next — the whole point is that it is an OLD file.
    /// </summary>
    private async Task WriteLegacyArchiveAsync(string file)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
COPY (
    SELECT * FROM (VALUES
        (1::BIGINT, TIMESTAMP '2026-05-10 10:00:00', {ServerId}::INTEGER, 'SRV', 'DB', 1::BIGINT, 11::BIGINT, 'Regular',
         TIMESTAMP '2026-05-10 09:55:00', TIMESTAMP '2026-05-10 10:00:00', 'SELECT 1', '0xH', 100::BIGINT, 1778::BIGINT, 998::BIGINT, 4708::BIGINT),
        (2::BIGINT, TIMESTAMP '2026-05-10 10:00:00', {ServerId}::INTEGER, 'SRV', 'DB', 1::BIGINT, 11::BIGINT, 'Regular',
         TIMESTAMP '2026-05-10 09:55:00', TIMESTAMP '2026-05-10 10:00:00', 'SELECT 1', '0xH', 25::BIGINT, 2245::BIGINT, 998::BIGINT, 5100::BIGINT),
        (3::BIGINT, TIMESTAMP '2026-05-10 10:05:00', {ServerId}::INTEGER, 'SRV', 'DB', 2::BIGINT, 22::BIGINT, 'Regular',
         TIMESTAMP '2026-05-10 10:01:00', TIMESTAMP '2026-05-10 10:05:00', 'SELECT 2', '0xH2', 55::BIGINT, 500::BIGINT, 400::BIGINT, 600::BIGINT)
    ) AS t(collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
           execution_type_desc, first_execution_time, last_execution_time, query_text, query_hash,
           execution_count, avg_duration_us, min_duration_us, max_duration_us)
) TO '{file.Replace("'", "''", StringComparison.Ordinal)}' (FORMAT PARQUET, COMPRESSION ZSTD)";
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// An archive file the collapse cannot combine: <c>execution_count</c> is TEXT, so the weighted-mean
    /// expression fails on it. It still carries the split signature, so the run reaches the rewrite and
    /// throws there — which is the point, since a file that simply had no work would never be touched.
    /// </summary>
    private async Task WriteUncombinableArchiveAsync(string file)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
COPY (
    SELECT * FROM (VALUES
        (1::BIGINT, TIMESTAMP '2026-04-10 10:00:00', {ServerId}::INTEGER, 'DB', 1::BIGINT, 11::BIGINT, 'Regular',
         TIMESTAMP '2026-04-10 09:55:00', 'not-a-number', 1778::BIGINT),
        (2::BIGINT, TIMESTAMP '2026-04-10 10:00:00', {ServerId}::INTEGER, 'DB', 1::BIGINT, 11::BIGINT, 'Regular',
         TIMESTAMP '2026-04-10 09:55:00', 'also-not', 2245::BIGINT)
    ) AS t(collection_id, collection_time, server_id, database_name, query_id, plan_id,
           execution_type_desc, first_execution_time, execution_count, avg_duration_us)
) TO '{file.Replace("'", "''", StringComparison.Ordinal)}' (FORMAT PARQUET, COMPRESSION ZSTD)";
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Reads the archive on a FRESH connection, deliberately.
    ///
    /// <para>DuckDB caches a parquet file's state against its PATH at INSTANCE scope, so once a file has been
    /// read, replacing the bytes at that path makes later reads fail with "No magic bytes found at end of
    /// file". A new connection from the SAME store does not escape it — that is the trap, and it is why the
    /// service flushes <c>enable_external_file_cache</c> and rebuilds the archive views after rewriting. This
    /// reader still avoids the seeding connection so a failure here points at the repair rather than at a
    /// connection the test itself poisoned.</para>
    /// </summary>
    private async Task<List<object[]>> QueryArchiveAsync(string file, string sqlTemplate)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = string.Format(
            System.Globalization.CultureInfo.InvariantCulture,
            sqlTemplate,
            file.Replace("'", "''", StringComparison.Ordinal));

        var rows = new List<object[]>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var values = new object[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                values[i] = Convert.ToInt64(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture);
            }
            rows.Add(values);
        }
        return rows;
    }

    private async Task<long> ScalarAsync(string sql)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        var value = await cmd.ExecuteScalarAsync();
        return value is null or DBNull ? 0L : Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture);
    }
}
