/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Investigations;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The upgrade every Lite 3.13.0 user takes: a store 3.13.0 built (schema v81, its tables, indexes, views and Parquet archive) opened by
/// this build. <c>Fixtures/StoreV81/lite-3.13.0-v81.sql</c> is that store as a script, captured from a store 3.13.0's own
/// DuckDbInitializer, ArchiveService and RetentionService built over three servers and ten days of rows: its DDL, two rows per table,
/// one row per archive file (rebuilt here with COPY, so each file carries v81's columns) and its views. The tests climb a copy to
/// <see cref="DuckDbInitializer.CurrentSchemaVersion"/> and hold it to a fresh store of this build loaded with the same rows:
/// the same tables, columns in the same order, indexes and views, with neither store holding an object the other lacks; no migration
/// step logged as non-fatal (#2748); every row kept; every
/// LocalDataService read and every MCP read tool that answers on the fresh store answering on the upgraded one with the same row
/// counts (for an MCP tool, the elements of every array in its answer, summed); and the housekeeping passes (archival, retention,
/// the purges, the size reset) leaving the two stores alike.
/// <para>Set <c>LITE_UPGRADE_STORE_DIR</c> to a folder holding a real store (monitor.duckdb, archive, config) to run the same checks
/// on a copy of it; <c>LITE_UPGRADE_REPORT</c> names a file for the full report.</para>
/// </summary>
public sealed class Lite313StoreUpgradeTests : IDisposable
{
    internal static readonly string[] FixtureServerNames = { "LAB-SQL01", "LAB-SQL02\\INST1", "lab-sql03.example.local" };

    /* Tables the initializer fills itself: they are compared, never copied into the fresh store. */
    private static readonly HashSet<string> InitializerTables = new(StringComparer.OrdinalIgnoreCase)
    {
        "schema_version", "analysis_schema_version", "store_identity",
    };

    private readonly string _root;

    public Lite313StoreUpgradeTests()
    {
        _root = Path.Combine(Path.GetTempPath(), "LiteTests_v81_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_root);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task A313Store_ClimbsToTheCurrentSchema_WithEveryTableColumnAndRow_AndNoNonFatalStep()
    {
        var run = await RunAsync(FromFixtureAsync, reads: false, housekeeping: false);
        Assert.Equal(81, run.VersionBefore);
        Assert.True(run.Climb.Count == 0, Report("the climb", run.Climb, run));
    }

    [Fact]
    public async Task AnUpgraded313Store_AnswersEveryTabAndMcpRead_LikeAFreshStoreOfTheSameRows()
    {
        var run = await RunAsync(FromFixtureAsync, reads: true, housekeeping: false);
        Assert.True(run.ReadsSwept > 250, $"only {run.ReadsSwept} reads were swept: the reflection under-enumerated LocalDataService and the MCP tools");
        Assert.True(run.ReadsWithRows > 40, $"only {run.ReadsWithRows} reads returned rows on the fresh store: the seeded rows are not visible to the reads");
        Assert.True(run.McpReadsWithRows > 20, $"only {run.McpReadsWithRows} MCP tools answered rows on the fresh store: the row count reads nothing, or the seeded rows are not visible to the tools");
        Assert.True(run.Reads.Count == 0, Report("the reads", run.Reads, run));
    }

    [Fact]
    public async Task AnUpgraded313Store_RunsTheHousekeepingPasses_LikeAFreshStoreOfTheSameRows()
    {
        var run = await RunAsync(FromFixtureAsync, reads: false, housekeeping: true);
        Assert.True(run.Housekeeping.Count == 0, Report("the housekeeping passes", run.Housekeeping, run));
    }

    [Fact]
    public async Task ARealStoreFolder_ClimbsTheSameWay()
    {
        var source = Environment.GetEnvironmentVariable("LITE_UPGRADE_STORE_DIR");
        Assert.SkipWhen(string.IsNullOrEmpty(source), "LITE_UPGRADE_STORE_DIR is not set");
        var run = await RunAsync(dir => FromFolderAsync(source!, dir), reads: true, housekeeping: true);
        var reportPath = Environment.GetEnvironmentVariable("LITE_UPGRADE_REPORT");
        if (!string.IsNullOrEmpty(reportPath)) File.WriteAllText(reportPath, run.Details.ToString());
        var all = run.Climb.Concat(run.Reads).Concat(run.Housekeeping).ToList();
        Assert.True(all.Count == 0, Report("the real store", all, run));
    }

    private static string Report(string what, List<string> findings, UpgradeRun run) =>
        $"{findings.Count} finding(s) in {what}:" + Environment.NewLine + string.Join(Environment.NewLine, findings) +
        Environment.NewLine + Environment.NewLine + run.Details;

    /* ═══════════════════════════ the run ═══════════════════════════ */

    private sealed class UpgradeRun
    {
        public int VersionBefore;
        public readonly List<string> Climb = new();
        public readonly List<string> Reads = new();
        public readonly List<string> Housekeeping = new();
        public readonly StringBuilder Details = new();
        public int ReadsSwept;
        public int ReadsWithRows;
        public int McpReadsWithRows;
    }

    private sealed record Store(string Dir, string DbPath, string ArchiveDir, string ConfigDir);

    private async Task<UpgradeRun> RunAsync(Func<string, Task<Store>> make, bool reads, bool housekeeping)
    {
        var run = new UpgradeRun();
        var v81 = await make(Path.Combine(_root, "v81"));
        var before = await SnapshotAsync(v81.DbPath, views: false);
        run.VersionBefore = (int)before.Version;
        run.Details.AppendLine($"source: schema v{before.Version}, {before.Tables.Count} tables, {before.Tables.Values.Sum()} rows, " +
            $"{Directory.GetFiles(v81.ArchiveDir, "*.parquet", SearchOption.AllDirectories).Length} archive files");

        var upgraded = CopyStore(v81, Path.Combine(_root, "upgraded"));
        var upgradedLog = new List<(LogLevel Level, string Message)>();
        using var upgradedDb = new DuckDbInitializer(upgraded.DbPath, new CapturingLogger<DuckDbInitializer>(upgradedLog));
        try
        {
            await upgradedDb.InitializeAsync();
        }
        catch (Exception ex)
        {
            run.Climb.Add($"InitializeAsync threw {ex.GetType().Name}: {ex.Message}");
            return run;
        }
        run.Details.AppendLine($"climb: v{upgradedDb.StartupSchemaBefore} -> v{upgradedDb.StartupSchemaAfter} (migration steps {upgradedDb.StartupMigrationMs} ms)");
        foreach (var (level, message) in upgradedLog.Where(l => l.Level >= LogLevel.Information && l.Message.Contains("migration", StringComparison.OrdinalIgnoreCase)))
            run.Details.AppendLine($"  log {level}: {message}");

        var fresh = await FreshOfTheSameRowsAsync(v81, Path.Combine(_root, "fresh"));
        var freshLog = new List<(LogLevel Level, string Message)>();
        using var freshDb = new DuckDbInitializer(fresh.DbPath, new CapturingLogger<DuckDbInitializer>(freshLog));
        await freshDb.InitializeAsync();

        /* The climb: the stamp, the log, the shape, the rows. */
        var after = await SnapshotAsync(upgradedDb, views: true);
        var freshShape = await SnapshotAsync(freshDb, views: true);
        if (after.Version != DuckDbInitializer.CurrentSchemaVersion)
            run.Climb.Add($"schema_version is {after.Version} after the climb, not {DuckDbInitializer.CurrentSchemaVersion}");
        foreach (var (level, message) in upgradedLog.Where(l => l.Level >= LogLevel.Warning))
            run.Climb.Add($"initializer logged {level}: {message}");
        foreach (var (table, columns) in freshShape.Columns)
        {
            if (after.Columns.TryGetValue(table, out var have) && !have.SequenceEqual(columns))
                run.Climb.Add($"table {table} columns differ: upgraded [{string.Join(", ", have)}] fresh [{string.Join(", ", columns)}]");
        }
        /* Both directions: an object the upgraded store lacks is a migration that did not run, and one it holds that a fresh store does
           not is a v81 table, index or view the climb should have dropped or renamed. */
        foreach (var table in freshShape.Columns.Keys.Except(after.Columns.Keys, StringComparer.OrdinalIgnoreCase))
            run.Climb.Add($"table {table} is missing from the upgraded store");
        foreach (var table in after.Columns.Keys.Except(freshShape.Columns.Keys, StringComparer.OrdinalIgnoreCase))
            run.Climb.Add($"table {table} is on the upgraded store and not on a fresh one");
        foreach (var index in freshShape.Indexes.Except(after.Indexes, StringComparer.OrdinalIgnoreCase))
            run.Climb.Add($"index {index} is missing from the upgraded store");
        foreach (var index in after.Indexes.Except(freshShape.Indexes, StringComparer.OrdinalIgnoreCase))
            run.Climb.Add($"index {index} is on the upgraded store and not on a fresh one");
        foreach (var view in freshShape.Views.Keys.Except(after.Views.Keys, StringComparer.OrdinalIgnoreCase))
            run.Climb.Add($"view {view} is missing from the upgraded store");
        foreach (var view in after.Views.Keys.Except(freshShape.Views.Keys, StringComparer.OrdinalIgnoreCase))
            run.Climb.Add($"view {view} is on the upgraded store and not on a fresh one");
        foreach (var (table, rows) in before.Tables)
        {
            if (InitializerTables.Contains(table)) continue;
            var now = after.Tables.TryGetValue(table, out var n) ? n : -1;
            if (now != rows) run.Climb.Add($"table {table} held {rows} rows before the climb and {now} after");
        }
        CompareViews(run.Climb, "after the climb", after.Views, freshShape.Views);
        run.Details.AppendLine($"upgraded: {after.Tables.Count} tables, {after.Views.Count} views, {after.Views.Values.Where(v => v.Rows >= 0).Sum(v => v.Rows)} view rows");

        if (reads)
        {
            var serverIds = FixtureServerNames.Select(StorageId).ToArray();
            var upgradedReads = await SweepAsync(upgradedDb, upgraded.ConfigDir, serverIds);
            var freshReads = await SweepAsync(freshDb, fresh.ConfigDir, serverIds);
            run.ReadsSwept = freshReads.Count;
            run.ReadsWithRows = freshReads.Values.Count(o => o.Rows > 0);
            run.McpReadsWithRows = freshReads.Count(o => o.Key.StartsWith("mcp.", StringComparison.Ordinal) && o.Value.Rows > 0);
            foreach (var (key, f) in freshReads.OrderBy(k => k.Key, StringComparer.Ordinal))
            {
                var u = upgradedReads.TryGetValue(key, out var x) ? x : new ReadOutcome("not swept", -1);
                if (f.Status == "ok" && u.Status != "ok")
                    run.Reads.Add($"{key}: {u.Status} on the upgraded store, answers on the fresh one");
                else if (f.Status == "ok" && f.Rows != u.Rows)
                    run.Reads.Add($"{key}: {u.Rows} rows on the upgraded store, {f.Rows} on the fresh one");
            }
            var common = freshReads.Where(f => f.Value.Status != "ok").Select(f => $"{f.Key}: {f.Value.Status}").ToList();
            run.Details.AppendLine($"reads: {freshReads.Count} swept, {run.ReadsWithRows} with rows on the fresh store ({run.McpReadsWithRows} of them MCP tools), {common.Count} failing on both stores (not an upgrade finding)");
            foreach (var c in common) run.Details.AppendLine("  both: " + c);
        }

        if (housekeeping)
        {
            var u = await HousekeepAsync(upgradedDb, upgraded);
            var f = await HousekeepAsync(freshDb, fresh);
            var previous = after.Views;
            for (var i = 0; i < f.Count; i++)
            {
                var (step, freshOutcome, freshViews, freshWarnings) = f[i];
                var (_, upgradedOutcome, upgradedViews, upgradedWarnings) = u[i];
                run.Details.AppendLine($"housekeeping {step}: upgraded {upgradedOutcome}, fresh {freshOutcome}, " +
                    $"{Directory.GetFiles(upgraded.ArchiveDir, "*.parquet", SearchOption.AllDirectories).Length} archive files after it");
                foreach (var w in upgradedWarnings.Intersect(freshWarnings))
                    run.Details.AppendLine($"  logged on both stores: {w}");
                if (upgradedOutcome != freshOutcome)
                    run.Housekeeping.Add($"{step}: {upgradedOutcome} on the upgraded store, {freshOutcome} on the fresh one");
                foreach (var w in upgradedWarnings.Except(freshWarnings))
                    run.Housekeeping.Add($"{step}: logged on the upgraded store only: {w}");
                CompareViews(run.Housekeeping, "after " + step, upgradedViews, freshViews);
                /* Archival and the size reset move rows between the tables and the archive, v81 files beside the new ones: every
                   archive view reads as many rows after the pass as before it. */
                if (step is "archival" or "size_reset")
                {
                    /* The v_ views only: a vh_ view reads the hot rows alone, and v_config_alert_log hides an archived alert that
                       dismissed_archive_alerts lists, so both read fewer rows once a pass moves rows to the archive. */
                    foreach (var (view, was) in previous.Where(v => v.Key.StartsWith("v_", StringComparison.Ordinal) && v.Key != "v_config_alert_log"))
                    {
                        var now = upgradedViews.TryGetValue(view, out var v) ? v : (-1, "missing");
                        if (now.Rows != was.Rows)
                            run.Housekeeping.Add($"{step}: {view} read {was.Rows} rows before the pass and {now.Rows} after it on the upgraded store{(now.Error is null ? "" : ": " + now.Error)}");
                    }
                }
                previous = upgradedViews;
            }
        }

        return run;
    }

    private static void CompareViews(List<string> findings, string when, Dictionary<string, (long Rows, string? Error)> upgraded, Dictionary<string, (long Rows, string? Error)> fresh)
    {
        foreach (var (view, f) in fresh)
        {
            if (!upgraded.TryGetValue(view, out var u)) continue;
            if (u.Error != null && f.Error == null) findings.Add($"{when}: SELECT from {view} threw on the upgraded store: {u.Error}");
            else if (u.Rows != f.Rows) findings.Add($"{when}: {view} reads {u.Rows} rows on the upgraded store, {f.Rows} on the fresh one");
        }
    }

    /* ═══════════════════════════ the stores ═══════════════════════════ */

    private static string FixturePath => Path.Combine(AppContext.BaseDirectory, "Fixtures", "StoreV81", "lite-3.13.0-v81.sql");

    /// <summary>The fixture's store: its timestamps placed against the test clock, its archive files rebuilt with COPY, and the
    /// three servers in a servers.json, the way 3.13.0 left them.</summary>
    private static async Task<Store> FromFixtureAsync(string dir)
    {
        var store = NewStore(dir);
        var now = DateTime.UtcNow.AddMinutes(-5);
        now = new DateTime(now.Ticks - now.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Unspecified);
        var sql = File.ReadAllText(FixturePath);
        sql = Regex.Replace(sql, @"'\{T:(-?[0-9.]+)\}'", m =>
            "TIMESTAMP '" + now.AddSeconds(double.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture)).ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "'");
        sql = sql.Replace("{ARCHIVE}", store.ArchiveDir.Replace('\\', '/'), StringComparison.Ordinal)
                 .Replace("{MONTH}", now.AddDays(-5).ToString("yyyyMM", CultureInfo.InvariantCulture), StringComparison.Ordinal)
                 .Replace("{DAY}", now.AddDays(-5).ToString("yyyyMMdd", CultureInfo.InvariantCulture), StringComparison.Ordinal);
        using (var conn = new DuckDBConnection($"Data Source={store.DbPath}"))
        {
            await conn.OpenAsync();
            foreach (var line in sql.Split('\n'))
            {
                var statement = line.Trim();
                if (statement.Length == 0 || statement.StartsWith("--", StringComparison.Ordinal)) continue;
                await ExecAsync(conn, statement);
            }
        }

        var servers = new ServerManager(store.ConfigDir);
        foreach (var name in FixtureServerNames)
            servers.AddServer(new ServerConnection { ServerName = name, DisplayName = name });
        return store;
    }

    private static Task<Store> FromFolderAsync(string source, string dir)
    {
        CopyTree(source, dir);
        var store = new Store(dir, Path.Combine(dir, "monitor.duckdb"), Path.Combine(dir, "archive"), Path.Combine(dir, "config"));
        Directory.CreateDirectory(store.ArchiveDir);
        Directory.CreateDirectory(store.ConfigDir);
        return Task.FromResult(store);
    }

    private static Store NewStore(string dir)
    {
        var store = new Store(dir, Path.Combine(dir, "monitor.duckdb"), Path.Combine(dir, "archive"), Path.Combine(dir, "config"));
        Directory.CreateDirectory(store.ArchiveDir);
        Directory.CreateDirectory(store.ConfigDir);
        return store;
    }

    private static Store CopyStore(Store from, string dir)
    {
        CopyTree(from.Dir, dir);
        return new Store(dir, Path.Combine(dir, "monitor.duckdb"), Path.Combine(dir, "archive"), Path.Combine(dir, "config"));
    }

    /// <summary>A store this build creates, then loaded with the v81 store's rows by column name, and given the same archive
    /// files and config: what the upgraded store should read like.</summary>
    private static async Task<Store> FreshOfTheSameRowsAsync(Store v81, string dir)
    {
        var store = NewStore(dir);
        CopyTree(v81.ArchiveDir, store.ArchiveDir);
        CopyTree(v81.ConfigDir, store.ConfigDir);
        using (var init = new DuckDbInitializer(store.DbPath))
        {
            await init.InitializeAsync();
        }

        using var conn = new DuckDBConnection($"Data Source={store.DbPath}");
        await conn.OpenAsync();
        await ExecAsync(conn, $"ATTACH '{v81.DbPath.Replace("'", "''")}' AS old (READ_ONLY)");
        var tables = await QueryAsync(conn, "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'old' AND table_schema = 'main' AND table_type = 'BASE TABLE'");
        var current = (await QueryAsync(conn, "SELECT table_name FROM information_schema.tables WHERE table_catalog = current_database() AND table_schema = 'main' AND table_type = 'BASE TABLE'"))
            .Select(r => (string)r[0]!).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var table in tables.Select(r => (string)r[0]!))
        {
            if (InitializerTables.Contains(table) || !current.Contains(table)) continue;
            var common = string.Join(", ", (await QueryAsync(conn,
                    $"SELECT o.column_name FROM information_schema.columns o JOIN information_schema.columns n ON n.table_name = o.table_name AND n.column_name = o.column_name " +
                    $"AND n.table_catalog = current_database() AND n.table_schema = 'main' WHERE o.table_catalog = 'old' AND o.table_schema = 'main' AND o.table_name = '{table}' ORDER BY o.ordinal_position"))
                .Select(r => $"\"{r[0]}\""));
            await ExecAsync(conn, $"DELETE FROM main.\"{table}\"");
            await ExecAsync(conn, $"INSERT INTO main.\"{table}\" ({common}) SELECT {common} FROM old.main.\"{table}\"");
        }
        await ExecAsync(conn, "DETACH old");
        return store;
    }

    private sealed record Snapshot(long Version, Dictionary<string, long> Tables, Dictionary<string, List<string>> Columns, HashSet<string> Indexes,
        Dictionary<string, (long Rows, string? Error)> Views);

    private static async Task<Snapshot> SnapshotAsync(string dbPath, bool views)
    {
        using var conn = new DuckDBConnection($"Data Source={dbPath}");
        await conn.OpenAsync();
        return await SnapshotAsync(conn, views);
    }

    private static async Task<Snapshot> SnapshotAsync(DuckDbInitializer db, bool views)
    {
        using var readLock = db.AcquireReadLock();
        using var conn = db.CreateConnection();
        await conn.OpenAsync();
        return await SnapshotAsync(conn, views);
    }

    private static async Task<Snapshot> SnapshotAsync(DuckDBConnection conn, bool views)
    {
        var version = Convert.ToInt64(await ScalarAsync(conn, "SELECT COALESCE(MAX(version), 0) FROM schema_version"), CultureInfo.InvariantCulture);
        var tables = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        foreach (var r in await QueryAsync(conn, "SELECT table_name FROM information_schema.tables WHERE table_catalog = current_database() AND table_schema = 'main' AND table_type = 'BASE TABLE'"))
            tables[(string)r[0]!] = Convert.ToInt64(await ScalarAsync(conn, $"SELECT COUNT(*) FROM \"{r[0]}\""), CultureInfo.InvariantCulture);
        var columns = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var r in await QueryAsync(conn, "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns c WHERE table_catalog = current_database() AND table_schema = 'main' " +
            "AND table_name IN (SELECT table_name FROM information_schema.tables t WHERE t.table_catalog = current_database() AND t.table_type = 'BASE TABLE') ORDER BY table_name, ordinal_position"))
        {
            var table = (string)r[0]!;
            if (!columns.TryGetValue(table, out var list)) columns[table] = list = new List<string>();
            list.Add($"{r[1]} {r[2]}{((string)r[3]! == "NO" ? " NOT NULL" : "")}");
        }
        var indexes = (await QueryAsync(conn, "SELECT table_name || '.' || index_name FROM duckdb_indexes() WHERE database_name = current_database()"))
            .Select(r => (string)r[0]!).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var viewRows = new Dictionary<string, (long, string?)>(StringComparer.OrdinalIgnoreCase);
        if (views)
        {
            foreach (var r in await QueryAsync(conn, "SELECT view_name FROM duckdb_views() WHERE NOT internal AND database_name = current_database() AND schema_name = 'main'"))
            {
                try { viewRows[(string)r[0]!] = (Convert.ToInt64(await ScalarAsync(conn, $"SELECT COUNT(*) FROM \"{r[0]}\""), CultureInfo.InvariantCulture), null); }
                catch (Exception ex) { viewRows[(string)r[0]!] = (-1, ex.Message.Split('\n')[0]); }
            }
        }
        return new Snapshot(version, tables, columns, indexes, viewRows);
    }

    /* ═══════════════════════════ the reads ═══════════════════════════ */

    private sealed record ReadOutcome(string Status, long Rows);

    /// <summary>Every public Get* read on LocalDataService, for the first server and for all three where a read takes a list, and every
    /// MCP read tool for each server, bound the way <see cref="McpReadToolBudgetTests"/> binds them.</summary>
    private static async Task<Dictionary<string, ReadOutcome>> SweepAsync(DuckDbInitializer db, string configDir, int[] serverIds)
    {
        var outcomes = new Dictionary<string, ReadOutcome>(StringComparer.Ordinal);
        var data = new LocalDataService(db);
        var now = DateTime.UtcNow;
        var reads = typeof(LocalDataService).GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m.DeclaringType == typeof(LocalDataService) && m.Name.StartsWith("Get", StringComparison.Ordinal)
                && typeof(Task).IsAssignableFrom(m.ReturnType) && !m.IsGenericMethodDefinition);
        foreach (var method in reads)
        {
            var key = "LocalDataService." + method.Name + "(" + string.Join(",", method.GetParameters().Select(p => p.ParameterType.Name)) + ")";
            var args = method.GetParameters().Select(p => BindRead(p, serverIds, now)).ToArray();
            try
            {
                var task = (Task)method.Invoke(data, args)!;
                await task.WaitAsync(TimeSpan.FromSeconds(60));
                var result = task.GetType().GetProperty("Result")?.GetValue(task);
                outcomes[key] = new ReadOutcome("ok", RowCount(result));
            }
            catch (Exception ex)
            {
                var inner = (ex as TargetInvocationException)?.InnerException ?? ex;
                outcomes[key] = new ReadOutcome($"threw {inner.GetType().Name}: {inner.Message.Split('\n')[0]}", -1);
            }
        }

        var serverManager = new ServerManager(configDir);
        var analysis = new AnalysisService(db) { MinimumDataHours = 0 };
        var catalog = new McpToolGuideCatalog();
        /* The services the MCP host builds for the tools, over this store and its config folder: 3.13.0's collection_schedule.json
           feeds the run times, its mute rules the mute service. */
        var services = new Dictionary<Type, object>
        {
            [typeof(McpCollectorRunTimes)] = new McpCollectorRunTimes(new ScheduleManager(configDir), serverManager),
            [typeof(MuteRuleService)] = new MuteRuleService(new DuckDbMuteRuleStore(db), new CapturingLogger<MuteRuleService>(new List<(LogLevel, string)>())),
            [typeof(StoreHealthReader)] = new StoreHealthReader(db, new StoreHousekeepingHistory(Path.Combine(configDir, "store-health-history.json"))),
            [typeof(IInvestigationRequestStore)] = new DuckDbInvestigationStore(db),
            [typeof(InvestigationBriefComposer)] = new InvestigationBriefComposer(new DuckDbInvestigationBriefSourceReader(db),
                new McpDataDetailSetting(McpDataDetail.Full), LiteInvestigationBriefTools.Tools),
        };
        /* Each tool once per fixture server: the fixture holds nearly all of its rows on the second and third server, and a tool bound
           to the first would answer from the archive alone. */
        foreach (var serverName in FixtureServerNames)
        foreach (var (name, method) in McpReadToolBudgetTests.ReadToolMethods())
        {
            var key = $"mcp.{name}({serverName})";
            var args = method.GetParameters().Select(p =>
                p.ParameterType == typeof(LocalDataService) ? data :
                p.ParameterType == typeof(ServerManager) ? serverManager :
                p.ParameterType == typeof(AnalysisService) ? analysis :
                p.ParameterType == typeof(McpToolGuideCatalog) ? catalog :
                services.TryGetValue(p.ParameterType, out var service) ? service :
                p.Name == "server_name" ? serverName :
                p.Name == "hours_back" && p.ParameterType == typeof(int) ? McpHelpers.MaxHoursBack :
                p.HasDefaultValue ? p.DefaultValue :
                p.ParameterType.IsValueType ? Activator.CreateInstance(p.ParameterType) : (object?)null).ToArray();
            try
            {
                var json = await McpReadToolBudgetTests.InvokeAsync(method, args).WaitAsync(TimeSpan.FromSeconds(60));
                /* A refusal (a window past the tool's ceiling, a required argument the sweep does not bind) answers no rows on either store:
                   it is reported as a refusal, never as a count of zero. */
                outcomes[key] = McpHelpers.IsErrorEnvelope(json) || McpHelpers.IsRefusalEnvelope(json)
                    ? new ReadOutcome("answered an error or a refusal: " + json[..Math.Min(json.Length, 200)], -1)
                    : new ReadOutcome("ok", McpRowCount(json));
            }
            catch (Exception ex)
            {
                var inner = (ex as TargetInvocationException)?.InnerException ?? ex;
                outcomes[key] = new ReadOutcome($"threw {inner.GetType().Name}: {inner.Message.Split('\n')[0]}", -1);
            }
        }
        return outcomes;
    }

    /// <summary>The rows an MCP tool answered: the elements of every array in its payload, at any depth, summed. Scalars (timings,
    /// generated_at, totals, notes) are never read, so two stores that hold the same rows answer with the same count however the
    /// rest of the envelope differs, and a tool that loses a row, or a row's nested list entry, after the climb answers with fewer.</summary>
    private static long McpRowCount(string json)
    {
        using var payload = JsonDocument.Parse(json);
        return CountArrayElements(payload.RootElement);
    }

    private static long CountArrayElements(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Array => element.GetArrayLength() + element.EnumerateArray().Sum(CountArrayElements),
        JsonValueKind.Object => element.EnumerateObject().Sum(property => CountArrayElements(property.Value)),
        _ => 0,
    };

    private static object? BindRead(ParameterInfo p, int[] serverIds, DateTime now)
    {
        var name = p.Name ?? "";
        var type = Nullable.GetUnderlyingType(p.ParameterType) ?? p.ParameterType;
        if (type == typeof(int) && name.Equals("serverId", StringComparison.Ordinal)) return serverIds[0];
        if (name.Equals("serverIds", StringComparison.Ordinal) && p.ParameterType.IsAssignableFrom(typeof(int[]))) return serverIds;
        if (type == typeof(int) && name == "hoursBack") return 240;
        if (type == typeof(int) && name is "limit" or "topN" or "top") return 50;
        if (type == typeof(int) && name == "lookbackMinutes") return 60 * 24 * 11;
        if (type == typeof(DateTime) && !p.HasDefaultValue)
            return name.Contains("start", StringComparison.OrdinalIgnoreCase) || name.Contains("from", StringComparison.OrdinalIgnoreCase) ? now.AddDays(-11) : now;
        if (type == typeof(string) && !p.HasDefaultValue)
            return name switch { "databaseName" => "Db0", "schemaName" => "dbo", "waitType" => "CXPACKET", _ => name + "_0" };
        if (p.HasDefaultValue) return p.DefaultValue;
        return p.ParameterType.IsValueType ? Activator.CreateInstance(p.ParameterType) : null;
    }

    private static long RowCount(object? result) => result switch
    {
        null => 0,
        string => 1,
        ICollection c => c.Count,
        IEnumerable e => e.Cast<object?>().LongCount(),
        _ => 1,
    };

    /* ═══════════════════════════ the housekeeping passes ═══════════════════════════ */

    private static async Task<List<(string Step, string Outcome, Dictionary<string, (long Rows, string? Error)> Views, List<string> Warnings)>> HousekeepAsync(DuckDbInitializer db, Store store)
    {
        var log = new List<(LogLevel Level, string Message)>();
        var archive = new ArchiveService(db, store.ArchiveDir, new CapturingLogger<ArchiveService>(log));
        var retention = new RetentionService(store.ArchiveDir, new CapturingLogger<RetentionService>(log));
        var steps = new (string Name, Func<Task<string>> Run)[]
        {
            ("archival", async () => $"returned {await archive.ArchiveOldDataAsync(hotDataDays: 1)}"),
            ("retention", async () => $"returned {(await retention.CleanupOldArchivesAndRefreshViewsAsync(db, RetentionService.ArchiveRetentionMonths))?.ToString() ?? "null"}"),
            ("investigation_purge", async () => $"returned {await new DuckDbInvestigationStore(db).PurgeAsync()}"),
            ("findings_cleanup", async () => { await new AnalysisService(db).CleanupAsync(); return "done"; }),
            ("size_reset", async () => $"returned {await archive.ArchiveAllAndResetAsync()}"),
        };
        var results = new List<(string, string, Dictionary<string, (long, string?)>, List<string>)>();
        foreach (var (name, runStep) in steps)
        {
            log.Clear();
            string outcome;
            try { outcome = await runStep().WaitAsync(TimeSpan.FromMinutes(2)); }
            catch (Exception ex) { outcome = $"threw {ex.GetType().Name}: {ex.Message.Split('\n')[0]}"; }
            var snapshot = await SnapshotAsync(db, views: true);
            results.Add((name, outcome, snapshot.Views, log.Where(l => l.Level >= LogLevel.Warning).Select(l => $"{l.Level}: {Regex.Replace(l.Message, @"[A-Za-z]:\\\S+|/\S+/", "<path>")}").ToList()));
        }
        return results;
    }

    /* ═══════════════════════════ helpers ═══════════════════════════ */

    private static int StorageId(string serverName) =>
        RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(new ServerConnection { ServerName = serverName, DisplayName = serverName }));

    private static void CopyTree(string from, string to)
    {
        Directory.CreateDirectory(to);
        if (!Directory.Exists(from)) return;
        foreach (var dir in Directory.GetDirectories(from, "*", SearchOption.AllDirectories))
            Directory.CreateDirectory(Path.Combine(to, Path.GetRelativePath(from, dir)));
        foreach (var file in Directory.GetFiles(from, "*", SearchOption.AllDirectories))
            File.Copy(file, Path.Combine(to, Path.GetRelativePath(from, file)), overwrite: true);
    }

    private static async Task ExecAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<object?> ScalarAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync();
    }

    private static async Task<List<object?[]>> QueryAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<object?[]>();
        while (await reader.ReadAsync())
        {
            var row = new object?[reader.FieldCount];
            for (var c = 0; c < row.Length; c++) row[c] = reader.IsDBNull(c) ? null : reader.GetValue(c);
            rows.Add(row);
        }
        return rows;
    }

    private sealed class CapturingLogger<T>(List<(LogLevel Level, string Message)> sink) : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (sink) sink.Add((logLevel, formatter(state, exception) + (exception is null ? "" : $" ({exception.GetType().Name}: {exception.Message.Split('\n')[0]})")));
        }
    }
}
