using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Text.Json;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;

namespace UiSmokeWpf;

/// <summary>
/// Writes the data folder Lite reads at startup (#5745): <c>monitor.duckdb</c> seeded by the analysis tests' own
/// scenarios, plus <c>config\servers.json</c> and <c>config\settings.json</c>.
///
/// <para>The scenarios seed under server_id -999 (<see cref="TestDataSeeder.TestServerId"/>), but Lite keys every read by
/// a hash of the server's storage name, so after each scenario every server_id column is rewritten to that hash (which
/// also takes the rows out of the next scenario's own clear of -999). The rows sit in the last day (the scenarios'
/// period ends at the latest 04:00 UTC), so settings.json opens the tab on "2d".</para>
///
/// <para>Meant for the CI runner. The seed writes into Lite's real data folder, so <see cref="LiteSeedGuard"/> stops it unless
/// that folder does not exist yet or the CI environment variable is set; a developer machine that uses Lite keeps its servers
/// and settings.</para>
/// </summary>
internal static class LiteSeed
{
    public const string ServerName = TestDataSeeder.TestServerName;

    /// <summary>A second server with collection off (#9085): the sidebar dot and the Overview card must say Disabled for it, not a live status.</summary>
    public const string DisabledServerName = "Paused-Reporting-Box";

    /// <summary>%LOCALAPPDATA%\PerformanceMonitorLite-Data, the folder App.xaml.cs resolves (DataRootMigration.DataRootName).</summary>
    public static string DataRoot { get; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "PerformanceMonitorLite-Data");

    /// <summary>The same deterministic hash Lite derives server_id from (ServerIdHelper, shared by every SKU).</summary>
    public static int ServerId => ServerIdHelper.GetDeterministicHashCode(ServerIdHelper.BuildStorageName(ServerName, null, false));

    public static async Task SeedAsync()
    {
        /* Before anything is created: Lite has no data-folder override, so this folder is the real one on a machine that uses Lite. */
        var refusal = LiteSeedGuard.Refusal(DataRoot, Environment.GetEnvironmentVariable("CI"));
        if (refusal != null)
            throw new InvalidOperationException(refusal);

        Directory.CreateDirectory(DataRoot);
        var config = Path.Combine(DataRoot, "config");
        Directory.CreateDirectory(config);

        var db = Path.Combine(DataRoot, "monitor.duckdb");
        var duck = new DuckDbInitializer(db);
        try
        {
            await duck.InitializeAsync();
            using var seeder = new TestDataSeeder(duck);

            // Waits (and CPU, memory, blocking and the rest of the "on fire" server).
            await seeder.SeedEverythingOnFireServerAsync();
            await RekeyAsync(duck);

            // Query stats (three plans); its own clear of -999 now finds nothing of the first pass.
            await seeder.SeedParameterSensitiveServerAsync();
            await RekeyAsync(duck);

            /* #8881: the "on fire" scenario's tempdb rows carry no log columns. Replace them with rows whose log is 92 percent used and held
               by a named session, so the tempdb chart draws the Log Used line and the card says who holds it. */
            using (var clearTempDb = duck.CreateConnection())
            {
                await clearTempDb.OpenAsync();
                using var del = clearTempDb.CreateCommand();
                del.CommandText = $"DELETE FROM tempdb_stats WHERE server_id = {ServerId}";
                await del.ExecuteNonQueryAsync();
            }
            await seeder.SeedTempDbAsync(reservedMb: 9_000, unallocatedMb: 1_000, maxSizeMb: 20_000, logUsedPercent: 92,
                logHoldup: "ACTIVE_TRANSACTION", holderSessionId: 57, holderLogin: "report_runner", holderProgram: "Nightly Reports",
                holderOpenMinutes: 95, holderLogBytesUsed: 4_500_000_000);
            await RekeyAsync(duck);

            using var conn = duck.CreateConnection();
            await conn.OpenAsync();

            /* The Top Queries grid (LocalDataService.TopQueriesByCpuSql) keeps only rows whose last_execution_time is inside
               the window and whose group has a positive delta; the analysis scenarios do not set last_execution_time (they
               never needed it), so a row with none gets its collection time. */
            foreach (var fix in new[]
            {
                "UPDATE query_stats SET last_execution_time = collection_time WHERE last_execution_time IS NULL",
                "UPDATE query_stats SET delta_execution_count = 1 WHERE delta_execution_count IS NULL",
            })
            {
                using var upd = conn.CreateCommand();
                upd.CommandText = fix;
                await upd.ExecuteNonQueryAsync();
            }
            foreach (var table in new[] { "wait_stats", "query_stats" })
            {
                using var chk = conn.CreateCommand();
                chk.CommandText = $"SELECT COUNT(*) FROM {table} WHERE server_id = {ServerId}";
                var n = Convert.ToInt64(await chk.ExecuteScalarAsync());
                if (n == 0) throw new InvalidOperationException($"the seed produced no {table} rows for the app's server_id {ServerId}");
                Console.WriteLine($"seeded {table}: {n} rows for server_id {ServerId}");
            }
        }
        finally
        {
            // Release the file before the app opens it.
            duck.Dispose();
        }

        var server = new Dictionary<string, object?>
        {
            ["Id"] = Guid.NewGuid().ToString(),
            ["ServerName"] = ServerName,
            ["DisplayName"] = ServerName,
            ["AuthenticationType"] = "Windows",
            ["EncryptMode"] = "Optional",
            ["TrustServerCertificate"] = true,
            ["IsEnabled"] = true,
        };
        /* #9085: collection off. It has no rows in the store: the card and the dot must still say Disabled and show no live status. */
        var disabled = new Dictionary<string, object?>
        {
            ["Id"] = Guid.NewGuid().ToString(),
            ["ServerName"] = DisabledServerName,
            ["DisplayName"] = DisabledServerName,
            ["AuthenticationType"] = "Windows",
            ["EncryptMode"] = "Optional",
            ["TrustServerCertificate"] = true,
            ["IsEnabled"] = false,
        };
        File.WriteAllText(Path.Combine(config, "servers.json"), JsonSerializer.Serialize(new Dictionary<string, object?> { ["Servers"] = new[] { server, disabled } }));
        File.WriteAllText(Path.Combine(config, "settings.json"), "{ \"default_time_range\": \"2d\" }");
    }

    private static async Task RekeyAsync(DuckDbInitializer duck)
    {
        using var conn = duck.CreateConnection();
        await conn.OpenAsync();
        var tables = new List<string>();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT c.table_name FROM information_schema.columns c JOIN information_schema.tables t "
                + "ON t.table_name = c.table_name AND t.table_schema = c.table_schema "
                + "WHERE c.column_name = 'server_id' AND c.table_schema = 'main' AND t.table_type = 'BASE TABLE'";
            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) tables.Add(r.GetString(0));
        }
        foreach (var t in tables)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"UPDATE \"{t}\" SET server_id = {ServerId} WHERE server_id = {TestDataSeeder.TestServerId}";
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
