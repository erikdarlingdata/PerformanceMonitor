using Installer.Tests.Helpers;
using Microsoft.Data.SqlClient;
using PerformanceMonitorInstallerGui.Services;

namespace Installer.Tests;

/// <summary>
/// Adversarial/misery-path tests: designed to break the installer and verify
/// it fails safely without data loss. These test the scenarios that caused #538.
/// </summary>
[Trait("Category", "Integration")]
[Collection("Database")]
public class AdversarialTests : IAsyncLifetime
{
    public async ValueTask InitializeAsync()
    {
        await TestDatabaseHelper.DropTestDatabaseAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await TestDatabaseHelper.DropTestDatabaseAsync();
    }

    /// <summary>
    /// #538 root cause: upgrade script failed, but installer continued to the
    /// install phase which ran 00_uninstall.sql and dropped the database.
    /// Verify that upgrade failures prevent install scripts from running.
    /// </summary>
    [Fact]
    public async Task UpgradeFailure_DoesNotDropDatabase()
    {
        // Setup: create a real database with data
        await TestDatabaseHelper.CreatePartialInstallationAsync("2.0.0");

        // Insert a canary row we can check survived
        using (var conn = new SqlConnection(TestDatabaseHelper.GetTestDbConnectionString()))
        {
            await conn.OpenAsync();
            using var cmd = new SqlCommand(@"
                CREATE TABLE config.canary_data (id int NOT NULL, value nvarchar(50) NOT NULL);
                INSERT INTO config.canary_data VALUES (1, 'must_survive');", conn);
            await cmd.ExecuteNonQueryAsync();
        }

        // Create a poisoned upgrade that will fail
        using var dir = new TempDirectoryBuilder()
            .WithInstallFiles("01_install_database.sql")
            .WithUpgrade("2.0.0", "2.1.0", "01_will_fail.sql");

        // Write a script that will definitely fail
        File.WriteAllText(
            Path.Combine(dir.UpgradesPath, "2.0.0-to-2.1.0", "01_will_fail.sql"),
            "SELECT 1/0; -- division by zero");

        // Run upgrades — should fail
        var (_, failureCount, _) = await InstallationService.ExecuteAllUpgradesAsync(
            dir.RootPath,
            TestDatabaseHelper.GetTestDbConnectionString(),
            "2.0.0",
            "2.1.0");

        Assert.True(failureCount > 0, "Upgrade should have failed");

        // The critical assertion: database and data must still exist
        using (var conn = new SqlConnection(TestDatabaseHelper.GetTestDbConnectionString()))
        {
            await conn.OpenAsync();
            using var cmd = new SqlCommand(
                "SELECT value FROM config.canary_data WHERE id = 1;", conn);
            var result = await cmd.ExecuteScalarAsync();
            Assert.Equal("must_survive", result?.ToString());
        }
    }

    /// <summary>
    /// Partial prior install: database exists with only installation_history,
    /// all other tables missing. The install scripts must CREATE them without
    /// failing on missing dependencies.
    ///
    /// Note: install scripts hardcode "PerformanceMonitor" as the database name,
    /// so we rewrite references to point at our test database (same approach as
    /// IdempotencyTests). This tests the scripts' IF NOT EXISTS / CREATE OR ALTER
    /// guards against a partial schema.
    /// </summary>
    [Fact]
    public async Task PartialInstall_InstallScriptsRecover()
    {
        await TestDatabaseHelper.CreatePartialInstallationAsync("2.0.0");

        // Verify: only installation_history exists, no collect/report schemas
        using (var conn = new SqlConnection(TestDatabaseHelper.GetTestDbConnectionString()))
        {
            await conn.OpenAsync();
            using var cmd = new SqlCommand(
                "SELECT COUNT(*) FROM sys.tables WHERE schema_id != SCHEMA_ID('config');", conn);
            var nonConfigTables = (int)(await cmd.ExecuteScalarAsync())!;
            Assert.Equal(0, nonConfigTables);
        }

        // Run install scripts with DB name rewriting
        var installDir = FindInstallDirectory();
        Assert.NotNull(installDir);

        var sqlFiles = GetFilteredInstallFiles(installDir!);
        var connectionString = TestDatabaseHelper.GetTestDbConnectionString();

        // Execute scripts with rewriting (same as IdempotencyTests)
        var failures = new List<string>();
        foreach (var file in sqlFiles)
        {
            var fileName = Path.GetFileName(file);
            try
            {
                var sql = await File.ReadAllTextAsync(file);
                sql = RewriteForTestDatabase(sql);
                var batches = SplitGoBatches(sql);

                using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync();

                foreach (var batch in batches)
                {
                    if (string.IsNullOrWhiteSpace(batch)) continue;
                    using var cmd = new SqlCommand(batch, conn) { CommandTimeout = 120 };
                    try { await cmd.ExecuteNonQueryAsync(); }
                    catch (SqlException ex)
                    {
                        if (IsExpectedTestFailure(ex, fileName)) continue;
                        failures.Add($"{fileName}: {ex.Message}");
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                if (!IsExpectedTestFailure(null, fileName))
                    failures.Add($"{fileName}: {ex.Message}");
            }
        }

        Assert.Empty(failures);

        // Verify core tables were created from the partial state
        using (var conn = new SqlConnection(TestDatabaseHelper.GetTestDbConnectionString()))
        {
            await conn.OpenAsync();
            using var cmd = new SqlCommand(@"
                SELECT COUNT(*) FROM sys.tables
                WHERE schema_id = SCHEMA_ID('collect')
                AND name IN ('wait_stats', 'query_stats', 'cpu_utilization_stats');", conn);
            var collectTables = (int)(await cmd.ExecuteScalarAsync())!;
            Assert.True(collectTables >= 3, $"Expected at least 3 collect tables, got {collectTables}");
        }
    }

    /// <summary>
    /// Critical file failure (01_, 02_, 03_) must abort the entire installation,
    /// not continue executing the remaining 50+ scripts.
    /// </summary>
    [Fact]
    public async Task CriticalFileFailure_AbortsInstallation()
    {
        await TestDatabaseHelper.CreateTestDatabaseAsync();

        // Create install files where 02_ will fail
        using var dir = new TempDirectoryBuilder()
            .WithInstallFiles(
                "01_install_database.sql",
                "02_create_tables.sql",
                "03_config.sql",
                "04_schedule.sql",
                "05_procs.sql");

        // 01_ succeeds (harmless)
        File.WriteAllText(Path.Combine(dir.InstallPath, "01_install_database.sql"),
            "PRINT 'ok';");
        // 02_ fails hard
        File.WriteAllText(Path.Combine(dir.InstallPath, "02_create_tables.sql"),
            "RAISERROR('Simulated critical failure', 16, 1);");
        // 03_-05_ should never execute
        File.WriteAllText(Path.Combine(dir.InstallPath, "03_config.sql"),
            "CREATE TABLE dbo.should_not_exist (id int);");
        File.WriteAllText(Path.Combine(dir.InstallPath, "04_schedule.sql"),
            "CREATE TABLE dbo.also_should_not_exist (id int);");
        File.WriteAllText(Path.Combine(dir.InstallPath, "05_procs.sql"),
            "CREATE TABLE dbo.definitely_should_not_exist (id int);");

        var files = dir.GetFilteredInstallFiles();
        var result = await InstallationService.ExecuteInstallationAsync(
            TestDatabaseHelper.GetTestDbConnectionString(),
            files,
            cleanInstall: false);

        Assert.False(result.Success);
        Assert.True(result.FilesFailed >= 1);

        // Verify abort: scripts after 02_ must NOT have run
        using var conn = new SqlConnection(TestDatabaseHelper.GetTestDbConnectionString());
        await conn.OpenAsync();
        using var cmd = new SqlCommand(
            "SELECT OBJECT_ID('dbo.should_not_exist', 'U');", conn);
        var obj = await cmd.ExecuteScalarAsync();
        Assert.True(obj == null || obj == DBNull.Value,
            "03_config.sql should not have executed after 02_ critical failure");
    }

    /// <summary>
    /// Cancellation mid-install should not leave the database in an unusable state.
    /// The version should remain at the pre-upgrade level so a retry works.
    /// </summary>
    [Fact]
    public async Task CancellationMidUpgrade_VersionUnchanged()
    {
        await TestDatabaseHelper.CreatePartialInstallationAsync("2.0.0");

        using var dir = new TempDirectoryBuilder()
            .WithUpgrade("2.0.0", "2.1.0", "01_slow.sql", "02_never_runs.sql");

        // First script runs for a while then we cancel
        File.WriteAllText(
            Path.Combine(dir.UpgradesPath, "2.0.0-to-2.1.0", "01_slow.sql"),
            "WAITFOR DELAY '00:00:05';");
        File.WriteAllText(
            Path.Combine(dir.UpgradesPath, "2.0.0-to-2.1.0", "02_never_runs.sql"),
            "PRINT 'should not reach here';");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

        try
        {
            await InstallationService.ExecuteAllUpgradesAsync(
                dir.RootPath,
                TestDatabaseHelper.GetTestDbConnectionString(),
                "2.0.0",
                "2.1.0",
                cancellationToken: cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        // Version must still be 2.0.0 — no SUCCESS row written for 2.1.0
        using var conn = new SqlConnection(TestDatabaseHelper.GetTestDbConnectionString());
        await conn.OpenAsync();
        using var cmd = new SqlCommand(@"
            SELECT TOP 1 installer_version
            FROM config.installation_history
            WHERE installation_status = 'SUCCESS'
            ORDER BY installation_date DESC;", conn);
        var version = await cmd.ExecuteScalarAsync();
        Assert.Equal("2.0.0", version?.ToString());
    }

    /// <summary>
    /// Non-critical file failure (04_+) should NOT abort — remaining scripts still run.
    /// </summary>
    [Fact]
    public async Task NonCriticalFileFailure_ContinuesInstallation()
    {
        await TestDatabaseHelper.CreateTestDatabaseAsync();

        using var dir = new TempDirectoryBuilder()
            .WithInstallFiles(
                "01_setup.sql",
                "04_will_fail.sql",
                "05_should_still_run.sql");

        File.WriteAllText(Path.Combine(dir.InstallPath, "01_setup.sql"),
            "PRINT 'ok';");
        File.WriteAllText(Path.Combine(dir.InstallPath, "04_will_fail.sql"),
            "RAISERROR('Non-critical failure', 16, 1);");
        File.WriteAllText(Path.Combine(dir.InstallPath, "05_should_still_run.sql"),
            "CREATE TABLE dbo.proof_it_continued (id int);");

        var files = dir.GetFilteredInstallFiles();
        var result = await InstallationService.ExecuteInstallationAsync(
            TestDatabaseHelper.GetTestDbConnectionString(),
            files,
            cleanInstall: false);

        // 04_ failed but 05_ should have run
        Assert.True(result.FilesFailed >= 1);

        using var conn = new SqlConnection(TestDatabaseHelper.GetTestDbConnectionString());
        await conn.OpenAsync();
        using var cmd = new SqlCommand(
            "SELECT OBJECT_ID('dbo.proof_it_continued', 'U');", conn);
        var obj = await cmd.ExecuteScalarAsync();
        Assert.True(obj != null && obj != DBNull.Value,
            "05_ should have executed despite 04_ failure");
    }

    /// <summary>
    /// Corrupt SQL content — garbage that isn't valid T-SQL.
    /// Should fail gracefully, not crash the installer process.
    /// </summary>
    [Fact]
    public async Task CorruptSqlContent_FailsGracefully()
    {
        await TestDatabaseHelper.CreateTestDatabaseAsync();

        using var dir = new TempDirectoryBuilder()
            .WithInstallFiles("01_setup.sql", "04_corrupt.sql");

        File.WriteAllText(Path.Combine(dir.InstallPath, "01_setup.sql"),
            "PRINT 'ok';");
        File.WriteAllText(Path.Combine(dir.InstallPath, "04_corrupt.sql"),
            "THIS IS NOT SQL AT ALL 🔥 §±∞ DROP TABLE BOBBY;; EXEC(((");

        var files = dir.GetFilteredInstallFiles();
        var result = await InstallationService.ExecuteInstallationAsync(
            TestDatabaseHelper.GetTestDbConnectionString(),
            files,
            cleanInstall: false);

        // Should complete (not throw), with 04_ counted as failed
        Assert.True(result.FilesFailed >= 1);
        Assert.True(result.FilesSucceeded >= 1); // 01_ should have succeeded
    }

    /// <summary>
    /// Empty SQL file — should not crash, just be a no-op.
    /// </summary>
    [Fact]
    public async Task EmptySqlFile_DoesNotCrash()
    {
        await TestDatabaseHelper.CreateTestDatabaseAsync();

        using var dir = new TempDirectoryBuilder()
            .WithInstallFiles("01_empty.sql");

        File.WriteAllText(Path.Combine(dir.InstallPath, "01_empty.sql"), "");

        var files = dir.GetFilteredInstallFiles();
        var result = await InstallationService.ExecuteInstallationAsync(
            TestDatabaseHelper.GetTestDbConnectionString(),
            files,
            cleanInstall: false);

        Assert.True(result.Success);
    }

    /// <summary>
    /// Version detection when database exists but connection is to wrong server/port.
    /// GUI silently returns null (potential data-loss vector) — verify this behavior
    /// is documented even if not fixed yet.
    /// </summary>
    [Fact]
    public async Task VersionDetection_ConnectionFailure_ReturnsNull()
    {
        // Intentionally bad connection string
        var badConnStr = "Server=DOESNOTEXIST;Database=master;User Id=sa;Password=x;TrustServerCertificate=true;Connect Timeout=2;";

        var version = await InstallationService.GetInstalledVersionAsync(badConnStr);

        // GUI swallows exceptions and returns null.
        // This means a transient network failure could cause the GUI to treat
        // an existing installation as a fresh install. Documenting this behavior.
        Assert.Null(version);
    }

    #region Helpers

    private static string? FindInstallDirectory()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        for (int i = 0; i < 10 && dir != null; i++)
        {
            var installPath = Path.Combine(dir.FullName, "install");
            if (Directory.Exists(installPath) &&
                Directory.GetFiles(installPath, "*.sql").Length > 0)
                return installPath;
            dir = dir.Parent;
        }
        return null;
    }

    private static List<string> GetFilteredInstallFiles(string installDir)
    {
        var pattern = new System.Text.RegularExpressions.Regex(@"^\d{2}[a-z]?_.*\.sql$");
        return Directory.GetFiles(installDir, "*.sql")
            .Where(f =>
            {
                var name = Path.GetFileName(f);
                if (!pattern.IsMatch(name)) return false;
                if (name.StartsWith("00_", StringComparison.Ordinal) ||
                    name.StartsWith("97_", StringComparison.Ordinal) ||
                    name.StartsWith("99_", StringComparison.Ordinal))
                    return false;
                return true;
            })
            .OrderBy(f => Path.GetFileName(f))
            .ToList();
    }

    private static string RewriteForTestDatabase(string sql)
    {
        return sql
            .Replace("[PerformanceMonitor]", "[PerformanceMonitor_Test]")
            .Replace("N'PerformanceMonitor'", "N'PerformanceMonitor_Test'")
            .Replace("'PerformanceMonitor'", "'PerformanceMonitor_Test'")
            .Replace("USE PerformanceMonitor;", "USE PerformanceMonitor_Test;")
            .Replace("USE PerformanceMonitor\r\n", "USE PerformanceMonitor_Test\r\n")
            .Replace("USE PerformanceMonitor\n", "USE PerformanceMonitor_Test\n")
            .Replace("DB_ID(N'PerformanceMonitor')", "DB_ID(N'PerformanceMonitor_Test')")
            .Replace("PerformanceMonitor.dbo.", "PerformanceMonitor_Test.dbo.")
            .Replace("PerformanceMonitor.collect.", "PerformanceMonitor_Test.collect.")
            .Replace("PerformanceMonitor.config.", "PerformanceMonitor_Test.config.")
            .Replace("PerformanceMonitor.report.", "PerformanceMonitor_Test.report.");
    }

    private static bool IsExpectedTestFailure(SqlException? ex, string fileName)
    {
        if (fileName.Contains("agent_jobs", StringComparison.OrdinalIgnoreCase) ||
            fileName.Contains("hung_job", StringComparison.OrdinalIgnoreCase) ||
            fileName.Contains("blocked_process_xe", StringComparison.OrdinalIgnoreCase))
            return true;
        if (ex?.Message.Contains("SQLServerAgent", StringComparison.OrdinalIgnoreCase) == true)
            return true;
        return false;
    }

    private static List<string> SplitGoBatches(string sql)
    {
        var batches = new List<string>();
        var current = new System.Text.StringBuilder();
        foreach (var line in sql.Split('\n'))
        {
            var trimmed = line.TrimEnd('\r').Trim();
            if (trimmed.Equals("GO", StringComparison.OrdinalIgnoreCase))
            {
                var batch = current.ToString().Trim();
                if (!string.IsNullOrEmpty(batch)) batches.Add(batch);
                current.Clear();
            }
            else
            {
                current.AppendLine(line.TrimEnd('\r'));
            }
        }
        var last = current.ToString().Trim();
        if (!string.IsNullOrEmpty(last)) batches.Add(last);
        return batches;
    }

    #endregion
}
