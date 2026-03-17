using Installer.Tests.Helpers;
using Microsoft.Data.SqlClient;

namespace Installer.Tests;

/// <summary>
/// Verifies all install scripts can be run twice without errors.
/// This is the single most important test for preventing #538-class bugs:
/// every script must use IF NOT EXISTS / CREATE OR ALTER guards.
/// </summary>
[Trait("Category", "Integration")]
[Collection("Database")]
public class IdempotencyTests : IAsyncLifetime
{
    private static readonly string[] BatchSeparators = ["GO", "go", "Go"];

    public async ValueTask InitializeAsync()
    {
        await TestDatabaseHelper.DropTestDatabaseAsync();
        await TestDatabaseHelper.CreateTestDatabaseAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await TestDatabaseHelper.DropTestDatabaseAsync();
    }

    [Fact]
    public async Task AllInstallScripts_CanRunTwice_WithoutErrors()
    {
        var installDir = FindInstallDirectory();
        Assert.NotNull(installDir);

        var sqlFiles = GetFilteredInstallFiles(installDir!);
        Assert.NotEmpty(sqlFiles);

        var connectionString = TestDatabaseHelper.GetTestDbConnectionString();

        // First run
        var firstRunFailures = await ExecuteAllScriptsAsync(sqlFiles, connectionString);
        Assert.Empty(firstRunFailures);

        // Second run — the idempotency test
        var secondRunFailures = await ExecuteAllScriptsAsync(sqlFiles, connectionString);
        Assert.Empty(secondRunFailures);
    }

    private static string? FindInstallDirectory()
    {
        // Walk up from the test output directory to find the repo's install/ folder
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        for (int i = 0; i < 10 && dir != null; i++)
        {
            var installPath = Path.Combine(dir.FullName, "install");
            if (Directory.Exists(installPath) &&
                Directory.GetFiles(installPath, "*.sql").Length > 0)
            {
                return installPath;
            }
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

    private static async Task<List<(string File, string Error)>> ExecuteAllScriptsAsync(
        List<string> sqlFiles, string connectionString)
    {
        var failures = new List<(string File, string Error)>();

        foreach (var file in sqlFiles)
        {
            var fileName = Path.GetFileName(file);

            try
            {
                var sql = await File.ReadAllTextAsync(file);

                // Replace PerformanceMonitor database references with our test database
                sql = RewriteForTestDatabase(sql);

                var batches = SplitBatches(sql);

                using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync();

                foreach (var batch in batches)
                {
                    if (string.IsNullOrWhiteSpace(batch)) continue;

                    using var cmd = new SqlCommand(batch, connection)
                    {
                        CommandTimeout = 120
                    };

                    try
                    {
                        await cmd.ExecuteNonQueryAsync();
                    }
                    catch (SqlException ex)
                    {
                        // Skip known non-fatal errors:
                        // - SQL Agent job errors (Agent may not be running / no permissions in test)
                        // - Extended events errors (may require sysadmin)
                        if (IsExpectedTestFailure(ex, fileName))
                            continue;

                        failures.Add((File: fileName, Error: $"Batch failed: {ex.Message}"));
                        break; // Stop this file on first real failure
                    }
                }
            }
            catch (Exception ex)
            {
                failures.Add((File: fileName, Error: $"File error: {ex.Message}"));
            }
        }

        return failures;
    }

    private static string RewriteForTestDatabase(string sql)
    {
        // The install scripts reference PerformanceMonitor by name in USE statements,
        // CREATE DATABASE, and cross-database references. Rewrite for our test database.
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

    private static bool IsExpectedTestFailure(SqlException ex, string fileName)
    {
        // SQL Agent operations fail without Agent running or sysadmin
        if (fileName.Contains("agent_jobs", StringComparison.OrdinalIgnoreCase) ||
            fileName.Contains("hung_job", StringComparison.OrdinalIgnoreCase))
            return true;

        // Extended events may require specific permissions
        if (fileName.Contains("blocked_process_xe", StringComparison.OrdinalIgnoreCase))
            return true;

        // "Cannot find the object" for Agent-related objects
        if (ex.Message.Contains("SQLServerAgent", StringComparison.OrdinalIgnoreCase))
            return true;

        return false;
    }

    private static List<string> SplitBatches(string sql)
    {
        var batches = new List<string>();
        var currentBatch = new System.Text.StringBuilder();

        foreach (var line in sql.Split('\n'))
        {
            var trimmed = line.TrimEnd('\r').Trim();

            if (BatchSeparators.Contains(trimmed))
            {
                var batch = currentBatch.ToString().Trim();
                if (!string.IsNullOrEmpty(batch))
                    batches.Add(batch);
                currentBatch.Clear();
            }
            else
            {
                currentBatch.AppendLine(line.TrimEnd('\r'));
            }
        }

        var lastBatch = currentBatch.ToString().Trim();
        if (!string.IsNullOrEmpty(lastBatch))
            batches.Add(lastBatch);

        return batches;
    }
}
