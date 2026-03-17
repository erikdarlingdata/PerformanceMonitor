using Installer.Tests.Helpers;
using Microsoft.Data.SqlClient;

namespace Installer.Tests;

/// <summary>
/// Tests version detection logic — the #538 regression fix is the most critical test here.
///
/// Note: InstallationService.GetInstalledVersionAsync hardcodes the database name
/// "PerformanceMonitor", so these tests replicate the same SQL queries against a
/// test database (PerformanceMonitor_Test) to avoid touching real data.
/// </summary>
[Trait("Category", "Integration")]
[Collection("Database")]
public class VersionDetectionTests : IAsyncLifetime
{
    public async ValueTask InitializeAsync()
    {
        await TestDatabaseHelper.DropTestDatabaseAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await TestDatabaseHelper.DropTestDatabaseAsync();
    }

    [Fact]
    public async Task DatabaseDoesNotExist_ReturnsNull()
    {
        // Database was dropped in InitializeAsync
        var version = await GetInstalledVersionFromTestDbAsync();
        Assert.Null(version);
    }

    [Fact]
    public async Task DatabaseExists_WithSuccessRow_ReturnsVersion()
    {
        await TestDatabaseHelper.CreatePartialInstallationAsync("2.1.0");

        var version = await GetInstalledVersionFromTestDbAsync();
        Assert.Equal("2.1.0", version);
    }

    [Fact]
    public async Task DatabaseExists_NoHistoryTable_ReturnsNull()
    {
        // Create database but don't create the installation_history table
        await TestDatabaseHelper.CreateTestDatabaseAsync();

        var version = await GetInstalledVersionFromTestDbAsync();
        Assert.Null(version);
    }

    [Fact]
    public async Task DatabaseExists_EmptyHistoryTable_ReturnsFallback_Regression538()
    {
        // This is the #538 regression test.
        // When installation_history exists but has NO SUCCESS rows,
        // the installer must return "1.0.0" (not null), so it attempts
        // upgrades rather than treating the existing database as a fresh install.
        await TestDatabaseHelper.CreateInstallationWithNoSuccessRowsAsync();

        var version = await GetInstalledVersionFromTestDbAsync();
        Assert.Equal("1.0.0", version);
    }

    [Fact]
    public async Task DatabaseExists_OnlyFailedRows_ReturnsFallback()
    {
        // All rows are FAILED — same fallback behavior as empty table
        await TestDatabaseHelper.CreateInstallationWithOnlyFailedRowsAsync();

        var version = await GetInstalledVersionFromTestDbAsync();
        Assert.Equal("1.0.0", version);
    }

    [Fact]
    public async Task MultipleSuccessRows_ReturnsLatest()
    {
        await TestDatabaseHelper.CreatePartialInstallationAsync("1.3.0");

        // Add a newer success row
        using var connection = new SqlConnection(TestDatabaseHelper.GetTestDbConnectionString());
        await connection.OpenAsync();
        using var cmd = new SqlCommand(@"
            -- Use explicit future date to ensure this row sorts first
            INSERT INTO config.installation_history
                (installer_version, installation_status, installation_type, sql_server_version, sql_server_edition, installation_date)
            VALUES
                (N'2.2.0', N'SUCCESS', N'UPGRADE', @@VERSION, N'Test', DATEADD(HOUR, 1, SYSDATETIME()));",
            connection);
        await cmd.ExecuteNonQueryAsync();

        var version = await GetInstalledVersionFromTestDbAsync();
        Assert.Equal("2.2.0", version);
    }

    /// <summary>
    /// Replicates the same SQL logic as InstallationService.GetInstalledVersionAsync
    /// but queries PerformanceMonitor_Test instead of the hardcoded PerformanceMonitor.
    /// </summary>
    private static async Task<string?> GetInstalledVersionFromTestDbAsync()
    {
        const string testDbName = "PerformanceMonitor_Test";

        try
        {
            using var connection = new SqlConnection(TestDatabaseHelper.GetConnectionString());
            await connection.OpenAsync();

            // Check if database exists
            using var dbCheckCmd = new SqlCommand($@"
                SELECT database_id FROM sys.databases WHERE name = N'{testDbName}';", connection);
            var dbExists = await dbCheckCmd.ExecuteScalarAsync();
            if (dbExists == null || dbExists == DBNull.Value)
                return null;

            // Check if installation_history table exists
            using var tableCheckCmd = new SqlCommand($@"
                SELECT OBJECT_ID(N'{testDbName}.config.installation_history', N'U');", connection);
            var tableExists = await tableCheckCmd.ExecuteScalarAsync();
            if (tableExists == null || tableExists == DBNull.Value)
                return null;

            // Get most recent successful version
            using var versionCmd = new SqlCommand($@"
                SELECT TOP 1 installer_version
                FROM {testDbName}.config.installation_history
                WHERE installation_status = 'SUCCESS'
                ORDER BY installation_date DESC;", connection);
            var version = await versionCmd.ExecuteScalarAsync();
            if (version != null && version != DBNull.Value)
                return version.ToString();

            // Fallback: database + table exist but no SUCCESS rows → return "1.0.0"
            return "1.0.0";
        }
        catch
        {
            return null;
        }
    }
}
