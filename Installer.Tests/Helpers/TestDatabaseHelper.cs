using Microsoft.Data.SqlClient;

namespace Installer.Tests.Helpers;

public static class TestDatabaseHelper
{
    private const string TestDatabaseName = "PerformanceMonitor_Test";

    public static string GetConnectionString(string database = "master")
    {
        return $"Server=SQL2022;Database={database};User Id=sa;Password=L!nt0044;TrustServerCertificate=true;";
    }

    public static string GetTestDbConnectionString()
    {
        return GetConnectionString(TestDatabaseName);
    }

    public static async Task CreateTestDatabaseAsync()
    {
        using var connection = new SqlConnection(GetConnectionString());
        await connection.OpenAsync();

        using var cmd = new SqlCommand($@"
            IF DB_ID(N'{TestDatabaseName}') IS NULL
                CREATE DATABASE [{TestDatabaseName}];", connection);
        await cmd.ExecuteNonQueryAsync();
    }

    public static async Task DropTestDatabaseAsync()
    {
        using var connection = new SqlConnection(GetConnectionString());
        await connection.OpenAsync();

        using var cmd = new SqlCommand($@"
            IF DB_ID(N'{TestDatabaseName}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{TestDatabaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{TestDatabaseName}];
            END;", connection);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Creates a partial installation: just the config schema and installation_history table
    /// with a SUCCESS row for the given version. Simulates a broken or incomplete prior install.
    /// </summary>
    public static async Task CreatePartialInstallationAsync(string version)
    {
        await CreateTestDatabaseAsync();

        using var connection = new SqlConnection(GetTestDbConnectionString());
        await connection.OpenAsync();

        // Must match the real schema from 01_install_database.sql exactly,
        // otherwise CREATE OR ALTER VIEW on config.current_version will fail
        // referencing columns that don't exist.
        using var cmd = new SqlCommand($@"
            IF SCHEMA_ID('config') IS NULL
                EXEC('CREATE SCHEMA config;');

            IF OBJECT_ID('config.installation_history', 'U') IS NULL
                CREATE TABLE config.installation_history
                (
                    installation_id integer IDENTITY NOT NULL,
                    installation_date datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
                    installer_version nvarchar(50) NOT NULL,
                    installer_info_version nvarchar(100) NULL,
                    sql_server_version nvarchar(255) NOT NULL DEFAULT N'Unknown',
                    sql_server_edition nvarchar(255) NOT NULL DEFAULT N'Unknown',
                    installation_type nvarchar(20) NOT NULL DEFAULT N'UPGRADE',
                    previous_version nvarchar(50) NULL,
                    installation_status nvarchar(20) NOT NULL,
                    files_executed integer NULL,
                    files_failed integer NULL,
                    installation_duration_ms integer NULL,
                    installation_notes nvarchar(max) NULL,
                    CONSTRAINT PK_installation_history PRIMARY KEY CLUSTERED (installation_id)
                );

            INSERT INTO config.installation_history
                (installer_version, installation_status, installation_type, sql_server_version, sql_server_edition)
            VALUES
                (N'{version}', N'SUCCESS', N'UPGRADE', @@VERSION, N'Test');",
            connection);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Creates the database with installation_history but NO success rows.
    /// This is the #538 scenario where the version detection fallback kicks in.
    /// </summary>
    public static async Task CreateInstallationWithNoSuccessRowsAsync()
    {
        await CreateTestDatabaseAsync();

        using var connection = new SqlConnection(GetTestDbConnectionString());
        await connection.OpenAsync();

        using var cmd = new SqlCommand(@"
            IF SCHEMA_ID('config') IS NULL
                EXEC('CREATE SCHEMA config;');

            IF OBJECT_ID('config.installation_history', 'U') IS NULL
                CREATE TABLE config.installation_history
                (
                    installation_id integer IDENTITY NOT NULL,
                    installation_date datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
                    installer_version nvarchar(50) NOT NULL,
                    installer_info_version nvarchar(100) NULL,
                    sql_server_version nvarchar(255) NOT NULL DEFAULT N'Unknown',
                    sql_server_edition nvarchar(255) NOT NULL DEFAULT N'Unknown',
                    installation_type nvarchar(20) NOT NULL DEFAULT N'UPGRADE',
                    previous_version nvarchar(50) NULL,
                    installation_status nvarchar(20) NOT NULL,
                    files_executed integer NULL,
                    files_failed integer NULL,
                    installation_duration_ms integer NULL,
                    installation_notes nvarchar(max) NULL,
                    CONSTRAINT PK_installation_history PRIMARY KEY CLUSTERED (installation_id)
                );",
            connection);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Creates the database with only FAILED rows in installation_history.
    /// </summary>
    public static async Task CreateInstallationWithOnlyFailedRowsAsync()
    {
        await CreateInstallationWithNoSuccessRowsAsync();

        using var connection = new SqlConnection(GetTestDbConnectionString());
        await connection.OpenAsync();

        using var cmd = new SqlCommand(@"
            INSERT INTO config.installation_history
                (installer_version, installation_status, installation_type, sql_server_version, sql_server_edition)
            VALUES
                (N'2.0.0', N'FAILED', N'UPGRADE', @@VERSION, N'Test');",
            connection);
        await cmd.ExecuteNonQueryAsync();
    }
}
