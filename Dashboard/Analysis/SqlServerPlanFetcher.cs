using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard.Analysis;

/// <summary>
/// Dashboard implementation of IPlanFetcher -- fetches execution plans from SQL Server
/// using the monitored server's connection string directly.
/// Simpler than Lite's SqlPlanFetcher because Dashboard has one connection string
/// per database (no need to look up servers by ID).
/// </summary>
public class SqlServerPlanFetcher : IPlanFetcher
{
    private readonly string _connectionString;

    public SqlServerPlanFetcher(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task<string?> FetchPlanXmlAsync(int serverId, string planHandle)
    {
        if (string.IsNullOrEmpty(planHandle)) return null;

        try
        {
            var builder = new SqlConnectionStringBuilder(_connectionString)
            {
                ConnectTimeout = 10,
                CommandTimeout = 15
            };

            await using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync();

            await using var cmd = new SqlCommand(@"
SET NOCOUNT ON;
SELECT query_plan
FROM sys.dm_exec_query_plan(CONVERT(varbinary(64), @plan_handle, 1));", connection);

            cmd.CommandTimeout = 15;
            cmd.Parameters.AddWithValue("@plan_handle", planHandle);

            var result = await cmd.ExecuteScalarAsync();
            if (result == null || result is DBNull) return null;

            return result.ToString();
        }
        catch (Exception ex)
        {
            Logger.Error(
                $"[SqlServerPlanFetcher] Failed to fetch plan for handle {planHandle}: {ex.Message}");
            return null;
        }
    }
}
