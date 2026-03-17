using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Lite implementation of IPlanFetcher — fetches plans from SQL Server
/// using the server connection managed by ServerManager.
/// </summary>
public class SqlPlanFetcher : IPlanFetcher
{
    private readonly ServerManager _serverManager;

    public SqlPlanFetcher(ServerManager serverManager)
    {
        _serverManager = serverManager;
    }

    public async Task<string?> FetchPlanXmlAsync(int serverId, string planHandle)
    {
        if (string.IsNullOrEmpty(planHandle)) return null;

        // serverId is a hash — find the server by matching the hash
        var server = _serverManager.GetAllServers()
            .FirstOrDefault(s => s.ServerName.GetHashCode() == serverId);
        if (server == null) return null;

        try
        {
            var connectionString = server.GetConnectionString(_serverManager.CredentialService);
            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                ConnectTimeout = 10,
                CommandTimeout = 15
            };

            await using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync();

            await using var cmd = new SqlCommand(@"
SET NOCOUNT ON;
SELECT query_plan
FROM sys.dm_exec_query_plan(CONVERT(varbinary(64), @plan_handle, 1))", connection);

            cmd.CommandTimeout = 15;
            cmd.Parameters.AddWithValue("@plan_handle", planHandle);

            var result = await cmd.ExecuteScalarAsync();
            if (result == null || result is DBNull) return null;

            return result.ToString();
        }
        catch (Exception ex)
        {
            AppLogger.Error("SqlPlanFetcher",
                $"Failed to fetch plan for handle {planHandle}: {ex.Message}");
            return null;
        }
    }
}
