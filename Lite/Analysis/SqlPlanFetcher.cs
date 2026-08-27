using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
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

    public async Task<string?> FetchPlanXmlAsync(int serverId, string planHandle, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(planHandle)) return null;

        // serverId is the deterministic FNV hash of the storage name (host[:db][:RO]) produced by
        // RemoteCollectorService.GetServerId. Match with the same function — string.GetHashCode()
        // is randomized per process and also ignores the db/RO suffixes, so it would never match.
        var server = _serverManager.GetAllServers()
            .FirstOrDefault(s =>
                RemoteCollectorService.GetDeterministicHashCode(
                    RemoteCollectorService.GetServerNameForStorage(s)) == serverId);
        if (server == null) return null;

        try
        {
            var connectionString = _serverManager.CredentialResolver.GetConnectionString(server);
            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                ConnectTimeout = 10,
                CommandTimeout = 15
            };

            await using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            await using var cmd = new SqlCommand(@"
SET NOCOUNT ON;
SELECT query_plan
FROM sys.dm_exec_query_plan(CONVERT(varbinary(64), @plan_handle, 1))", connection);

            cmd.CommandTimeout = 15;
            cmd.Parameters.AddWithValue("@plan_handle", planHandle);

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            if (result == null || result is DBNull) return null;

            return result.ToString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #2443: review caught this, and it is a real behaviour gap rather than a tidiness one.
               Lite arms a genuine per-pass budget (#2412), so the token this method now takes can and
               does fire mid-fetch on a healthy running app. An unconditional catch would log
               "Failed to fetch plan for handle …: The operation was canceled." as an ERROR for work
               we ourselves called off, AND swallow it, so the pass would carry on enriching under a
               fired token instead of unwinding to AnalysisService's one quiet line. Same arm the
               Darling twin's PgPlanFetcher carries for the identical call shape. */
            AppLogger.Error("SqlPlanFetcher",
                $"Failed to fetch plan for handle {planHandle}: {ex.Message}");
            return null;
        }
    }
}
