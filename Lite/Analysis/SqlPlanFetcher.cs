using System;
using System.Data;
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
public class SqlPlanFetcher : IPlanFetcher, IQueryStorePlanSource
{
    private readonly ServerManager _serverManager;

    public SqlPlanFetcher(ServerManager serverManager)
    {
        _serverManager = serverManager;
    }

    /// <summary>
    /// The Query Store plan of <paramref name="planId"/> (#5630), for the PLAN_REGRESSION inputs check. A short
    /// connect timeout keeps an unreachable server from holding the analysis pass; the caller also bounds each
    /// fetch with its own timeout and token.
    /// </summary>
    public async Task<string?> FetchQueryStorePlanXmlAsync(
        int serverId, string databaseName, long planId, CancellationToken cancellationToken)
    {
        var server = _serverManager.GetAllServers()
            .FirstOrDefault(s =>
                RemoteCollectorService.GetDeterministicHashCode(
                    RemoteCollectorService.GetServerNameForStorage(s)) == serverId);
        if (server == null) return null;

        var builder = new SqlConnectionStringBuilder(_serverManager.CredentialResolver.GetConnectionString(server))
        {
            ConnectTimeout = 5
        };
        /* The plan passes through the statement filter like every other live plan read (#4348): a statement the
           filter withholds is replaced before the comparison, so it compares Unknown and the candidate stays. */
        return await LivePlanDisplay.FilterAsync(await LocalDataService.FetchQueryStorePlanAsync(
            builder.ConnectionString, databaseName, planId, cancellationToken));
    }

    /// <summary>The plan of the whole batch a plan_handle names. <c>@plan_handle</c> is the '0x...' hex string the store keeps.</summary>
    internal const string BatchPlanSql = @"
SET NOCOUNT ON;
SELECT query_plan
FROM sys.dm_exec_query_plan(CONVERT(varbinary(64), @plan_handle, 1))";

    /// <summary>
    /// The plan of one statement of the batch (#9195): the same plan_handle, narrowed by the statement's byte offsets
    /// into the batch text. <c>@statement_end_offset</c> of -1 runs to the end of the batch. The text DMV returns the
    /// plan as <c>nvarchar(max)</c>, which the live plan read takes in chunks like the batch plan's <c>xml</c>.
    /// </summary>
    internal const string StatementPlanSql = @"
SET NOCOUNT ON;
SELECT query_plan
FROM sys.dm_exec_text_query_plan(CONVERT(varbinary(64), @plan_handle, 1), @statement_start_offset, @statement_end_offset)";

    /// <summary>
    /// The command that fetches a live plan: the one statement's plan when <paramref name="statement"/> carries its
    /// byte offsets (the end offset is passed as it is, -1 included), otherwise the whole batch's plan.
    /// </summary>
    internal static SqlCommand CreatePlanCommand(SqlConnection connection, string planHandle, (int Start, int End)? statement)
    {
        SqlCommand cmd;
        if (statement is { } offsets)
        {
            cmd = new SqlCommand(StatementPlanSql, connection);
            cmd.Parameters.AddWithValue("@plan_handle", planHandle);
            cmd.Parameters.Add(new SqlParameter("@statement_start_offset", SqlDbType.Int) { Value = offsets.Start });
            cmd.Parameters.Add(new SqlParameter("@statement_end_offset", SqlDbType.Int) { Value = offsets.End });
        }
        else
        {
            cmd = new SqlCommand(BatchPlanSql, connection);
            cmd.Parameters.AddWithValue("@plan_handle", planHandle);
        }

        cmd.CommandTimeout = 15;
        return cmd;
    }

    public Task<string?> FetchPlanXmlAsync(int serverId, string planHandle, CancellationToken cancellationToken) =>
        FetchAsync(serverId, planHandle, null, cancellationToken);

    public Task<string?> FetchStatementPlanXmlAsync(
        int serverId, string planHandle, int statementStartOffset, int statementEndOffset, CancellationToken cancellationToken) =>
        FetchAsync(serverId, planHandle, (statementStartOffset, statementEndOffset), cancellationToken);

    private async Task<string?> FetchAsync(
        int serverId, string planHandle, (int Start, int End)? statement, CancellationToken cancellationToken)
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

            await using var cmd = CreatePlanCommand(connection, planHandle, statement);

            // #9195: a batch plan can be ten megabytes, and ExecuteScalarAsync's CPU grows faster than the plan does.
            return await LivePlanXmlRead.FirstValueAsync(cmd, cancellationToken);
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
