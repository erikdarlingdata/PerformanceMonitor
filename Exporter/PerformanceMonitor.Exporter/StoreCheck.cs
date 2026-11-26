using System.Net.Sockets;
using Npgsql;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// The store checks `check-config` and `run --plan-only` run before anything is exported, so a store the exporter
/// cannot read correctly is found when the config is checked and not at the first hourly window. The check is the same
/// <see cref="QueryStoreWindow.AssertCCollationAsync"/> the export runs: a non-C collation is a
/// <see cref="ConfigException"/> (exit 2). A store that cannot be reached is not a configuration error and does not
/// fail the command: the check reports why it could not run and the command goes on.
/// </summary>
public static class StoreCheck
{
    /// <summary>How long the connect waits. A shorter wait than the export's 30 seconds keeps a config check quick when the store is down.</summary>
    internal const int ConnectTimeoutSeconds = 10;

    /// <summary>
    /// The connection settings of <paramref name="config"/> with <paramref name="secret"/> (the resolved password, or the
    /// whole connection string for `store.connection_string_env`), with the exporter's session guards applied.
    /// </summary>
    public static NpgsqlConnectionStringBuilder BuildConnection(ExporterConfig config, string secret)
    {
        var options = ExportOptions.Validate(
            config.Store.Name, "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z", config.Paths.Out, null,
            config.Store.Host, config.Store.Port, config.Store.Database, config.Store.User, null,
            config.Store.StatementTimeoutSeconds, false, true, DateTime.UtcNow, applicationName: config.Store.ApplicationName);
        return config.Store.ConnectionStringEnv is not null
            ? StoreConnectionFactory.BuildFromConnectionString(options, secret)
            : StoreConnectionFactory.BuildConnectionStringBuilder(options, secret);
    }

    /// <summary>
    /// Opens the store with <paramref name="builder"/> and runs the collation check. Returns null when the check ran and
    /// the store passed. Returns one line saying why the check could not run when the store could not be reached or
    /// read. Throws <see cref="ConfigException"/> when the store is not usable.
    /// </summary>
    public static async Task<string?> CheckCollationAsync(NpgsqlConnectionStringBuilder builder, CancellationToken ct)
    {
        builder.Timeout = Math.Min(builder.Timeout, ConnectTimeoutSeconds);
        try
        {
            await using var connection = new NpgsqlConnection(builder.ConnectionString);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await QueryStoreWindow.AssertCCollationAsync(connection, ct).ConfigureAwait(false);
            return null;
        }
        catch (Exception ex) when (ex is NpgsqlException or SocketException or TimeoutException or IOException)
        {
            return $"the store could not be read ({ex.GetType().Name}: {OneLine(ex.Message)})";
        }
    }

    /// <summary>
    /// `run --plan-only`: resolves the credential and checks the collation. A credential that cannot be resolved here
    /// (a dpapi_file password on another OS, an unset variable) returns the reason instead of failing, because a plan is
    /// worked out on any machine. Throws <see cref="ConfigException"/> for a non-C store.
    /// </summary>
    public static async Task<string?> PreflightForPlanAsync(ExporterConfig config, CancellationToken ct)
    {
        string secret;
        try { secret = CredentialResolver.Resolve(config.Store.Credential, TextWriter.Null); }
        catch (CredentialException ex) { return $"the store credential could not be resolved ({OneLine(ex.Message)})"; }
        return await CheckCollationAsync(BuildConnection(config, secret), ct).ConfigureAwait(false);
    }

    private static string OneLine(string text) => text.ReplaceLineEndings(" ").Trim();
}
