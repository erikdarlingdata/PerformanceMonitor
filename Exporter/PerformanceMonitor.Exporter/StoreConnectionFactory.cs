using System.Collections.Generic;
using Npgsql;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Builds the Npgsql connection used to read a store. Always read-only, always bounded by a
/// statement timeout, and the connection string is never logged (it embeds the password).
/// </summary>
public static class StoreConnectionFactory
{
    /// <summary>
    /// Builds the read-only connection string for a store. `Options` sets
    /// default_transaction_read_only so a bug can never write to the store, plus a
    /// statement_timeout and a short lock_timeout so the exporter can't sit behind a lock
    /// Darling's collector needs.
    /// </summary>
    public static NpgsqlConnectionStringBuilder BuildConnectionStringBuilder(
        ExportOptions options, string password)
    {
        return ApplyGuards(new NpgsqlConnectionStringBuilder
        {
            Host = options.PgHost,
            Port = options.PgPort,
            Database = options.PgDatabase,
            Username = options.PgUser,
            Password = password,
        }, options);
    }

    /// <summary>
    /// Builds the connection from a full connection string (config `store.connection_string_env`). The string supplies the
    /// host, login and anything else, but the read-only, statement_timeout and lock_timeout `Options`, the application
    /// name and both timeouts are overwritten, so a connection string can never turn a session guard off.
    /// </summary>
    public static NpgsqlConnectionStringBuilder BuildFromConnectionString(ExportOptions options, string connectionString)
    {
        NpgsqlConnectionStringBuilder builder;
        try
        {
            builder = new NpgsqlConnectionStringBuilder(connectionString);
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException or NotSupportedException)
        {
            // The message of an Npgsql parse error can quote part of the string; never pass it on.
            throw new CredentialException("store.connection_string_env: the connection string is not a valid Npgsql connection string");
        }

        return ApplyGuards(builder, options);
    }

    /// <summary>
    /// The connection for <paramref name="options"/>: resolves the credential (a config source, or today's env var and
    /// DPAPI file when there is no config) and applies the guards.
    /// </summary>
    public static NpgsqlConnectionStringBuilder Build(ExportOptions options)
    {
        if (options.Credential is not { } spec)
        {
            return BuildConnectionStringBuilder(options, CredentialResolver.Resolve(options.PgCredentialFile));
        }

        var secret = CredentialResolver.Resolve(spec);
        return spec.Source == CredentialSources.ConnectionStringEnv
            ? BuildFromConnectionString(options, secret)
            : BuildConnectionStringBuilder(options, secret);
    }

    private static NpgsqlConnectionStringBuilder ApplyGuards(NpgsqlConnectionStringBuilder builder, ExportOptions options)
    {
        var statementTimeoutMs = options.StatementTimeoutSeconds * 1000;
        builder.ApplicationName = options.ApplicationName;
        builder.Options = $"-c default_transaction_read_only=on -c statement_timeout={statementTimeoutMs} -c lock_timeout=5000";
        builder.Timeout = 30;
        builder.CommandTimeout = options.StatementTimeoutSeconds;
        return builder;
    }

    /// <summary>Opens a new connection to the store using the given connection string, then applies the session guards.</summary>
    public static async Task<NpgsqlConnection> OpenAsync(NpgsqlConnectionStringBuilder builder, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync(ct).ConfigureAwait(false);
        await ApplySessionGuardsAsync(connection, ct).ConfigureAwait(false);
        return connection;
    }

    /// <summary>
    /// Applies the session-level guards after connecting, never in the startup options: `lock_timeout` and
    /// `application_name` are already set there and stay put. Order matters: `work_mem` and
    /// `max_parallel_workers_per_gather` first, because no statement of ours needs more than 16MB or a
    /// parallel worker; `temp_file_limit` last, because it is superuser-only and a non-superuser role must not block
    /// the run over it. On SQLSTATE 42501 (insufficient_privilege) for `temp_file_limit`, log one warning to stderr
    /// and continue -- the connection must never fail over a guard that only exists to make a mistake cancel our own
    /// statement sooner. Before any SET, the current values are read with one combined SHOW (pg_settings), tagged `/*
    /// darling-export:session_guard */`. A guard already at least as strict as ours (role-level defaults, e.g. a
    /// dedicated read-only role with `temp_file_limit` set at the role) is left alone: the SET is skipped, so a
    /// non-superuser role with a role-level `temp_file_limit` never hits the 42501 warning path at all, and no SET
    /// round trip is spent re-applying a setting that already holds.
    /// </summary>
    public static async Task ApplySessionGuardsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var current = await ReadCurrentGuardSettingsAsync(connection, ct).ConfigureAwait(false);
        var inheritedGuards = new List<string>();
        var appliedGuards = new List<string>();

        if (current.WorkMemKb.HasValue && current.WorkMemKb.Value <= 16 * 1024)
        {
            inheritedGuards.Add($"work_mem={current.WorkMemKb.Value}kB");
        }
        else
        {
            await using (var workMemCommand = new NpgsqlCommand(
                "/* darling-export:session_guard.work_mem */ SET work_mem = '16MB'", connection))
            {
                await workMemCommand.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            appliedGuards.Add("work_mem=16MB");
        }

        if (current.MaxParallelWorkersPerGather.HasValue && current.MaxParallelWorkersPerGather.Value == 0)
        {
            inheritedGuards.Add("max_parallel_workers_per_gather=0");
        }
        else
        {
            await using (var parallelCommand = new NpgsqlCommand(
                "/* darling-export:session_guard.max_parallel_workers_per_gather */ SET max_parallel_workers_per_gather = 0", connection))
            {
                await parallelCommand.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            appliedGuards.Add("max_parallel_workers_per_gather=0");
        }

        // temp_file_limit's setting value is -1 (disabled) or a size in KB; "already strict enough" means a finite
        // limit at or below 256MB. -1 (no limit) is never inherited as sufficient.
        if (current.TempFileLimitKb.HasValue && current.TempFileLimitKb.Value >= 0 && current.TempFileLimitKb.Value <= 256 * 1024)
        {
            inheritedGuards.Add($"temp_file_limit={current.TempFileLimitKb.Value}kB");
        }
        else
        {
            try
            {
                await using var tempFileLimitCommand = new NpgsqlCommand(
                    "/* darling-export:session_guard.temp_file_limit */ SET temp_file_limit = '256MB'", connection);
                await tempFileLimitCommand.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                appliedGuards.Add("temp_file_limit=256MB");
            }
            catch (PostgresException ex) when (ex.SqlState == "42501")
            {
                // insufficient_privilege: temp_file_limit is superuser-only. The role may not be a superuser; the run
                // continues without this guard rather than refusing the connection and breaking the whole export.
                Console.Error.WriteLine(
                    "Warning: could not SET temp_file_limit (insufficient privilege). Ask Darling to run " +
                    "ALTER ROLE <exporter role> SET temp_file_limit = '256MB'. Continuing without this guard.");
            }
        }

        // DEBUG-level log line: lists the session guards actually applied this
        // connection vs. inherited (already strict enough at the role/session level) so a test can prove
        // ApplySessionGuardsAsync ran without needing a second session to read pg_settings mid-export (not
        // possible across sessions). A guard neither applied nor inherited (temp_file_limit denied by 42501)
        // is simply absent from both lists.
        Console.Error.WriteLine(
            $"DEBUG: session_guard applied: {(appliedGuards.Count > 0 ? string.Join(", ", appliedGuards) : "(none)")}; " +
            $"inherited: {(inheritedGuards.Count > 0 ? string.Join(", ", inheritedGuards) : "(none)")}");
    }

    private sealed class CurrentGuardSettings
    {
        public int? WorkMemKb;
        public int? MaxParallelWorkersPerGather;
        public long? TempFileLimitKb;
    }

    /// <summary>
    /// One combined SHOW-equivalent round trip (pg_settings, tagged `/* darling-export:session_guard */`) for
    /// the three guard settings, so checking whether a guard is already inherited never costs more than one
    /// extra query per connection.
    /// </summary>
    private static async Task<CurrentGuardSettings> ReadCurrentGuardSettingsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var result = new CurrentGuardSettings();

        await using var command = new NpgsqlCommand(
            "/* darling-export:session_guard */ SELECT name, setting, unit FROM pg_settings " +
            "WHERE name IN ('work_mem', 'max_parallel_workers_per_gather', 'temp_file_limit')", connection);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var name = reader.GetString(0);
            var settingText = reader.IsDBNull(1) ? null : reader.GetString(1);
            var unit = reader.IsDBNull(2) ? null : reader.GetString(2);
            if (settingText is null || !long.TryParse(settingText, out var settingValue)) continue;

            // pg_settings reports work_mem/temp_file_limit's unit as "kB" (kibibytes) unless the DBA has
            // overridden it to a coarser unit; anything else is treated as already-in-kB (kB is standard).
            switch (name)
            {
                case "work_mem":
                    result.WorkMemKb = (int)ToKb(settingValue, unit);
                    break;
                case "max_parallel_workers_per_gather":
                    result.MaxParallelWorkersPerGather = (int)settingValue;
                    break;
                case "temp_file_limit":
                    result.TempFileLimitKb = settingValue < 0 ? settingValue : ToKb(settingValue, unit);
                    break;
            }
        }
        return result;
    }

    private static long ToKb(long value, string? unit) => unit switch
    {
        "kB" or null => value,
        "MB" => value * 1024,
        "GB" => value * 1024 * 1024,
        "8kB" => value * 8,
        _ => value,
    };
}
