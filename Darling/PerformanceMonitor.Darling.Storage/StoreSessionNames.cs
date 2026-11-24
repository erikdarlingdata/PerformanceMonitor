/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The application name each background pass and each collector puts on its STORE connection (#9416), under the service's
/// own prefix. During an incident the store's <c>pg_stat_activity</c>, its lock waits and any log line that carries the
/// application name (<c>%a</c>) say which step is running, instead of one name for all of the service's work.
///
/// <para>Every name starts with <see cref="ServicePrefix"/>, which itself starts with
/// <c>PerformanceMonitor.Common.MonitorApplicationNames.SqlProgramNamePrefix</c> and with the
/// <c>PerformanceMonitorDarling</c> prefix the remediation guards match, so every reader that recognises the service's own
/// sessions by prefix still does.</para>
///
/// <para><b>Why a name per use and not a pool per name.</b> Npgsql keys a pool by connection string, so one pool per
/// name would put a pool of up to <c>Maximum Pool Size</c> connections behind each of about thirty steps, against the store's
/// <c>max_connections</c>. <see cref="OpenConnectionAsync(NpgsqlDataSource, string, CancellationToken)"/> instead leases from
/// the one service pool and sets the name on the leased connection. Npgsql resets a pooled connection when it is returned
/// (<c>DISCARD ALL</c>, sent with the next use), and a reset puts <c>application_name</c> back to the value the connection
/// started with, so the next user of that connection never inherits this step's name. An idle pooled connection can keep its
/// last step's name until it is next leased; its <c>state</c> column says it is idle.</para>
/// </summary>
public static class StoreSessionNames
{
    /// <summary>The service's store connection name, which the pooled connections start with. Every step name begins with it.</summary>
    public const string ServicePrefix = "PerformanceMonitorDarling-Service";

    /// <summary>PostgreSQL cuts <c>application_name</c> to this many bytes (NAMEDATALEN - 1). Names are ASCII, so bytes are characters.</summary>
    public const int MaxLength = 63;

    private const string Slash = ServicePrefix + "/";

    // Hourly store-maintenance tick steps (DarlingWorker.Passes.cs and DarlingWorker.Housekeeping.cs).
    public const string TimescaleReprobe = Slash + "timescale-reprobe";
    public const string CompressionHealth = Slash + "compression-health";
    public const string RetentionReevaluate = Slash + "retention-reevaluate";
    public const string StoreConverge = Slash + "store-converge";
    public const string RoleProvisioning = Slash + "role-provisioning";
    public const string PgIoHourly = Slash + "pg-io-hourly";
    public const string ModuleMap = Slash + "module-map";
    public const string PlanRegression = Slash + "plan-regression";
    public const string QueryStoreTopDaily = Slash + "qs-top-daily";
    public const string ObjectExecutionsDaily = Slash + "object-exec-daily";
    public const string ObjectReferencesDaily = Slash + "object-refs-daily";
    public const string JobOutcomeDaily = Slash + "job-outcome-daily";
    public const string ObjectUsageChain = Slash + "object-usage-chain";
    public const string HoleRepair = Slash + "hole-repair";
    public const string BaselineBackfill = Slash + "baseline-backfill";
    public const string RollupWarm = Slash + "rollup-warm";
    public const string AnalysisInput = Slash + "analysis-input";

    // Startup, before the collection loop runs.
    public const string Startup = Slash + "startup";

    // Timers of their own.
    public const string ComposeRollup = Slash + "compose-rollup";
    public const string ComposeStuckCheck = Slash + "compose-stuck-check";
    public const string QueryStoreClosedDay = Slash + "qs-closed-day";
    public const string QueryStatsHostHourly = Slash + "qstats-host-hourly";
    public const string StoreMetrics = Slash + "store-metrics";
    public const string StoreSettings = Slash + "store-settings";
    public const string StoreSize = Slash + "store-size";
    public const string CheckpointSample = Slash + "checkpoint-sample";
    public const string SampleFlush = Slash + "sample-flush";
    public const string SelfAlert = Slash + "self-alert";
    public const string Investigation = Slash + "investigation";
    public const string AlertEngine = Slash + "alert-engine";
    public const string PerfmonRegroup = Slash + "perfmon-regroup";

    // Retention.
    public const string RetentionPurge = Slash + "retention-purge";
    public const string PurgeNow = Slash + "purge-now";
    public const string FindingCleanup = Slash + "finding-cleanup";

    // Collector side work that has no collector of its own.
    public const string CollectorState = Slash + "collector-state";

    private const string CollectPrefix = Slash + "collect-";

    /// <summary>
    /// The name for a collector's store writes: <c>PerformanceMonitorDarling-Service/collect-&lt;collector&gt;</c>, cut to
    /// <see cref="MaxLength"/> so the name the store holds is the name written here and not a silent cut.
    /// </summary>
    public static string ForCollector(string collectorName)
    {
        if (string.IsNullOrWhiteSpace(collectorName))
        {
            return CollectorState;
        }

        var name = CollectPrefix + collectorName;
        return name.Length <= MaxLength ? name : name[..MaxLength];
    }

    /// <summary>
    /// Leases a connection from <paramref name="source"/> and names it <paramref name="applicationName"/> for this use. A
    /// source whose own application name is not the service's (an operator's connection string that names its own, which
    /// <see cref="DarlingStoreConnection.WithApplicationName"/> keeps) is left as it is, and so is a server that does not
    /// report <c>application_name</c>.
    /// </summary>
    public static async ValueTask<NpgsqlConnection> OpenConnectionAsync(
        this NpgsqlDataSource source, string applicationName, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentException.ThrowIfNullOrEmpty(applicationName);

        var connection = await source.OpenConnectionAsync(cancellationToken);
        try
        {
            if (connection.PostgresParameters.TryGetValue("application_name", out var current)
                && current.StartsWith(ServicePrefix, StringComparison.Ordinal))
            {
                await using var command = new NpgsqlCommand("SELECT pg_catalog.set_config('application_name', $1, false)", connection);
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = applicationName });
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }
}
