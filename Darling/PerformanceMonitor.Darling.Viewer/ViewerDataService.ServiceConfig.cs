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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's control-plane reads + writes of <c>config.config_service</c> — the single-row (id=1)
/// service-wide flags the Stage-1 <c>StoreConfigProvider</c> honors: <c>capture_plans</c> (global plan
/// capture), <c>mcp_enabled</c>/<c>mcp_port</c> (the service's embedded MCP), <c>web_enabled</c>/<c>web_port</c>
/// (the read-only web dashboard, #1562), and the observed <c>paused</c> state. Saving the Settings window's
/// Data Collection + MCP + Web sections now drives the running service.
///
/// <para><b>Write shape is UPDATE-only, never INSERT.</b> The service SEEDS this row LAST in its startup
/// (its presence marks the seed complete — the reload gate keys on <c>config_version</c>); if the viewer
/// INSERTed it early, the service would skip seeding the OTHER desired-state sections from darling.json. So
/// the viewer only ever <c>UPDATE ... WHERE id = 1</c> (a no-op affecting zero rows on an unseeded store —
/// correct: nothing to change until the service has seeded). The BEFORE-UPDATE self-bump trigger increments
/// <c>config_version</c> so the service reloads on its next sweep. <c>paused</c> is NOT written here — it is a
/// <c>pause</c>/<c>resume</c> command (see <c>ViewerDataService.ControlCommands.cs</c>) so the imperative
/// intent flows through the command plane; the viewer only READS it here to reflect the toggle's state.</para>
///
/// <para>Same discipline as the sibling write partials: public-const SQL (Darling.Tests pin the shape +
/// parity with <c>StoreConfigProvider.ReadServiceRowAsync</c>), bound <c>$N</c> parameters, routed through
/// <see cref="ExecuteWriteAsync"/> so a read-only seat degrades to <see cref="ViewerReadOnlyException"/>.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The service-wide flags (id=1): paused + the three viewer-owned toggles. Column order matches
    /// the service's <c>ReadServiceRowAsync</c> prefix.</summary>
    public const string ServiceConfigSelectSql =
        "SELECT paused, capture_plans, mcp_enabled, mcp_port, web_enabled, web_port, query_store_backfill_enabled, query_store_text_budget_mb, max_concurrent_sweeps FROM config_service WHERE id = 1";

    /// <summary>Updates ONLY the viewer-owned service flags on the seeded row (never <c>paused</c> — a
    /// command). The self-bump trigger fires <c>config_version</c>. $1 capture_plans, $2 mcp_enabled, $3 mcp_port,
    /// $4 web_enabled, $5 web_port, $6 query_store_backfill_enabled (#2167).</summary>
    public const string ServiceConfigUpdateFlagsSql = @"
UPDATE config_service
SET capture_plans = $1, mcp_enabled = $2, mcp_port = $3, web_enabled = $4, web_port = $5, query_store_backfill_enabled = $6, query_store_text_budget_mb = $7, max_concurrent_sweeps = $8
WHERE id = 1";

    /// <summary>Reads the service-wide flags, or null when the store has not seeded <c>config_service</c> yet
    /// (a pre-Stage-1 store, or the service has not started) — the Settings window then shows viewer defaults
    /// and the Pause toggle reflects "unknown/not paused".</summary>
    public async Task<ServiceConfigRow?> GetServiceConfigAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ServiceConfigSelectSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new ServiceConfigRow
        {
            Paused = reader.GetBoolean(0),
            CapturePlans = reader.GetBoolean(1),
            McpEnabled = reader.GetBoolean(2),
            McpPort = reader.GetInt32(3),
            WebEnabled = reader.GetBoolean(4),
            WebPort = reader.GetInt32(5),
            QueryStoreBackfillEnabled = reader.GetBoolean(6),
            QueryStoreTextBudgetMb = reader.GetInt32(7),
            MaxConcurrentSweeps = reader.GetInt32(8),
        };
    }

    /// <summary>
    /// Whether the service reports itself paused right now (reflected on the Pause/Resume toggle). Fails safe
    /// to <c>false</c> on ANY non-cancellation error (a pre-V17 store, a transient blip) — mirrors
    /// <see cref="IsConfigSeededAsync"/>'s posture so the toggle degrades to "Running" rather than throwing.
    /// </summary>
    public async Task<bool> IsServicePausedAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var row = await GetServiceConfigAsync(cancellationToken);
            return row?.Paused ?? false;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>Updates the viewer-owned service flags (Settings window Save — MCP + web dashboard + global plan
    /// capture). A no-op on an unseeded store (zero rows). Read-only seats throw <see cref="ViewerReadOnlyException"/>.</summary>
    public async Task UpdateServiceFlagsAsync(
        bool capturePlans, bool mcpEnabled, int mcpPort, bool webEnabled, int webPort, bool queryStoreBackfillEnabled, int queryStoreTextBudgetMb, int maxConcurrentSweeps, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ServiceConfigUpdateFlagsSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = capturePlans });
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = mcpEnabled });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = mcpPort });
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = webEnabled });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = webPort });
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = queryStoreBackfillEnabled });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = queryStoreTextBudgetMb });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = maxConcurrentSweeps });
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>
    /// Touches <c>config_service</c> to bump the <c>config_version</c> reload beacon WITHOUT changing any flag
    /// (mirrors the service's <c>reload</c> command SQL) — the self-bump trigger increments the beacon, so the
    /// worker re-reads the store and re-<c>LoadAsync()</c>es its caches on the next sweep. This is how a write
    /// to a <c>config.*</c> table that carries NO statement-level bump trigger — notably
    /// <c>config_mute_rules</c> (F16): its rules live in the service's in-memory <c>MuteRuleService</c> cache,
    /// reloaded only on the beacon — still takes effect live. A no-op on an unseeded store (zero rows).
    /// </summary>
    public const string ConfigReloadSignalSql =
        "UPDATE config_service SET updated_at = (now() AT TIME ZONE 'UTC') WHERE id = 1";

    /// <summary>Bumps the reload beacon (see <see cref="ConfigReloadSignalSql"/>) so the service re-reads the
    /// store on its next sweep — used after a write to a config table without its own bump trigger.</summary>
    public async Task SignalConfigReloadAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ConfigReloadSignalSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        await ExecuteWriteAsync(command, cancellationToken);
    }
}

/// <summary>The viewer-visible <c>config_service</c> flags: the observed <see cref="Paused"/> state plus the
/// viewer-owned toggles (<see cref="CapturePlans"/>, <see cref="McpEnabled"/>/<see cref="McpPort"/>, and the
/// web dashboard <see cref="WebEnabled"/>/<see cref="WebPort"/>). Defaults mirror the V17/V30 DDL.</summary>
public sealed class ServiceConfigRow
{
    public bool Paused { get; set; }
    public bool CapturePlans { get; set; } = true;
    public bool McpEnabled { get; set; }
    public int McpPort { get; set; } = 5152;
    public bool WebEnabled { get; set; }
    public int WebPort { get; set; } = 5153;

    /// <summary>The #2167 Query Store backfill off switch — service reads it live; default on.</summary>
    public bool QueryStoreBackfillEnabled { get; set; } = true;

    /// <summary>The #2164 per-database query_store text budget in MB — service reads it live; default 64.</summary>
    public int QueryStoreTextBudgetMb { get; set; } = 64;

    /// <summary>The #2170 fleet sweep width — service reads it live; default 4.</summary>
    public int MaxConcurrentSweeps { get; set; } = 4;
}
