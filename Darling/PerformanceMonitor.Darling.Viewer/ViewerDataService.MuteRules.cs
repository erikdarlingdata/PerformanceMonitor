/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's user-initiated writes to <c>config_mute_rules</c> — the alert mute-rule CRUD the
/// Alerts tab's "Manage Mute Rules" surface drives. This goes STRAIGHT to Postgres (the store is
/// the coordination point; the running Darling service's shared
/// <see cref="MuteRuleService"/>/<c>PgMuteRuleStore</c> re-reads it on its next load), so there is
/// no IPC to the service.
///
/// <para>
/// The SQL mirrors the service's <c>PerformanceMonitor.Darling.Service.PgMuteRuleStore</c>
/// statement-for-statement — same columns, same $N positional parameters, same naive-UTC timestamp
/// discipline (Npgsql 6+ rejects Kind=Utc against <c>timestamp</c>). That store lives in the Service
/// project, which the viewer does not (and should not) reference, so the shape is duplicated here
/// rather than shared; the ungated <c>ViewerMuteRuleSqlTests</c> pin the two in lockstep.
/// </para>
///
/// <para>
/// Scope note (the cross-app "server_id" question previous agents flagged): <c>config_mute_rules</c>
/// has NO server_id column — a rule scopes to a server by its <c>server_name</c> text
/// (<see cref="MuteRule.ServerName"/>, nullable = all servers), exactly as Lite's
/// <c>DuckDbMuteRuleStore</c>/<c>MuteRule.Matches</c> do. The separate ANALYSIS-FINDING mute path
/// (<c>analysis_muted</c>) instead keys on an integer <c>server_id</c> where NULL = all servers; its
/// MCP "all servers" tool path now persists NULL and honors legacy <c>server_id = 0</c> rows as global
/// (see <see cref="PerformanceMonitor.Darling.Analysis.PgFindingStore.GetMutedStoriesAsync"/>). This
/// mute-RULE surface has no server_id column at all and mirrors Lite exactly.
/// </para>
/// </summary>
public sealed partial class ViewerDataService
{
    /* Exposed const so Darling.Tests can pin the dialect and column parity with PgMuteRuleStore
       without a live Postgres (the DarlingAlertReadAdapter/ViewerWave2 pattern). */

    public const string MuteRulesSelectSql = @"
SELECT id, enabled, created_at_utc, expires_at_utc, reason,
       server_name, metric_name, database_pattern,
       query_text_pattern, wait_type_pattern, job_name_pattern
FROM config_mute_rules
ORDER BY created_at_utc DESC";

    public const string MuteRuleInsertSql = @"
INSERT INTO config_mute_rules
    (id, enabled, created_at_utc, expires_at_utc, reason,
     server_name, metric_name, database_pattern,
     query_text_pattern, wait_type_pattern, job_name_pattern)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";

    public const string MuteRuleUpdateSql = @"
UPDATE config_mute_rules SET
    enabled = $2, expires_at_utc = $3, reason = $4,
    server_name = $5, metric_name = $6, database_pattern = $7,
    query_text_pattern = $8, wait_type_pattern = $9, job_name_pattern = $10
WHERE id = $1";

    public const string MuteRuleSetEnabledSql = "UPDATE config_mute_rules SET enabled = $2 WHERE id = $1";

    public const string MuteRuleDeleteSql = "DELETE FROM config_mute_rules WHERE id = $1";

    /// <summary>The <see cref="MuteRule.Reason"/> stamped on a whole-server silence created from the sidebar's
    /// "Silence This Server" one-click, so it reads clearly in the Manage Mute Rules list.</summary>
    public const string ServerSilenceReason = "Silenced from server list";

    /// <summary>
    /// Builds the whole-server silence rule the sidebar's "Silence This Server" writes: a <see cref="MuteRule"/>
    /// scoped to <paramref name="serverName"/> with every pattern field left null, so it matches EVERY alert for
    /// that server (<see cref="MuteRule.Matches"/> only filters on the fields that are set). No expiry — a silence
    /// stays until "Unsilence" removes it, mirroring Lite's indefinite per-server silence.
    ///
    /// <para><paramref name="serverName"/> must be the server's DISPLAY name, not its host name: the service's
    /// alert engine builds its mute context with <c>ServerName = snapshot.ServerName</c> (the monitored server's
    /// display name), and the alert rows the viewer reads carry that same display name — so a rule keyed on the
    /// display name is what actually suppresses the alerts (matching the existing "Mute This Server" action,
    /// which mutes on the alert row's <c>ServerName</c>).</para>
    /// </summary>
    public static MuteRule BuildServerSilenceRule(string serverName) => new()
    {
        ServerName = serverName,
        Reason = ServerSilenceReason,
        Enabled = true,
        ExpiresAtUtc = null,
        /* MetricName/DatabasePattern/QueryTextPattern/WaitTypePattern/JobNamePattern stay null → matches all. */
    };

    /// <summary>
    /// True when <paramref name="rule"/> is a WHOLE-SERVER silence for <paramref name="serverName"/> — scoped to
    /// that server (case-insensitive) with no narrowing pattern on any other field. This is the shape
    /// <see cref="BuildServerSilenceRule"/> writes, and the predicate "Unsilence" uses to find the rule(s) to
    /// remove (so it targets only blanket silences, never a specific metric/query/db mute the operator authored
    /// for that server). Enabled/expiry are intentionally not part of the match — Unsilence clears any lingering
    /// blanket silence regardless.
    /// </summary>
    public static bool IsWholeServerSilence(MuteRule rule, string serverName) =>
        rule is not null
        && string.Equals(rule.ServerName, serverName, StringComparison.OrdinalIgnoreCase)
        && rule.MetricName is null
        && rule.DatabasePattern is null
        && rule.QueryTextPattern is null
        && rule.WaitTypePattern is null
        && rule.JobNamePattern is null;

    /// <summary>All mute rules, newest first — the "Manage Mute Rules" list. Timestamps come back
    /// tagged Utc (stored naive), so <see cref="MuteRule.ExpiresDisplay"/> converts correctly.</summary>
    public async Task<List<MuteRule>> GetMuteRulesAsync(CancellationToken cancellationToken = default)
    {
        var rules = new List<MuteRule>();

        await using var command = _dataSource.CreateCommand(MuteRulesSelectSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rules.Add(new MuteRule
            {
                Id = reader.GetString(0),
                Enabled = reader.GetBoolean(1),
                CreatedAtUtc = DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc),
                ExpiresAtUtc = reader.IsDBNull(3) ? null : DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Utc),
                Reason = reader.IsDBNull(4) ? null : reader.GetString(4),
                ServerName = reader.IsDBNull(5) ? null : reader.GetString(5),
                MetricName = reader.IsDBNull(6) ? null : reader.GetString(6),
                DatabasePattern = reader.IsDBNull(7) ? null : reader.GetString(7),
                QueryTextPattern = reader.IsDBNull(8) ? null : reader.GetString(8),
                WaitTypePattern = reader.IsDBNull(9) ? null : reader.GetString(9),
                JobNamePattern = reader.IsDBNull(10) ? null : reader.GetString(10),
            });
        }

        return rules;
    }

    /// <summary>Inserts a new mute rule (the Add dialog's Save).</summary>
    public async Task InsertMuteRuleAsync(MuteRule rule, CancellationToken cancellationToken = default)
    {
        if (rule is null)
        {
            throw new ArgumentNullException(nameof(rule));
        }

        await using var command = _dataSource.CreateCommand(MuteRuleInsertSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = rule.Id });
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = rule.Enabled });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = Naive(rule.CreatedAtUtc) });
        AddNullableTimestamp(command, rule.ExpiresAtUtc);
        AddNullableText(command, rule.Reason);
        AddNullableText(command, rule.ServerName);
        AddNullableText(command, rule.MetricName);
        AddNullableText(command, rule.DatabasePattern);
        AddNullableText(command, rule.QueryTextPattern);
        AddNullableText(command, rule.WaitTypePattern);
        AddNullableText(command, rule.JobNamePattern);
        await ExecuteWriteAsync(command, cancellationToken);
        await SignalConfigReloadAsync(cancellationToken);
    }

    /// <summary>Updates an existing mute rule (the Edit dialog's Save).</summary>
    public async Task UpdateMuteRuleAsync(MuteRule rule, CancellationToken cancellationToken = default)
    {
        if (rule is null)
        {
            throw new ArgumentNullException(nameof(rule));
        }

        await using var command = _dataSource.CreateCommand(MuteRuleUpdateSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = rule.Id });
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = rule.Enabled });
        AddNullableTimestamp(command, rule.ExpiresAtUtc);
        AddNullableText(command, rule.Reason);
        AddNullableText(command, rule.ServerName);
        AddNullableText(command, rule.MetricName);
        AddNullableText(command, rule.DatabasePattern);
        AddNullableText(command, rule.QueryTextPattern);
        AddNullableText(command, rule.WaitTypePattern);
        AddNullableText(command, rule.JobNamePattern);
        await ExecuteWriteAsync(command, cancellationToken);
        await SignalConfigReloadAsync(cancellationToken);
    }

    /// <summary>Toggles a rule's enabled flag without rewriting its other columns.</summary>
    public async Task SetMuteRuleEnabledAsync(string ruleId, bool enabled, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(MuteRuleSetEnabledSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = ruleId });
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = enabled });
        await ExecuteWriteAsync(command, cancellationToken);
        await SignalConfigReloadAsync(cancellationToken);
    }

    /// <summary>Deletes a mute rule by id (the Delete action).</summary>
    public async Task DeleteMuteRuleAsync(string ruleId, CancellationToken cancellationToken = default)
    {
        await DeleteMuteRuleRawAsync(ruleId, cancellationToken);
        await SignalConfigReloadAsync(cancellationToken);
    }

    /// <summary>The bare delete without the reload signal — shared by the single delete and the purge so the
    /// purge signals the beacon ONCE for the whole batch rather than per row.</summary>
    private async Task DeleteMuteRuleRawAsync(string ruleId, CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand(MuteRuleDeleteSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = ruleId });
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>
    /// Deletes every currently-expired rule (the "Purge Expired" action). Returns the number
    /// removed. Expiry is evaluated in memory from the read (matching Lite's
    /// <see cref="MuteRuleService.PurgeExpiredRulesAsync"/> flow: load, pick expired, delete).
    /// Signals the reload beacon once if anything was removed so the service drops the rules live (F16).
    /// </summary>
    public async Task<int> PurgeExpiredMuteRulesAsync(CancellationToken cancellationToken = default)
    {
        var rules = await GetMuteRulesAsync(cancellationToken);
        var removed = 0;
        foreach (var rule in rules)
        {
            if (rule.IsExpired)
            {
                await DeleteMuteRuleRawAsync(rule.Id, cancellationToken);
                removed++;
            }
        }

        if (removed > 0)
        {
            await SignalConfigReloadAsync(cancellationToken);
        }

        return removed;
    }

    /// <summary>Npgsql 6+ rejects Kind=Utc against `timestamp`; the values are UTC by contract.</summary>
    private static DateTime Naive(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static void AddNullableTimestamp(NpgsqlCommand command, DateTime? value) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = value.HasValue ? Naive(value.Value) : DBNull.Value,
        });

    private static void AddNullableText(NpgsqlCommand command, string? value) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Text,
            Value = (object?)value ?? DBNull.Value,
        });
}
