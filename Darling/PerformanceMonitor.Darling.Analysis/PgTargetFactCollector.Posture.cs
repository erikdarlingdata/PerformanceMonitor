/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// The three durability settings from the LATEST <c>pg_server_config</c> snapshot, joined to the registry
    /// for the engine kind. <c>$1</c> server_id. This family is filled by lane 8 (#3542 step 8, D6).
    ///
    /// <para><b>Why a second read of a snapshot lane 2 also reads.</b> The config family and the posture family
    /// read the same hourly <c>pg_server_config</c> rows, and the plan considered letting the config read emit
    /// the posture facts as a side effect. It does not, on purpose: the whole point of <c>pg_posture</c> is
    /// that nothing in the tuning machinery can reach it, and a shared read is a shared file — the moment one
    /// method emits both sources, a change to how config facts are normalised or gated is a change to how
    /// posture facts are emitted, reviewed by whoever owns config that week. Three rows an hour buys
    /// file-level isolation; the isolation is what the source exists for.</para>
    ///
    /// <para><b>The snapshot, not the window.</b> <c>pg_server_config</c> is a 60-minute schedule
    /// (<c>CollectorScheduleDefaults</c>), and the newest snapshot is the server's configuration NOW whatever
    /// the analysis window is. Pinning <c>collection_time = MAX(collection_time)</c> is the
    /// <c>DarlingPgServerConfigReader.CurrentConfigSql</c> shape verbatim, including its reasoning that an
    /// hours filter would return nothing on a server whose hourly collector last ran just outside the window,
    /// which reads as "this server has no durability settings". The age of the snapshot travels on the fact
    /// (<c>snapshot_age_minutes</c>) so the advice can say how current its statement is.</para>
    ///
    /// <para><b>The three-source exclusion</b> (<c>client</c>, <c>session</c>, <c>override</c>) is the reader's
    /// too, for the reader's reason: <c>pg_settings</c> is a per-backend view, and a row whose source is the
    /// collector's own session describes that connection, not the server. A <c>SET synchronous_commit = off</c>
    /// in the collector's session would otherwise read as the server's policy.</para>
    ///
    /// <para><b>The registry join</b> is the one non-collector table an analysis read may name
    /// (<c>PgTargetFactCollectorTests</c>), and it is here rather than read off the metadata fact emitted one
    /// step earlier because that fact is emitted only when the major is known: a stamped Aurora row with a
    /// NULL major would otherwise be graded as stock and its platform-managed <c>fsync</c> read as the
    /// operator's choice.</para>
    /// </summary>
    public const string PgTargetPostureSql = """
        SELECT
            c.name,
            c.setting,
            c.context,
            coalesce(c.source, 'default'),
            coalesce(c.pending_restart, false),
            c.collection_time,
            s.engine_kind
        FROM pg_server_config AS c
        JOIN servers AS s
          ON s.server_id = c.server_id
        WHERE c.server_id = $1
        AND   c.collection_time = (
                  SELECT MAX(collection_time)
                  FROM pg_server_config
                  WHERE server_id = $1)
        AND   coalesce(c.source, '') NOT IN ('client', 'session', 'override')
        /* V138 (#3691): server-wide rows only. pg_server_config now also holds the per-database and
           per-role overrides, which repeat a setting's NAME under a different scope, and a posture fact is
           emitted per setting NAME - an override row would emit a second fact for the same setting or
           shadow the server-wide one. All three of these are postmaster/sighup-context settings that
           cannot legally be set per database or per role, so in practice the overrides never carry them;
           the filter is spelled anyway because a read whose correctness depends on which settings happen
           to be overridable is a read nobody can check. The inner MAX(collection_time) is per SERVER. */
        AND   c.database_name IS NULL
        AND   c.role_name IS NULL
        AND   c.name IN ('fsync', 'full_page_writes', 'synchronous_commit')
        """;

    /// <summary>
    /// The three durability settings (<c>fsync</c>, <c>full_page_writes</c>, <c>synchronous_commit</c>) as
    /// <see cref="PgTargetSources.PostureSource"/> facts — one per setting the snapshot carries, emitted
    /// whether the setting is on or off, so a target's posture is visible in <c>get_analysis_facts</c> as a
    /// statement and not only as a complaint. The SQL is <see cref="PgTargetPostureSql"/>.
    ///
    /// <para><b>What the fact says.</b> <c>Value</c> is 1 when the setting is <c>off</c> and 0 otherwise — the
    /// finding condition is categorical, as <c>CONFIG_PRIORITY_BOOST</c>'s is on the SQL Server side, and the
    /// only value of <c>synchronous_commit</c> that gives up LOCAL durability is <c>off</c>: <c>local</c>,
    /// <c>remote_write</c> and <c>remote_apply</c> differ in what they wait for on a standby and all keep the
    /// local commit durable. Metadata: <c>managed_by_platform</c> (1 when the engine is Aurora and the setting
    /// is <c>fsync</c> or <c>full_page_writes</c> — Aurora's storage layer owns both and the parameter group
    /// does not expose them, so the value is the platform's and is graded 0 by the scorer, D6);
    /// <c>requires_restart</c> (1 when <c>pg_settings.context</c> is <c>postmaster</c>);
    /// <c>session_settable</c> (1 when the context is <c>user</c> / <c>superuser</c> / <c>backend</c>, i.e. a
    /// per-session <c>SET</c> can override the server default — true of <c>synchronous_commit</c>, false of
    /// the other two, which are <c>sighup</c>: <c>ALTER SYSTEM</c> plus <c>pg_reload_conf()</c>, no restart);
    /// <c>pending_restart</c>; <c>is_default</c> (PostgreSQL's own <c>source = 'default'</c> verdict, for the
    /// reader's reason — never a text comparison against <c>boot_val</c>); <c>snapshot_age_minutes</c> against
    /// the window end, never the wall clock.</para>
    ///
    /// <para><b>What it does not do.</b> No fact when the snapshot has no row for a setting (a pre-V102 store,
    /// or a collector that has not yet run on this server): an absent row makes no claim, and a fabricated
    /// "on" would be the fabricated zero the #3541 A12 sweep removed elsewhere. No rate, so nothing here
    /// divides by the observed window. Nothing here reads the value of any other family's fact.</para>
    /// </summary>
    private async partial Task CollectPostureFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetPostureSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                var name = reader.GetString(0);
                var setting = reader.IsDBNull(1) ? null : reader.GetString(1);
                var settingContext = reader.IsDBNull(2) ? null : reader.GetString(2);
                var source = reader.GetString(3);
                var pendingRestart = reader.GetBoolean(4);
                var collectedAt = reader.GetDateTime(5);
                var engineKind = reader.IsDBNull(6) ? null : reader.GetString(6);

                var key = PostureKeyFor(name);
                if (key is null) continue;

                var isAurora = MonitoredEngineKind.IsAurora(engineKind);
                var managedByPlatform = isAurora && key != PgTargetFactKeys.PostureSynchronousCommit;

                facts.Add(new Fact
                {
                    Source = PgTargetSources.PostureSource,
                    Key = key,
                    Value = IsOff(setting) ? 1 : 0,
                    ServerId = context.ServerId,
                    Metadata =
                    {
                        ["managed_by_platform"] = managedByPlatform ? 1 : 0,
                        ["is_aurora"] = isAurora ? 1 : 0,
                        ["requires_restart"] = string.Equals(settingContext, "postmaster", StringComparison.Ordinal) ? 1 : 0,
                        ["session_settable"] = settingContext is "user" or "superuser" or "backend" ? 1 : 0,
                        ["pending_restart"] = pendingRestart ? 1 : 0,
                        ["is_default"] = string.Equals(source, "default", StringComparison.Ordinal) ? 1 : 0,
                        ["snapshot_age_minutes"] = Math.Max(0, (context.TimeRangeEnd - collectedAt).TotalMinutes),
                    },
                });
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_server_config arrived in V102 — a pre-migration store raises 42P01 here, which the reporter
               classifies quiet. An abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>The setting name → posture key map, closed over the three names the SQL selects; a name the SQL
    /// grew that this map did not is dropped rather than emitted under a guessed key.</summary>
    private static string? PostureKeyFor(string settingName) => settingName switch
    {
        "fsync" => PgTargetFactKeys.PostureFsync,
        "full_page_writes" => PgTargetFactKeys.PostureFullPageWrites,
        "synchronous_commit" => PgTargetFactKeys.PostureSynchronousCommit,
        _ => null,
    };

    /// <summary><c>pg_settings.setting</c> renders every bool as <c>on</c> / <c>off</c> and
    /// <c>synchronous_commit</c> as one of its five enum tokens; <c>off</c> is the one token that means the same
    /// thing for all three. Trimmed and case-folded because the stored text is whatever the collector read.</summary>
    private static bool IsOff(string? setting) =>
        setting is not null && string.Equals(setting.Trim(), "off", StringComparison.OrdinalIgnoreCase);
}
