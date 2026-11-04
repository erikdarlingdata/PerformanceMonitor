using System.Text.RegularExpressions;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// What a store-side projection does with one column, before it is ever sent.
/// </summary>
public enum ColumnAction
{
    Drop,
    KeepWhen,
}

/// <summary>
/// One column's action in a <see cref="TablePolicy"/>. <see cref="PredicateSql"/> and
/// <see cref="TruncateChars"/> only apply to <see cref="ColumnAction.KeepWhen"/>.
/// </summary>
public sealed record ColumnRule(string Column, ColumnAction Action, string? PredicateSql = null, int? TruncateChars = null);

/// <summary>
/// The full column policy for one table. <see cref="KeepOnly"/>, when set, is a keep-list: the projection is the
/// discovered columns intersected with the list, so a new store column is dropped by default. <see cref="Version"/> is
/// the policy_version stamped into the manifest; it must be bumped whenever a rule changes.
/// </summary>
public sealed record TablePolicy(IReadOnlyList<ColumnRule> Rules, IReadOnlyList<string>? KeepOnly = null, int Version = 1);

/// <summary>
/// The hard deny-list and per-table column policies for the optional-table export path.
/// </summary>
public static class ColumnPolicies
{
    /// <summary>
    /// Tables that can never be exported, whatever the registry, --also, --tables or
    /// optional-tables.txt say.
    /// </summary>
    public static readonly IReadOnlySet<string> DenyTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "config.config_monitored_servers",
        "config.config_notification",
        "config.config_notification_routes",
    };

    // (?i)(password|_url$|routing_key|^smtp_|^username$|^host$|proxy) — the column-name tripwire. This catches a
    // future config table that holds secrets. NOTE: this regex matches endpoint_url too (it ends in "_url"). Scoping
    // the tripwire to the config schema only (so collect.ag_replica_states.endpoint_url is unaffected) is a later
    // later change.s job — this type has no per-table scoping yet.
    private static readonly Regex SecretLikeColumnRegex = new(
        "(?i)(password|_url$|routing_key|^smtp_|^username$|^host$|proxy)",
        RegexOptions.Compiled);

    /// <summary>
    /// Normalizes quoting, case and whitespace. A bare name (no schema qualifier) matches any
    /// schema in <see cref="DenyTables"/>.
    /// </summary>
    public static bool IsDenied(string requested)
    {
        if (string.IsNullOrWhiteSpace(requested))
        {
            return false;
        }

        var normalized = requested.Trim().Replace("\"", string.Empty);

        foreach (var denied in DenyTables)
        {
            if (string.Equals(normalized, denied, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            var dot = denied.IndexOf('.');
            var deniedBareName = dot >= 0 ? denied[(dot + 1)..] : denied;

            if (string.Equals(normalized, deniedBareName, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The column-name tripwire.</summary>
    public static bool IsSecretLikeColumn(string column)
    {
        return !string.IsNullOrEmpty(column) && SecretLikeColumnRegex.IsMatch(column);
    }

    /// <summary>Returns the column policy for a table, by bare name, or null if none applies.</summary>
    public static TablePolicy? For(string table)
    {
        return table switch
        {
            "config_alert_log" => new TablePolicy(
                Rules: new[] { new ColumnRule("send_error", ColumnAction.Drop) },
                Version: 1),

            "job_history" => new TablePolicy(
                Rules: new[]
                {
                    new ColumnRule("message", ColumnAction.KeepWhen, PredicateSql: "\"run_status\" <> 1", TruncateChars: 2000),
                },
                Version: 1),

            "analysis_findings" => new TablePolicy(
                Rules: new[] { new ColumnRule("finding_id", ColumnAction.Drop) },
                Version: 1),

            "pg_log_events" => new TablePolicy(
                Rules: Array.Empty<ColumnRule>(),
                KeepOnly: new[]
                {
                    "collection_time",
                    "occurred_at",
                    "family",
                    "severity",
                    "sqlstate",
                    "database_name",
                    "relation_name",
                    "statement_fingerprint",
                    "bytes",
                    "duration_ms",
                    "pages_removed",
                    "pages_remaining",
                    "tuples_removed",
                    "tuples_remaining",
                    "buffer_hits",
                    "buffer_misses",
                    "buffer_dirtied",
                    "wal_records",
                    "wal_bytes",
                    "is_analyze",
                    "server_name",
                    "pid",
                },
                Version: 1),

            _ => null,
        };
    }
}
