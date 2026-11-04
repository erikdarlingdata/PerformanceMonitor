using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class ColumnPolicyTests
{
    // ---- IsDenied ---------------------------------------------------------

    [Theory]
    [InlineData("config_notification")]
    [InlineData("config.config_notification")]
    [InlineData("\"config\".\"config_notification\"")]
    [InlineData("CONFIG.CONFIG_NOTIFICATION")]
    [InlineData("  config.config_notification  ")]
    [InlineData("config_monitored_servers")]
    [InlineData("config_notification_routes")]
    public void IsDenied_matches_bare_qualified_quoted_mixedcase_and_padded_names(string requested)
    {
        Assert.True(ColumnPolicies.IsDenied(requested));
    }

    [Theory]
    [InlineData("config_alert_log")]
    [InlineData("job_history")]
    [InlineData("collect.pg_log_events")]
    [InlineData("")]
    public void IsDenied_is_false_for_non_denied_names(string requested)
    {
        Assert.False(ColumnPolicies.IsDenied(requested));
    }

    // ---- IsSecretLikeColumn -------------------------------------------------

    [Theory]
    [InlineData("password")]
    [InlineData("db_password")]
    [InlineData("routing_key")]
    [InlineData("smtp_host")]
    [InlineData("username")]
    [InlineData("host")]
    [InlineData("proxy_server")]
    [InlineData("PASSWORD")]
    // endpoint_url matches this regex (it ends in "_url"). Scoping the tripwire to the
    // config schema only, so collect.ag_replica_states.endpoint_url is unaffected, is a
    // later lane's job (design §3.1 point 4) — ColumnPolicies has no per-table scoping yet.
    [InlineData("endpoint_url")]
    public void IsSecretLikeColumn_matches_the_tripwire(string column)
    {
        Assert.True(ColumnPolicies.IsSecretLikeColumn(column));
    }

    [Theory]
    [InlineData("message")]
    [InlineData("server_name")]
    [InlineData("duration_ms")]
    [InlineData("send_error")]
    public void IsSecretLikeColumn_does_not_match_ordinary_columns(string column)
    {
        Assert.False(ColumnPolicies.IsSecretLikeColumn(column));
    }

    // ---- For: policy contents ----------------------------------------------

    [Fact]
    public void For_config_alert_log_drops_send_error()
    {
        var policy = ColumnPolicies.For("config_alert_log");

        Assert.NotNull(policy);
        Assert.Equal(1, policy!.Version);
        var rule = Assert.Single(policy.Rules);
        Assert.Equal("send_error", rule.Column);
        Assert.Equal(ColumnAction.Drop, rule.Action);
    }

    [Fact]
    public void For_job_history_keeps_message_when_run_failed_truncated_to_2000()
    {
        var policy = ColumnPolicies.For("job_history");

        Assert.NotNull(policy);
        Assert.Equal(1, policy!.Version);
        var rule = Assert.Single(policy.Rules);
        Assert.Equal("message", rule.Column);
        Assert.Equal(ColumnAction.KeepWhen, rule.Action);
        Assert.Equal("\"run_status\" <> 1", rule.PredicateSql);
        Assert.Equal(2000, rule.TruncateChars);
    }

    [Fact]
    public void For_analysis_findings_drops_finding_id()
    {
        var policy = ColumnPolicies.For("analysis_findings");

        Assert.NotNull(policy);
        Assert.Equal(1, policy!.Version);
        var rule = Assert.Single(policy.Rules);
        Assert.Equal("finding_id", rule.Column);
        Assert.Equal(ColumnAction.Drop, rule.Action);
    }

    [Fact]
    public void For_pg_log_events_keeps_only_the_approved_column_list_including_pid()
    {
        var policy = ColumnPolicies.For("pg_log_events");

        Assert.NotNull(policy);
        Assert.Equal(1, policy!.Version);
        Assert.Empty(policy.Rules);
        Assert.NotNull(policy.KeepOnly);

        var expected = new[]
        {
            "collection_time", "occurred_at", "family", "severity", "sqlstate",
            "database_name", "relation_name", "statement_fingerprint",
            "bytes", "duration_ms", "pages_removed", "pages_remaining",
            "tuples_removed", "tuples_remaining", "buffer_hits", "buffer_misses",
            "buffer_dirtied", "wal_records", "wal_bytes", "is_analyze",
            "server_name", "pid",
        };

        Assert.Equal(expected, policy.KeepOnly);

        // dropped: message, detail, context, user_name, application_name, and raw_line_hash
        // (design §4 and §10 point 6 — pid is added, raw_line_hash stays out).
        Assert.DoesNotContain("message", policy.KeepOnly);
        Assert.DoesNotContain("detail", policy.KeepOnly);
        Assert.DoesNotContain("context", policy.KeepOnly);
        Assert.DoesNotContain("user_name", policy.KeepOnly);
        Assert.DoesNotContain("application_name", policy.KeepOnly);
        Assert.DoesNotContain("raw_line_hash", policy.KeepOnly);
    }

    [Fact]
    public void For_unknown_table_returns_null()
    {
        Assert.Null(ColumnPolicies.For("perfmon_stats"));
    }
}
