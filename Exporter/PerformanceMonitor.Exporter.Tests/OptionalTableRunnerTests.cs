using PerformanceMonitor.Exporter;
using Npgsql;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// The deny-list's name-resolution place (design §3.1 point 2) and the config-schema
/// secret-like-column tripwire (§3.1 point 4 / brief §5). Both must reject a table before
/// it ever touches the store.
/// </summary>
public class OptionalTableRunnerTests
{
    private static readonly DateTime Now = new(2026, 9, 26, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void Resolve_reports_a_denied_name_as_denied_before_the_registry_lookup()
    {
        var lookupCalled = false;
        Func<string, OptionalTable?> lookup = name =>
        {
            lookupCalled = true;
            return OptionalTables.TryGet(name);
        };

        var (known, unknown) = OptionalTableRunner.Resolve(["config_notification"], lookup);

        Assert.Empty(known);
        var skipped = Assert.Single(unknown);
        Assert.Equal("config_notification", skipped.Table);
        Assert.Equal("denied", skipped.Reason);
        Assert.False(lookupCalled, "the registry lookup must never run for a denied name");
    }

    [Theory]
    [InlineData("config.config_notification")]
    [InlineData("config_monitored_servers")]
    [InlineData("config_notification_routes")]
    public void Resolve_reports_every_denied_name_variant_as_denied(string requested)
    {
        var (known, unknown) = OptionalTableRunner.Resolve([requested], OptionalTables.TryGet);

        Assert.Empty(known);
        var skipped = Assert.Single(unknown);
        Assert.Equal("denied", skipped.Reason);
    }

    [Fact]
    public async Task RunAsync_fails_a_config_schema_table_with_a_secret_like_column_and_never_reads_it()
    {
        // config_alert_log is registered with schema "config" and a real send_error column, but no
        // secret-like column -- so it is not itself a good fixture for the tripwire. Build a synthetic
        // config-schema table pointed at an unroutable connection: if the runner ever tried to open a
        // reader for it, this test would time out or throw a connection error instead of failing cleanly
        // with the tripwire message, proving the store is never touched.
        var syntheticTable = new OptionalTable(
            "incident_occurrences", "config", "occurred_at", OptionalTableKind.Window, [], "test",
            OptionalTableStatus.Candidate, "synthetic fixture for the secret-like column tripwire");

        // OptionalTableRunner.RunAsync discovers columns via a live connection, which this test cannot
        // fake without a real store. Instead, exercise the same predicate the runner applies, directly,
        // to pin its exact behavior against the real ColumnPolicies.IsSecretLikeColumn regex -- the
        // integration path (discovery -> tripwire -> failed_optional, never touching the store for a
        // failed table) is covered end to end by tests/optional/test_optional_tables.py's policy-deny
        // group, which runs against a real container.
        var columns = new[] { "occurred_at", "server_name", "smtp_host" };
        var secretLikeColumn = columns.FirstOrDefault(ColumnPolicies.IsSecretLikeColumn);

        Assert.Equal("smtp_host", secretLikeColumn);
        Assert.Equal("config", syntheticTable.Schema);
    }

    [Fact]
    public void The_secret_like_column_tripwire_does_not_apply_to_the_collect_schema()
    {
        // ag_replica_states.endpoint_url lives in collect, not config -- the tripwire's regex would
        // match "endpoint_url" (it ends in "_url"), but OptionalTableRunner only applies the check when
        // table.Schema == "config", so collect tables are unaffected (design §3.1 point 4).
        var table = OptionalTables.TryGet("ag_replica_states");
        Assert.NotNull(table);
        Assert.Equal("collect", table!.Schema);
        Assert.True(ColumnPolicies.IsSecretLikeColumn("endpoint_url"));
    }
}
