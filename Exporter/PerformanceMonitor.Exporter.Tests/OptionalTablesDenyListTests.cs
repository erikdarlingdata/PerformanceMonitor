using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// The fail-closed deny-list, enforced in four places. This file covers place 1 (registry load) and
/// place 3 (the SQL builder); OptionalTableRunnerTests covers place 2 (name resolution) and
/// the config-schema secret-like-column tripwire.
/// </summary>
public class OptionalTablesDenyListTests
{
    [Fact]
    public void ValidateRegistry_throws_when_an_entry_names_a_denied_table()
    {
        var entries = new[]
        {
            new OptionalTable("config_notification", "config", "created_at", OptionalTableKind.Window, [], "test", OptionalTableStatus.Candidate, "denied"),
        };

        var ex = Assert.Throws<InvalidOperationException>(() => OptionalTables.ValidateRegistry(entries));
        Assert.Contains("denied table in registry", ex.Message);
        Assert.Contains("config.config_notification", ex.Message);
    }

    [Fact]
    public void ValidateRegistry_accepts_a_registry_with_no_denied_entries()
    {
        var entries = new[]
        {
            new OptionalTable("job_history", "collect", "collection_time", OptionalTableKind.Window, [], "test", OptionalTableStatus.Candidate, "not denied"),
        };

        // Must not throw.
        OptionalTables.ValidateRegistry(entries);
    }

    [Fact]
    public void The_real_registry_validates()
    {
        // Reading OptionalTables.All forces its static field initializers, including the
        // registry-validation one -- so this just has to not throw.
        Assert.NotEmpty(OptionalTables.All);
    }

    [Fact]
    public void BuildOptionalSelect_throws_for_a_denied_table()
    {
        var denied = new OptionalTable("config_notification", "config", "created_at", OptionalTableKind.Window, [], "test", OptionalTableStatus.Candidate, "denied");

        var ex = Assert.Throws<InvalidOperationException>(() => SqlBuilder.BuildOptionalSelect(denied, ["created_at", "slack_url"]));
        Assert.Contains("denied table", ex.Message);
        Assert.Contains("config.config_notification", ex.Message);
    }

    [Fact]
    public void BuildOptionalSelect_throws_for_a_denied_table_named_with_the_bare_name()
    {
        var denied = new OptionalTable("config_monitored_servers", "config", null, OptionalTableKind.Snapshot, [], "test", OptionalTableStatus.Candidate, "denied");

        Assert.Throws<InvalidOperationException>(() => SqlBuilder.BuildOptionalSelect(denied, ["server_name"]));
    }
}
