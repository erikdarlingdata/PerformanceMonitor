using System.Linq;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>Pins the alert/collector state tables that the 512 MB archive-and-reset restores (#1145).</summary>
public class ArchivePreservedStateTablesTests
{
    [Theory]
    [InlineData("config_mute_rules")]
    [InlineData("dismissed_archive_alerts")]
    [InlineData("config_edge_trigger_watermarks")]
    [InlineData("config_incident_occurrences")]
    [InlineData("config_alert_persistence_state")]
    [InlineData("config_database_state_expected")]
    [InlineData("collector_state")]
    public void PreservedConfigTables_KeepsTable(string table)
    {
        Assert.Contains(table, ArchiveService.PreservedConfigTables);
    }

    [Fact]
    public void PreservedConfigTables_AreDisjointFromArchivableTables()
    {
        var archived = ArchiveService.ArchivableTables.Select(t => t.Table).ToHashSet();
        Assert.DoesNotContain(ArchiveService.PreservedConfigTables, archived.Contains);
    }
}
