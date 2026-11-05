using PerformanceMonitor.Exporter;
using PerformanceMonitor.Exporter.Vendor.PerformanceMonitor;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// Parses each real captured system_health fixture through the path the exporter uses and
/// checks the values against the fixture XML read by eye.
/// </summary>
public sealed class SystemHealthTests
{
    private static string FixturePath(string name) =>
        Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", name);

    private static string ReadFixture(string name) => File.ReadAllText(FixturePath(name));

    [Fact]
    public void SchedulerMonitor_parses_expected_fields()
    {
        var xml = ReadFixture("scheduler_monitor.xml");

        var record = SchedulerMonitorParser.Parse(xml);

        Assert.NotNull(record);
        Assert.Equal(0L, record!.RecordId);
        Assert.Equal(10L, record.ProcessUtilization);
        Assert.Equal(88L, record.SystemIdle);
        Assert.Equal(100L, record.MemoryUtilization);
        Assert.Equal(249218750L, record.UserModeTime);
        Assert.Equal(1875000L, record.KernelModeTime);
        Assert.Equal(194L, record.PageFaults);
        Assert.Equal(61440L, record.WorkingSetDelta);
    }

    [Fact]
    public void System_component_parses_expected_fields()
    {
        var xml = ReadFixture("system.xml");

        var record = SystemHealthParser.ParseSystemHealth(xml);

        Assert.NotNull(record);
        Assert.Equal("CLEAN", record!.State);
        Assert.Equal(3377L, record.SpinlockBackoffs);
        Assert.Equal("none", record.SickSpinlockType);
        Assert.Equal(316276L, record.PageFaults);
        Assert.Equal(14L, record.SystemCpuUtilization);
        Assert.Equal(13L, record.SqlCpuUtilization);
        Assert.Equal(0L, record.BadPagesDetected);
    }

    [Fact]
    public void Resource_component_parses_expected_memory_numbers()
    {
        var xml = ReadFixture("resource.xml");

        var record = SystemHealthParser.ParseMemoryConditions(xml);

        Assert.NotNull(record);
        Assert.Equal("RESOURCE_MEMPHYSICAL_HIGH", record!.LastNotification);
        // 14050582528 bytes / 1024^3, truncated.
        Assert.Equal(13L, record.AvailablePhysicalMemoryGb);
        Assert.Equal(656928768L / 1024 / 1024 / 1024, record.WorkingSetGb);
        Assert.Equal(100L, record.PercentOfCommittedMemoryInWs);
        Assert.Equal(562165862L, record.PageFaults);
        Assert.Equal(1L, record.SystemPhysicalMemoryHigh);
        Assert.Equal(0L, record.SystemPhysicalMemoryLow);
    }

    [Fact]
    public void QueryProcessing_component_parses_expected_fields()
    {
        var xml = ReadFixture("query_processing.xml");

        var record = SystemHealthParser.ParseCpuTasks(xml);

        Assert.NotNull(record);
        Assert.Equal(512L, record!.MaxWorkers);
        Assert.Equal(119L, record.WorkersCreated);
        Assert.Equal(48L, record.WorkersIdle);
        Assert.Equal(6021L, record.TasksCompletedWithinInterval);
        Assert.Equal(0L, record.PendingTasks);
        Assert.False(record.HasUnresolvableDeadlockOccurred);
        Assert.False(record.HasDeadlockedSchedulersOccurred);
        Assert.False(record.DidBlockingOccur);
    }

    [Fact]
    public void IoSubsystem_component_parses_to_empty_list_when_no_pending_requests()
    {
        var xml = ReadFixture("io_subsystem.xml");

        var records = SystemHealthParser.ParseIoIssues(xml);

        // The fixture's <longestPendingRequests/> is empty, so sp_HealthParser's WHERE
        // duration IS NOT NULL filter leaves nothing to fan out.
        Assert.Empty(records);
    }

    [Fact]
    public void IoSubsystem_counters_are_read_from_every_event()
    {
        var counters = IoSubsystemParser.Parse(ReadFixture("io_subsystem.xml"));

        Assert.NotNull(counters);
        Assert.Equal(0, counters.IoLatchTimeouts);
        Assert.Equal(0, counters.IntervalLongIos);
        Assert.Equal(0, counters.TotalLongIos);
        Assert.False(string.IsNullOrEmpty(counters.State));
    }

    [Fact]
    public void IoSubsystem_counters_are_null_for_other_components_and_garbage()
    {
        Assert.Null(IoSubsystemParser.Parse(ReadFixture("system.xml")));
        Assert.Null(IoSubsystemParser.Parse("<event name=\"broken\"><data"));
        Assert.Null(IoSubsystemParser.Parse(null));
    }

    [Fact]
    public void ErrorReported_parses_error_number_severity_and_message()
    {
        var xml = ReadFixture("error_reported.xml");

        var record = SystemHealthParser.ParseSevereError(xml);

        Assert.NotNull(record);
        Assert.Equal(17810, record!.ErrorNumber);
        Assert.Equal(20, record.Severity);
        Assert.Equal(2, record.State);
        Assert.Contains("dedicated administrator connection", record.Message);
    }

    [Fact]
    public void Unparseable_xml_returns_null_for_every_parser()
    {
        const string garbage = "<event name=\"error_reported\"";

        Assert.Null(SchedulerMonitorParser.Parse(garbage));
        Assert.Null(SystemHealthParser.ParseSevereError(garbage));
        Assert.Null(SystemHealthParser.ParseSystemHealth(garbage));
        Assert.Null(SystemHealthParser.ParseMemoryConditions(garbage));
        Assert.Null(SystemHealthParser.ParseCpuTasks(garbage));
        Assert.Empty(SystemHealthParser.ParseIoIssues(garbage));
    }

    [Fact]
    public void SystemHealthTables_are_all_selectable_via_ExportOptions()
    {
        Assert.All(SystemHealthTables.All, table => Assert.Contains(table, ExportOptions.AllTables));
    }
}
