using System.Xml;
using System.Xml.Linq;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// One row of CPU-scheduler utilization, shredded from a
/// <c>scheduler_monitor_system_health_ring_buffer_recorded</c> event's &lt;data&gt; children.
/// </summary>
public sealed record SchedulerMonitorCpuRecord
{
    public long? RecordId { get; init; }
    public long? ProcessUtilization { get; init; }
    public long? SystemIdle { get; init; }
    public long? MemoryUtilization { get; init; }
    public long? UserModeTime { get; init; }
    public long? KernelModeTime { get; init; }
    public long? PageFaults { get; init; }
    public long? WorkingSetDelta { get; init; }
}

/// <summary>
/// Parses a <c>scheduler_monitor_system_health_ring_buffer_recorded</c> event. The real event's
/// &lt;data&gt; set (verified against 60,478 events on a live store) is exactly id,
/// timestamp, process_utilization, system_idle, user_mode_time, kernel_mode_time, page_faults,
/// working_set_delta, memory_utilization, call_stack; it never carries the scheduler_id/cpu_id/
/// status fields Darling's vendored <c>SystemHealthParser.ParseSchedulerIssue</c> expects (that
/// parser targets a different, non-existent event shape -- erikdarlingdata/PerformanceMonitor#4452),
/// so this event needs its own small reader instead of the vendored one.
/// </summary>
public static class SchedulerMonitorParser
{
    public const string EventType = "scheduler_monitor_system_health_ring_buffer_recorded";

    private static readonly XmlReaderSettings ReaderSettings = new()
    {
        CheckCharacters = false,
        DtdProcessing = DtdProcessing.Prohibit,
    };

    /// <summary>Parses the event XML. Returns null for null/blank or unparseable XML (never throws).</summary>
    public static SchedulerMonitorCpuRecord? Parse(string? eventXml)
    {
        var ev = EventRoot(eventXml);
        if (ev == null)
        {
            return null;
        }

        return new SchedulerMonitorCpuRecord
        {
            RecordId = ParseLong(DataValue(ev, "id")),
            ProcessUtilization = ParseLong(DataValue(ev, "process_utilization")),
            SystemIdle = ParseLong(DataValue(ev, "system_idle")),
            MemoryUtilization = ParseLong(DataValue(ev, "memory_utilization")),
            UserModeTime = ParseLong(DataValue(ev, "user_mode_time")),
            KernelModeTime = ParseLong(DataValue(ev, "kernel_mode_time")),
            PageFaults = ParseLong(DataValue(ev, "page_faults")),
            WorkingSetDelta = ParseLong(DataValue(ev, "working_set_delta")),
        };
    }

    private static XElement? EventRoot(string? xml)
    {
        if (string.IsNullOrWhiteSpace(xml))
        {
            return null;
        }

        try
        {
            using var reader = XmlReader.Create(new StringReader(xml.TrimStart()), ReaderSettings);
            var root = XElement.Load(reader);
            return root.Name.LocalName == "event" ? root : root.DescendantsAndSelf("event").FirstOrDefault();
        }
        catch
        {
            return null;
        }
    }

    private static string? DataValue(XElement ev, string name) =>
        ev.Elements("data")
            .FirstOrDefault(d => string.Equals((string?)d.Attribute("name"), name, StringComparison.Ordinal))
            ?.Element("value")?.Value;

    private static long? ParseLong(string? s) =>
        long.TryParse(s, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var v)
            ? v
            : (long?)null;
}
