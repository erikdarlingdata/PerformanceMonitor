using System.Xml;
using System.Xml.Linq;

namespace PerformanceMonitor.Exporter;

/// <summary>The interval counters of one IO_SUBSYSTEM <c>sp_server_diagnostics_component_result</c> event.</summary>
public sealed record IoSubsystemCounters
{
    public string? State { get; init; }
    public long? IoLatchTimeouts { get; init; }
    public long? IntervalLongIos { get; init; }
    public long? TotalLongIos { get; init; }
}

/// <summary>
/// Reads the <c>&lt;ioSubsystem&gt;</c> node's counters from an IO_SUBSYSTEM event, for every event, whether or not it
/// carries pending requests.
/// </summary>
public static class IoSubsystemParser
{
    private static readonly XmlReaderSettings ReaderSettings = new()
    {
        CheckCharacters = false,
        DtdProcessing = DtdProcessing.Prohibit,
    };

    /// <summary>Returns null for blank or unparseable XML, or an event with no <c>&lt;ioSubsystem&gt;</c> node.</summary>
    public static IoSubsystemCounters? Parse(string? eventXml)
    {
        if (string.IsNullOrWhiteSpace(eventXml))
        {
            return null;
        }

        XElement root;
        try
        {
            using var reader = XmlReader.Create(new StringReader(eventXml.TrimStart()), ReaderSettings);
            root = XElement.Load(reader);
        }
        catch (XmlException)
        {
            return null;
        }

        var io = root.Descendants("ioSubsystem").FirstOrDefault();
        if (io == null)
        {
            return null;
        }

        var state = root.Elements("data")
            .FirstOrDefault(d => string.Equals((string?)d.Attribute("name"), "state", StringComparison.Ordinal))
            ?.Element("text")?.Value;
        return new IoSubsystemCounters
        {
            State = state,
            IoLatchTimeouts = ParseLong((string?)io.Attribute("ioLatchTimeouts")),
            IntervalLongIos = ParseLong((string?)io.Attribute("intervalLongIos")),
            TotalLongIos = ParseLong((string?)io.Attribute("totalLongIos")),
        };
    }

    private static long? ParseLong(string? s) =>
        long.TryParse(s, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var v)
            ? v
            : null;
}
