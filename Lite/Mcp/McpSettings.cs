using System.Globalization;
using System.IO;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The MCP endpoint's two settings, read out of the same settings.json every other Lite setting lives in.
///
/// <para><b>What a broken settings.json does to this endpoint, and why (#2431).</b> The endpoint stays
/// OFF, Lite still starts, and every reader of this type is told which capability that cost. All three
/// halves of that were decided rather than inherited from <c>new McpSettings()</c>.</para>
///
/// <para><i>Off rather than on.</i> These two keys are consent and address for a TCP listener. A file we
/// cannot read supplies neither, so starting anyway means opening a port nobody in this session asked
/// for, at an address that is a guess — and Lite's firewall rule follows the configured port (#2414), so
/// the guess is not even the port a client would be aimed at. Failing closed is the only defensible
/// answer at a boundary whose configuration is unavailable.</para>
///
/// <para><i>But not by refusing to start.</i> The alternative the issue asks about — treat a
/// present-but-broken file as fatal — trades a small outage for a larger one. Lite IS the collector: a
/// process that will not start collects nothing, alerts on nothing, and leaves a hole in the history that
/// fixing the JSON afterwards does not backfill. It also removes the window the person who has to repair
/// the file would use to find out what is wrong with it. Repair needs that machine either way, so
/// refusing to start shortens nobody's path to the fix; it only deletes every other capability while
/// somebody walks it. #2425 made the same call for the other eighty-eight settings, and one file must not
/// have two failure modes depending on which loader happened to read it first.</para>
///
/// <para><i>So the fix is saying so.</i> Off was never the defect — silence was. A disabled endpoint
/// surfaces as a refused connection on a different machine, against an app that reports itself healthy,
/// and nobody debugging that has a reason to suspect a comma in a JSON file on the monitored workstation.
/// <see cref="Problem"/> therefore leaves this class instead of being swallowed, and each of the three
/// call sites names the lost capability rather than the lost settings.</para>
/// </summary>
internal sealed class McpSettings
{
    /// <summary>
    /// The port used when settings.json does not name one. Also the value <see cref="Port"/> carries when
    /// the file could not be read — which is a fallback and not a reading of the file, so a caller that
    /// displays it must check <see cref="DisabledByUnreadableSettings"/> first.
    /// </summary>
    public const int DefaultPort = 5151;

    public bool Enabled { get; set; } = false;
    public int Port { get; set; } = DefaultPort;

    /// <summary>
    /// Why the MCP configuration could not be read, or null when it could.
    ///
    /// <para>Non-null covers two shapes that produce the identical user-visible outcome: settings.json is
    /// present and unparseable, or it parses and one of these two keys holds a value of the wrong type.
    /// Both end with the endpoint off through no decision of the operator's, which is the thing that has
    /// to be said out loud. An ABSENT file is deliberately not one of them — a first run has no
    /// settings.json, off is the correct answer, and there is no endpoint to have lost.</para>
    /// </summary>
    public string? Problem { get; private set; }

    /// <summary>
    /// True when <see cref="Enabled"/> is false because the file could not be read rather than because
    /// anyone turned the endpoint off. The distinction is the whole of #2431: the two states are identical
    /// in behaviour and opposite in meaning.
    /// </summary>
    public bool DisabledByUnreadableSettings => Problem is not null;

    public static McpSettings Load(string configDirectory)
    {
        var path = Path.Combine(configDirectory, "settings.json");
        var read = SettingsFileGuard.Read(path);

        /* Absent is the one case that stays silent, and #2425's guard exists to keep it separable from
           the others: a first run has no file, defaults are right, and a warning would be pure noise. */
        if (read.State == SettingsFileState.Absent)
        {
            return new McpSettings();
        }

        if (read.State == SettingsFileState.Unreadable || read.Text is null)
        {
            return Lost(read.Problem ?? "the reason could not be determined");
        }

        JsonDocument doc;
        try
        {
            /* The guard already parsed this exact text with JsonNode and default options, so JsonDocument
               cannot disagree with it — but the old bare catch here is the bug being fixed, so the
               unreachable arm is routed to the loud path rather than back to silent defaults. */
            doc = JsonDocument.Parse(read.Text);
        }
        catch (JsonException ex)
        {
            return Lost(SettingsFileGuard.Describe(ex));
        }

        using (doc)
        {
            var root = doc.RootElement;

            var settings = new McpSettings();

            if (root.TryGetProperty("mcp_enabled", out var enabled))
            {
                /* Checked by kind rather than caught from GetBoolean, because the catch is what turned a
                   quoted "true" into a silently disabled endpoint. A hand-edited file is exactly where a
                   quoted boolean comes from, and #2418's sample makes hand-editing likelier, not rarer. */
                if (enabled.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                {
                    return Lost(WrongKind("mcp_enabled", enabled, "true or false"));
                }

                settings.Enabled = enabled.GetBoolean();
            }

            if (root.TryGetProperty("mcp_port", out var port))
            {
                if (port.ValueKind != JsonValueKind.Number)
                {
                    return Lost(WrongKind("mcp_port", port, "a port number"));
                }

                if (!port.TryGetInt32(out var portNumber))
                {
                    return Lost(string.Format(CultureInfo.InvariantCulture,
                        "'mcp_port' is {0}, which is not a whole port number", port.GetRawText()));
                }

                settings.Port = portNumber;
            }

            return settings;
        }
    }

    private static McpSettings Lost(string problem) => new() { Problem = problem };

    private static string WrongKind(string key, JsonElement value, string expected) =>
        string.Format(CultureInfo.InvariantCulture, "'{0}' holds a JSON {1} where {2} belongs",
            key, value.ValueKind.ToString().ToLowerInvariant(), expected);
}
