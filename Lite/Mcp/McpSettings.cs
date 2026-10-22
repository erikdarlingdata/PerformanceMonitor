using System.Globalization;
using System.IO;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The MCP endpoint's three settings (on/off, port, and whether the write tools are served), read out of the same
/// settings.json every other Lite setting lives in.
///
/// <para><b>What a broken settings.json does to this endpoint, and why (#2431).</b> The endpoint stays
/// OFF, Lite still starts, and every reader of this type is told which capability that cost. All three
/// halves of that were decided rather than inherited from <c>new McpSettings()</c>.</para>
///
/// <para><i>Off rather than on.</i> These keys are consent, address and permissions for a TCP listener. A file we
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
    /// Whether the MCP write tools (<see cref="McpWritePolicy.WriteTools"/>) are served. Default OFF: the
    /// endpoint answers reads only, and a call to a write tool fails as an unknown tool. Read from the
    /// <c>mcp_allow_writes</c> key (<see cref="McpWritePolicy.WritesSetting"/>), at start, so a change needs a
    /// restart, like <c>mcp_enabled</c>. There is no Settings window control for it: it is a file setting on
    /// purpose, so it is turned on by someone who edited the file rather than by a click.
    /// </summary>
    public bool AllowWrites { get; set; } = false;

    /// <summary>
    /// Whether the four investigation tools (<see cref="McpInvestigationPolicy.InvestigationTools"/>, #5820) are
    /// offered. Default OFF. Read from <c>mcp_allow_investigations</c> (<see cref="McpInvestigationPolicy.InvestigationsSetting"/>)
    /// at start, like <c>mcp_allow_writes</c>. They are served only when this is on AND <see cref="AllowWrites"/> is
    /// off (<see cref="McpInvestigationPolicy.Served"/>): they also need a token, so with mcp_allow_tokenless_loopback on they are not served, and the mute
    /// and route-disable tools are never served alongside them. A file setting only, no Settings window control.
    /// </summary>
    public bool AllowInvestigations { get; set; } = false;

    /// <summary>
    /// Default OFF: once the MCP server is on, every request needs the token (<see cref="McpTokenStore"/>), like Darling's.
    /// On, the server answers local clients without one, as earlier builds did, but serves the read tools only: the write and
    /// investigation tools need a token. Import Settings never brings this value in from another folder. Read from <c>mcp_allow_tokenless_loopback</c>
    /// (<see cref="AllowTokenlessLoopbackSetting"/>) at start, so a change needs a restart. File setting only, no Settings window control.
    /// </summary>
    public bool AllowTokenlessLoopback { get; set; } = false;

    /// <summary>The settings.json key for <see cref="AllowTokenlessLoopback"/>.</summary>
    public const string AllowTokenlessLoopbackSetting = "mcp_allow_tokenless_loopback";

    /// <summary>
    /// How much collected data the MCP answers hand out (#5632 on Darling, #5820 here): full (the default),
    /// maskLiterals or metadataOnly. Read from <c>mcp_data_detail</c> (<see cref="DataDetailSetting"/>) at start;
    /// a word that is not one of the three leaves the endpoint off with the accepted words named, as for the other keys.
    /// </summary>
    public McpDataDetail DataDetail { get; set; } = McpDataDetail.Full;

    /// <summary>The settings.json key for <see cref="DataDetail"/>.</summary>
    public const string DataDetailSetting = "mcp_data_detail";

    /// <summary>
    /// Why the MCP configuration could not be read, or null when it could.
    ///
    /// <para>Non-null covers two shapes that produce the identical user-visible outcome: settings.json is
    /// present and unparseable, or it parses and one of these keys holds a value of the wrong type.
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

            if (root.TryGetProperty("mcp_allow_writes", out var allowWrites))
            {
                /* Same kind check as mcp_enabled: a quoted "true" from a hand edit is loud, not a silent OFF. The
                   endpoint stays off with the problem named, exactly as for the other keys. */
                if (allowWrites.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                {
                    return Lost(WrongKind("mcp_allow_writes", allowWrites, "true or false"));
                }

                settings.AllowWrites = allowWrites.GetBoolean();
            }

            if (root.TryGetProperty("mcp_allow_investigations", out var allowInvestigations))
            {
                if (allowInvestigations.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                {
                    return Lost(WrongKind(McpInvestigationPolicy.InvestigationsSetting, allowInvestigations, "true or false"));
                }

                settings.AllowInvestigations = allowInvestigations.GetBoolean();
            }

            if (root.TryGetProperty("mcp_allow_tokenless_loopback", out var allowTokenless))
            {
                if (allowTokenless.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                {
                    return Lost(WrongKind(AllowTokenlessLoopbackSetting, allowTokenless, "true or false"));
                }

                settings.AllowTokenlessLoopback = allowTokenless.GetBoolean();
            }

            if (root.TryGetProperty("mcp_data_detail", out var dataDetail))
            {
                if (dataDetail.ValueKind != JsonValueKind.String)
                {
                    return Lost(WrongKind(DataDetailSetting, dataDetail, "full, maskLiterals or metadataOnly"));
                }

                /* TryParse treats a blank word as full and any other unknown word as metadataOnly with a false return.
                   Darling runs an unknown word as metadataOnly with a warning; Lite follows its other keys and refuses to
                   start with the accepted words named, so a typo is seen at once and never hands out more than asked for. */
                if (!McpDataDetailModes.TryParse(dataDetail.GetString(), out var mode))
                {
                    return Lost(string.Format(CultureInfo.InvariantCulture,
                        "'{0}' is '{1}', which is not one of full, maskLiterals or metadataOnly",
                        DataDetailSetting, dataDetail.GetString()));
                }

                settings.DataDetail = mode;
            }

            return settings;
        }
    }

    private static McpSettings Lost(string problem) => new() { Problem = problem };

    private static string WrongKind(string key, JsonElement value, string expected) =>
        string.Format(CultureInfo.InvariantCulture, "'{0}' holds a JSON {1} where {2} belongs",
            key, value.ValueKind.ToString().ToLowerInvariant(), expected);
}
