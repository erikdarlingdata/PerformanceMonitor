/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One Recommendations-tab list row: a persisted <see cref="AnalysisFinding"/> banded and
/// titled the way Lite's recommendations reader maps it (same severity cutoffs, same
/// headline-or-root-fact-key title, same display sort), carrying the underlying finding for
/// the detail pane. Advise-only — the viewer renders the stored remediation action as
/// read-only JSON; there is no Apply here.
/// </summary>
public sealed class ViewerFindingRow
{
    public required string SeverityBand { get; init; }

    public required double RawSeverity { get; init; }

    public required string Title { get; init; }

    public required string Category { get; init; }

    public required string DatabaseName { get; init; }

    public required AnalysisFinding Finding { get; init; }

    /// <summary>The batch's analysis time in the viewer machine's local time.</summary>
    public DateTime AnalysisTimeLocal => ViewerTimeHelper.ForDisplay(Finding.AnalysisTime);

    /// <summary>
    /// True when this finding's story pattern is in <c>analysis_muted</c> for the server (the
    /// engine will drop it on the next analysis run; it stays in this already-persisted batch,
    /// shown muted). Set after the mapping from the muted-story registry, so <see cref="MapFindings"/>
    /// stays pure.
    /// </summary>
    public bool IsMuted { get; set; }

    /// <summary>The <c>mute_id</c> of the matching registry row when <see cref="IsMuted"/>, for unmute.</summary>
    public long? MuteId { get; set; }

    /// <summary>Grid cell text for the muted state — "Muted" or blank.</summary>
    public string MutedLabel => IsMuted ? "Muted" : "";
}

/// <summary>
/// The per-server analysis-state marker (V19) the Darling service writes after each analysis pass:
/// whether the pass hit the 24h data-span gate (<see cref="InsufficientData"/> true, with the engine's
/// <see cref="Message"/>) or completed on enough data (false). The Recommendations tab reads it so a
/// zero-finding young deployment shows "still collecting" instead of a false all-clear. A read yields
/// <c>null</c> when no pass has run for the server yet (no row), which the tab treats as "not
/// insufficient" — an explicit marker is required to show the collecting state.
/// </summary>
public sealed record AnalysisStateMarker(bool InsufficientData, string? Message, DateTime AnalysisTimeUtc);

public sealed partial class ViewerDataService
{
    private PgFindingStore? _findingStore;

    private static readonly JsonSerializerOptions s_indentedJson = new() { WriteIndented = true };

    /// <summary>
    /// The per-server analysis-state marker (V19). Bare <c>analysis_state</c> resolves through the
    /// database-default search path to <c>collect.analysis_state</c> (the same schema the viewer reads
    /// <c>analysis_findings</c> from).
    /// </summary>
    public const string GetAnalysisStateSql = @"
SELECT insufficient_data, message, analysis_time
FROM analysis_state
WHERE server_id = $1";

    /// <summary>
    /// Reads the per-server analysis-state marker the Darling service writes after each analysis pass
    /// (<c>DarlingObservability.WriteAnalysisStateAsync</c>). Returns <c>null</c> when no pass has run for
    /// the server yet (no row) — the tab then treats a zero-finding read as a genuine all-clear. Degrades
    /// to <c>null</c> on any read error (a store not yet migrated to V19, a transient failure), never
    /// throwing, matching the finding read's read-degrades discipline so the tab never crashes on it.
    /// </summary>
    public async Task<AnalysisStateMarker?> GetAnalysisStateAsync(int serverId)
    {
        try
        {
            await using var command = _dataSource.CreateCommand(GetAnalysisStateSql);
            command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });

            await using var reader = await command.ExecuteReaderAsync();
            if (!await reader.ReadAsync())
            {
                return null;
            }

            var insufficient = !reader.IsDBNull(0) && reader.GetBoolean(0);
            var message = reader.IsDBNull(1) ? null : reader.GetString(1);
            /* Stored naive-UTC; tag Utc on read so the kind is explicit (the store-wide read discipline). */
            var analysisTime = DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc);
            return new AnalysisStateMarker(insufficient, message, analysisTime);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// The latest analysis run's findings for one server, mapped to display rows. Reads
    /// through the shared <see cref="PgFindingStore"/> (its <c>GetLatestFindingsSql</c> —
    /// the MAX(analysis_time) batch, severity descending, including each row's persisted
    /// <c>remediation_action_json</c>), so the viewer binds the exact store contract the
    /// analysis pipeline writes. Note the store's read degrades to an empty list on error
    /// (its twins' discipline); the tab then shows the analysis-cadence empty state.
    /// </summary>
    public async Task<List<ViewerFindingRow>> GetLatestFindingsAsync(int serverId)
    {
        _findingStore ??= new PgFindingStore(_dataSource);
        var findings = await _findingStore.GetLatestFindingsAsync(serverId);
        var rows = MapFindings(findings);

        /* Mark rows whose story pattern is already muted so the tab shows the state and can offer
           the unmute. Muting doesn't retract this persisted batch — the engine filters the pattern
           on the NEXT analysis run — so a just-muted finding stays visible here, flagged "Muted".

           GetMutedStoriesAsync spans this server's mutes AND global ones (server_id IS NULL, plus legacy
           server_id = 0 all-servers rows). Only a PER-SERVER mute carries a MuteId here — the viewer
           offers unmute for those — while a global mute flags the finding muted but is left un-unmutable,
           because deleting it would unmute the pattern for EVERY server (a scope the per-server viewer
           must not silently change). A real server_id is never 0, so the m.ServerId == serverId check
           below routes both NULL and legacy-0 rows to the global branch. Among duplicate per-server rows
           for one hash the smallest mute_id wins, so the choice is stable. */
        var muted = await _findingStore.GetMutedStoriesAsync(serverId);
        if (muted.Count > 0)
        {
            var perServerMuteId = new Dictionary<string, long>(StringComparer.Ordinal);
            var globallyMuted = new HashSet<string>(StringComparer.Ordinal);
            foreach (var m in muted)
            {
                if (m.ServerId == serverId)
                {
                    if (!perServerMuteId.TryGetValue(m.StoryPathHash, out var existing) || m.MuteId < existing)
                    {
                        perServerMuteId[m.StoryPathHash] = m.MuteId;
                    }
                }
                else
                {
                    globallyMuted.Add(m.StoryPathHash);
                }
            }

            foreach (var row in rows)
            {
                var hash = row.Finding.StoryPathHash;
                if (perServerMuteId.TryGetValue(hash, out var muteId))
                {
                    row.IsMuted = true;
                    row.MuteId = muteId;
                }
                else if (globallyMuted.Contains(hash))
                {
                    row.IsMuted = true; /* MuteId stays null — global mute, not unmutable from here. */
                }
            }
        }

        return rows;
    }

    /// <summary>
    /// Mutes a finding's story pattern for the selected server so the engine drops it on the next
    /// analysis run — the viewer's user-initiated write, straight to Postgres through the SAME
    /// <see cref="PgFindingStore.MuteStoryAsync"/> the MCP <c>mute_analysis_finding</c> tool uses.
    /// The viewer always mutes per-selected-server (a real non-zero server_id), so it never takes the
    /// MCP "all servers" path (which now persists a NULL server_id — see
    /// <see cref="PgFindingStore.GetMutedStoriesAsync"/>).
    ///
    /// <para>Under a read-only (<c>viewer</c>-role) connection this write degrades gracefully already:
    /// <see cref="PgFindingStore.MuteStoryAsync"/> logs and swallows the permission error (its shared
    /// read-and-write no-op discipline, also relied on by the service/MCP), so there is no crash. The
    /// viewer has no finding-mute affordance today; a future one should gate on
    /// <see cref="IsReadOnly"/> like the mute-rule and alert-dismiss surfaces.</para>
    /// </summary>
    public async Task MuteFindingAsync(int serverId, AnalysisFinding finding)
    {
        if (finding is null)
        {
            throw new ArgumentNullException(nameof(finding));
        }

        _findingStore ??= new PgFindingStore(_dataSource);
        await _findingStore.MuteStoryAsync(
            serverId,
            finding.StoryPathHash,
            string.IsNullOrEmpty(finding.StoryPath) ? finding.StoryPathHash : finding.StoryPath,
            reason: "Muted from Recommendations");
    }

    /// <summary>
    /// Removes a mute registry row (<see cref="PgFindingStore.UnmuteStoryAsync"/>), so the pattern
    /// is reported again from the next analysis run.
    /// </summary>
    public async Task UnmuteFindingAsync(long muteId)
    {
        _findingStore ??= new PgFindingStore(_dataSource);
        await _findingStore.UnmuteStoryAsync(muteId);
    }

    /// <summary>
    /// Maps stored findings to display rows and applies Lite's deterministic display order:
    /// severity band descending, then raw severity descending, then database
    /// (case-insensitive), then title. Pure over the input — unit-testable without a store.
    /// </summary>
    public static List<ViewerFindingRow> MapFindings(IReadOnlyList<AnalysisFinding> findings)
    {
        if (findings is null)
        {
            throw new ArgumentNullException(nameof(findings));
        }

        var rows = new List<ViewerFindingRow>(findings.Count);
        foreach (var finding in findings)
        {
            if (finding is null)
            {
                continue;
            }

            rows.Add(new ViewerFindingRow
            {
                SeverityBand = SeverityBand(finding.Severity),
                RawSeverity = finding.Severity,
                Title = FindingTitle(finding),
                Category = finding.Category,
                DatabaseName = finding.DatabaseName ?? "",
                Finding = finding,
            });
        }

        rows.Sort(CompareForDisplay);
        return rows;
    }

    /// <summary>
    /// The engine's double severity (0–~2.0) mapped onto the canonical three bands with the
    /// SAME cutoffs both apps' recommendation readers use: &gt;= 1.5 CRITICAL,
    /// &gt;= 0.75 WARNING, else INFO. Uppercase so <see cref="StatusToBrushConverter"/> bands the
    /// Recommendations grid (its case-insensitive CRITICAL/WARNING/INFO mapping).
    /// </summary>
    public static string SeverityBand(double severity)
    {
        if (severity >= 1.5)
        {
            return "CRITICAL";
        }

        if (severity >= 0.75)
        {
            return "WARNING";
        }

        return "INFO";
    }

    /// <summary>
    /// The list title: the composed advice headline (value-stated advice frozen into
    /// StoryText at analysis time, static block fallback), falling back to the root fact key —
    /// Lite's reader's title mapping.
    /// </summary>
    public static string FindingTitle(AnalysisFinding finding)
    {
        if (finding is null)
        {
            throw new ArgumentNullException(nameof(finding));
        }

        var advice = FactAdvice.GetComposedForFinding(finding);
        return !string.IsNullOrEmpty(advice?.Headline) ? advice!.Headline : finding.RootFactKey;
    }

    /// <summary>
    /// The detail pane's text for one finding: the composed story (headline, investigation,
    /// remediation via <see cref="FactAdvice.GetForFinding"/>, which also derives any
    /// copy-paste T-SQL the stored row supports), then the persisted remediation action
    /// pretty-printed as read-only JSON. No Apply in the viewer — the action is disclosure.
    /// </summary>
    public static string ComposeFindingDetailText(AnalysisFinding finding)
    {
        if (finding is null)
        {
            throw new ArgumentNullException(nameof(finding));
        }

        var advice = FactAdvice.GetForFinding(finding);
        var sb = new StringBuilder();

        sb.AppendLine(!string.IsNullOrEmpty(advice?.Headline) ? advice!.Headline : finding.RootFactKey);
        sb.AppendLine();

        if (advice is null)
        {
            sb.AppendLine($"No advice text is available for fact key '{finding.RootFactKey}'.");
            sb.AppendLine();
        }
        else
        {
            if (!string.IsNullOrEmpty(advice.Investigation))
            {
                sb.AppendLine("Investigation:");
                sb.AppendLine(advice.Investigation);
                sb.AppendLine();
            }

            if (!string.IsNullOrEmpty(advice.Remediation))
            {
                sb.AppendLine("Remediation:");
                sb.AppendLine(advice.Remediation);
                sb.AppendLine();
            }

            if (!string.IsNullOrEmpty(advice.RemediationTsql))
            {
                sb.AppendLine("Suggested T-SQL (copy/paste):");
                sb.AppendLine(advice.RemediationTsql);
                sb.AppendLine();
            }
        }

        var remediationJson = PrettyPrintRemediation(finding.Remediation);
        if (remediationJson is not null)
        {
            sb.AppendLine("Stored remediation action (read-only):");
            sb.AppendLine(remediationJson);
        }

        return sb.ToString().TrimEnd();
    }

    /// <summary>
    /// Pretty-prints a finding's persisted remediation action through the SAME serializer
    /// that wrote the <c>remediation_action_json</c> column
    /// (<see cref="AlertContextSerializer.SerializeAction"/>), re-indented for reading.
    /// Null when the finding carries no action.
    /// </summary>
    public static string? PrettyPrintRemediation(RemediationAction? action)
    {
        var json = AlertContextSerializer.SerializeAction(action);
        if (json is null)
        {
            return null;
        }

        using var document = JsonDocument.Parse(json);
        return JsonSerializer.Serialize(document.RootElement, s_indentedJson);
    }

    /// <summary>
    /// Lite's recommendations display order: band descending, raw severity descending,
    /// database (ordinal, case-insensitive), then title — deterministic regardless of
    /// input order.
    /// </summary>
    private static int CompareForDisplay(ViewerFindingRow x, ViewerFindingRow y)
    {
        int bySeverityBand = BandRank(y.SeverityBand).CompareTo(BandRank(x.SeverityBand));
        if (bySeverityBand != 0)
        {
            return bySeverityBand;
        }

        int byRaw = y.RawSeverity.CompareTo(x.RawSeverity);
        if (byRaw != 0)
        {
            return byRaw;
        }

        int byDb = string.Compare(x.DatabaseName, y.DatabaseName, StringComparison.OrdinalIgnoreCase);
        if (byDb != 0)
        {
            return byDb;
        }

        return string.Compare(x.Title, y.Title, StringComparison.Ordinal);
    }

    private static int BandRank(string band) => band switch
    {
        "CRITICAL" => 2,
        "WARNING" => 1,
        _ => 0,
    };
}
