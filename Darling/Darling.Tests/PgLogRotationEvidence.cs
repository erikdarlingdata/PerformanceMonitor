using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using PerformanceMonitor.Collectors;

namespace Darling.Tests;

/// <summary>
/// Renders the evidence behind a failed log-rotation count check, so a failure names what was read instead of only
/// "expected 1, actual 2": every row of the wait's backend (with its hash, time and text), which of them the test's
/// identity matched, the resume state going in and coming out, and the target's log directory. The caller builds it
/// only when a check fails.
/// </summary>
internal static class PgLogRotationEvidence
{
    private const int TextLimit = 200;

    internal static string Describe(
        string what,
        IEnumerable<PgLogEvent> rows,
        int waitPid,
        DateTime waitFloorUtc,
        Func<PgLogEvent, bool> isTheWait,
        IReadOnlyDictionary<string, string>? carriedState,
        IReadOnlyDictionary<string, string>? newState,
        string logDirListing)
    {
        var all = rows.ToList();
        var sb = new StringBuilder();
        sb.Append("--- ").Append(what).AppendLine(" ---");
        sb.Append("wait: pid=").Append(waitPid.ToString(CultureInfo.InvariantCulture))
            .Append(" floor=").Append(waitFloorUtc.ToString("O", CultureInfo.InvariantCulture)).AppendLine();
        sb.Append("rows read: ").Append(all.Count.ToString(CultureInfo.InvariantCulture))
            .Append(", matching the wait: ").Append(all.Count(isTheWait).ToString(CultureInfo.InvariantCulture))
            .Append(", distinct hashes among all rows: ").Append(all.Select(r => r.RawLineHash).Distinct().Count().ToString(CultureInfo.InvariantCulture)).AppendLine();

        sb.AppendLine("rows matching the wait's identity, or carrying its pid (M = matched):");
        var shown = 0;
        foreach (var row in all.Where(r => isTheWait(r) || r.Pid == waitPid))
        {
            shown++;
            sb.Append(isTheWait(row) ? "  M " : "    ")
                .Append("hash=").Append(row.RawLineHash)
                .Append(" at=").Append(row.OccurredAtUtc.ToString("O", CultureInfo.InvariantCulture))
                .Append(" pid=").Append(row.Pid.ToString(CultureInfo.InvariantCulture))
                .Append(" severity=").Append(row.Severity)
                .Append(" family=").Append(row.Family)
                .Append(" message=").Append(Clip(row.Message))
                .Append(" detail=").Append(Clip(row.Detail))
                .Append(" context=").Append(Clip(row.Context))
                .AppendLine();
        }

        if (shown == 0)
        {
            sb.AppendLine("  (none)");
        }

        var repeated = all.GroupBy(r => r.RawLineHash).Where(g => g.Count() > 1).ToList();
        sb.Append("hashes that occur more than once: ").Append(repeated.Count.ToString(CultureInfo.InvariantCulture)).AppendLine();
        foreach (var group in repeated)
        {
            sb.Append("  ").Append(group.Key).Append(" x").Append(group.Count().ToString(CultureInfo.InvariantCulture))
                .Append(" message=").Append(Clip(group.First().Message)).AppendLine();
        }

        sb.Append("carried state: ").AppendLine(State(carriedState));
        sb.Append("resulting state: ").AppendLine(State(newState));
        sb.AppendLine("log directory (name, size, modification):");
        sb.AppendLine(string.IsNullOrEmpty(logDirListing) ? "  (not read)" : logDirListing);
        return sb.ToString();
    }

    private static string State(IReadOnlyDictionary<string, string>? state) =>
        state is null || state.Count == 0
            ? "(none)"
            : string.Join("; ", state.OrderBy(p => p.Key, StringComparer.Ordinal).Select(p => p.Key + "=" + p.Value));

    private static string Clip(string? text)
    {
        if (text is null)
        {
            return "<null>";
        }

        var flat = text.Replace("\r", "\\r", StringComparison.Ordinal).Replace("\n", "\\n", StringComparison.Ordinal);
        return flat.Length <= TextLimit ? "\"" + flat + "\"" : "\"" + flat[..TextLimit] + "\"...(" + flat.Length.ToString(CultureInfo.InvariantCulture) + " chars)";
    }
}
