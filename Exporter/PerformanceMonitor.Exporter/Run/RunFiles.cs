using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Exporter.Run;

/// <summary>The daily log (R21) and retention/cleanup (R22, R23). Every cleanup step swallows its own errors.</summary>
public static class RunFiles
{
    public static string DailyLogPath(string logDir, string store, DateTime nowUtc) =>
        Path.Combine(logDir, $"export-{store}-{nowUtc.ToString("yyyyMMdd", CultureInfo.InvariantCulture)}.log");

    public static string RunStamp(DateTime nowUtc) => nowUtc.ToString("yyyyMMddTHHmmssZ", CultureInfo.InvariantCulture);

    /// <summary>UTF-8 without a BOM, appended. The caller holds the run lock, so two runs never interleave blocks.</summary>
    public static void AppendDaily(string path, string text)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.AppendAllText(path, text, new UTF8Encoding(false));
    }

    /// <summary>The text of one per-run file, or "" when missing or empty; always ends in one newline when not empty.</summary>
    public static string SectionText(string? path)
    {
        if (string.IsNullOrEmpty(path) || !File.Exists(path)) return "";
        var t = File.ReadAllText(path).TrimEnd('\r', '\n');
        return t.Length == 0 ? "" : t + "\n";
    }

    public static string SkipBlock(DateTime nowUtc, string store, string message) =>
        $"===== run {RunStamp(nowUtc)} start export {store}\n{message}\n===== run {RunStamp(nowUtc)} end exit=0 elapsed=0s\n";

    /// <summary>
    /// Daily files (`export-&lt;store&gt;-yyyyMMdd.log`) are deleted when their date is more than keepDays before nowUtc. Per-run leftovers
    /// (killed runs) are deleted when last written more than leftoverHours before the REAL clock (a --now test hook never moves it).
    /// </summary>
    public static void ClearExpiredLogs(string dir, DateTime nowUtc, DateTime realNowUtc, int keepDays, int leftoverHours, string? postRunName)
    {
        if (!Directory.Exists(dir)) return;
        // Daily files: the export log and the plan-only parity files (`plan-only-std{out,err}-<store>-d.log`,
        // `plan-only-<store>-d.jsonl`, `plan-actual-<store>-d.jsonl`).
        var daily = new Regex(@"^(?:export-.+-(?<d>\d{8})\.log|plan-only-(?:stdout|stderr)-.+-(?<d>\d{8})\.log|plan-(?:actual|only)-.+-(?<d>\d{8})\.jsonl)$");
        // alert-check-*.log is always a leftover: a wrapper script that has its own scheduled task may still write
        // these files to this folder, and no post-run hook owns them.
        var leftoverText = @"^(export-.+\.log(\.err)?|alert-check-.+\.log";
        if (!string.IsNullOrEmpty(postRunName)) leftoverText += "|" + Regex.Escape(postRunName) + @"-.+\.log";
        leftoverText += ")$";
        var leftover = new Regex(leftoverText);
        var dayCutoff = nowUtc.Date.AddDays(-keepDays);
        var fileCutoff = realNowUtc.AddHours(-leftoverHours);
        foreach (var f in new DirectoryInfo(dir).EnumerateFiles())
        {
            try
            {
                var m = daily.Match(f.Name);
                if (m.Success)
                {
                    if (DateTime.TryParseExact(m.Groups["d"].Value, "yyyyMMdd", CultureInfo.InvariantCulture,
                            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var day) && day < dayCutoff)
                    {
                        f.Delete();
                    }
                }
                else if (leftover.IsMatch(f.Name) && f.LastWriteTimeUtc < fileCutoff)
                {
                    f.Delete();
                }
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { }
        }
    }

    /// <summary>Deletes this store's output files older than keepHours, then folders left empty (deepest first). The store folder itself stays.</summary>
    public static void ClearOldExportOutput(string dir, DateTime nowUtc, int keepHours)
    {
        if (!Directory.Exists(dir)) return;
        var cutoff = nowUtc.AddHours(-keepHours);
        try
        {
            foreach (var f in new DirectoryInfo(dir).EnumerateFiles("*", SearchOption.AllDirectories).ToArray())
            {
                try { if (f.LastWriteTimeUtc < cutoff) f.Delete(); }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { }
            }
            foreach (var d in new DirectoryInfo(dir).EnumerateDirectories("*", SearchOption.AllDirectories).OrderByDescending(x => x.FullName.Length).ToArray())
            {
                try { if (!d.EnumerateFileSystemInfos().Any()) d.Delete(); }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { }
    }

    /// <summary>Deletes entries directly under the temp folder with one of the prefixes (files and folders) older than keepHours.</summary>
    public static void ClearStaleTemp(string tempDir, IReadOnlyList<string> prefixes, DateTime nowUtc, int keepHours)
    {
        if (string.IsNullOrEmpty(tempDir) || !Directory.Exists(tempDir)) return;
        var cutoff = nowUtc.AddHours(-keepHours);
        try
        {
            foreach (var e in new DirectoryInfo(tempDir).EnumerateFileSystemInfos().ToArray())
            {
                if (!prefixes.Any(p => e.Name.StartsWith(p, StringComparison.Ordinal))) continue;
                try
                {
                    if (e.LastWriteTimeUtc >= cutoff) continue;
                    if (e is DirectoryInfo di) di.Delete(recursive: true); else e.Delete();
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { }
    }
}
