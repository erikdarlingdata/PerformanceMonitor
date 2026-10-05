/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The service-log section of the diagnostics bundle: the recent WARN, ERROR and CRIT entries of the
/// <c>darling-service_yyyyMMdd.log</c> files in a directory. Files open with
/// <c>FileShare.ReadWrite | FileShare.Delete</c> because the logger appends to today's file and retention may delete
/// an old one while this reads. Each file contributes at most <see cref="TailBytes"/> from its end, with the partial
/// first line dropped. A section that cannot be read says why (<c>not_found</c>, <c>no_files_in_window</c>, a per-file
/// <c>error_class</c>) and never fails the verb.
/// </summary>
internal static class DiagnosticsBundleServiceLog
{
    internal const int TailBytes = 8 * 1024 * 1024;
    internal const int MaxEntryChars = 2_000;

    /// <summary>
    /// How much of an entry <see cref="Read"/> keeps for the alias pass. The cut to <see cref="MaxEntryChars"/> comes
    /// AFTER aliasing, so a name straddling the final cut cannot be left as an unrecognizable prefix.
    /// </summary>
    internal const int AliasInputChars = 16_384;
    internal const int MaxEntries = 300;
    private const string FilePrefix = "darling-service_";

    private static readonly Regex s_entryStart = new(
        @"^(?<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \[(?<level>TRACE|DEBUG|INFO |WARN |ERROR|CRIT )\] \[(?<cat>[^\]]*)\] (?<msg>.*)$",
        RegexOptions.CultureInvariant | RegexOptions.Singleline,
        TimeSpan.FromSeconds(1));

    /// <summary>Reads the section. <paramref name="searched"/> names where the directory came from (<c>default</c> or <c>--log-dir</c>); the path itself never enters the bundle.</summary>
    internal static JsonObject Read(string directory, string searched, DateTime windowStartLocal, bool includeEntries = true)
    {
        if (!Directory.Exists(directory))
        {
            return NotFound(searched);
        }

        var files = new List<(string Path, DateTime Date)>();
        try
        {
            foreach (var path in Directory.EnumerateFiles(directory, FilePrefix + "*.log"))
            {
                var stem = System.IO.Path.GetFileNameWithoutExtension(path)[FilePrefix.Length..];
                if (DateTime.TryParseExact(stem, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date)
                    && date >= windowStartLocal.Date)
                {
                    files.Add((path, date));
                }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return new JsonObject { ["status"] = "error", ["searched"] = searched, ["error_class"] = SlowReadLog.ErrorClassOf(ex) };
        }

        if (files.Count == 0)
        {
            return new JsonObject
            {
                ["status"] = "no_files_in_window",
                ["searched"] = searched,
                ["hint"] = "The directory exists but holds no darling-service_yyyyMMdd.log file dated inside the window.",
            };
        }

        var entries = new List<(DateTime Time, string Level, string Category, string Message)>();
        var fileInfo = new JsonArray();
        foreach (var (path, date) in files.OrderByDescending(f => f.Date))
        {
            var info = new JsonObject { ["date"] = date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) };
            try
            {
                var (text, bytes, partial) = ReadTail(path);
                info["bytes"] = bytes;
                info["tail_only"] = partial;
                entries.AddRange(Parse(text, partial, windowStartLocal, AliasInputChars));
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                info["error_class"] = SlowReadLog.ErrorClassOf(ex);
            }

            fileInfo.Add(info);
        }

        var counts = new JsonArray();
        foreach (var group in entries.GroupBy(e => (e.Level, e.Category)).OrderByDescending(g => g.Count()).ThenBy(g => g.Key.Level, StringComparer.Ordinal).ThenBy(g => g.Key.Category, StringComparer.Ordinal))
        {
            counts.Add(new JsonObject { ["level"] = group.Key.Level, ["category"] = group.Key.Category, ["count"] = group.Count() });
        }

        if (!includeEntries)
        {
            /* Counts only: no message text leaves the service log unless the caller opted in. */
            return new JsonObject
            {
                ["status"] = "ok",
                ["searched"] = searched,
                ["text_withheld"] = true,
                ["files"] = fileInfo,
                ["entries_in_window"] = entries.Count,
                ["counts"] = counts,
            };
        }

        var newest = new JsonArray();
        foreach (var e in entries.OrderByDescending(e => e.Time).Take(MaxEntries))
        {
            newest.Add(new JsonObject
            {
                ["time_local"] = e.Time.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
                ["level"] = e.Level,
                ["category"] = e.Category,
                ["message"] = e.Message,
            });
        }

        return new JsonObject
        {
            ["status"] = "ok",
            ["searched"] = searched,
            ["files"] = fileInfo,
            ["entries_in_window"] = entries.Count,
            ["counts"] = counts,
            ["entries"] = newest,
        };
    }

    private static JsonObject NotFound(string searched) => new()
    {
        ["status"] = "not_found",
        ["searched"] = searched,
        ["reason"] = "The service log directory does not exist. File logging may be off (the service disables it when it cannot create the directory), or the log is somewhere else.",
        ["hint"] = "Pass --log-dir <dir> to point at the log directory. Under systemd the log is in the journal (journalctl -u <unit>), which this verb does not read.",
    };

    private static (string Text, long Bytes, bool Partial) ReadTail(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        var length = stream.Length;
        var partial = length > TailBytes;
        var start = partial ? length - TailBytes : 0;
        stream.Seek(start, SeekOrigin.Begin);
        var buffer = new byte[(int)(length - start)];
        var read = 0;
        while (read < buffer.Length)
        {
            var n = stream.Read(buffer, read, buffer.Length - read);
            if (n == 0)
            {
                break;
            }

            read += n;
        }

        return (Encoding.UTF8.GetString(buffer, 0, read), length, partial);
    }

    /// <summary>Cuts text to <paramref name="max"/> characters, backing up to the last whitespace so a word is not left half-cut.</summary>
    internal static string CutAtWhitespace(string text, int max)
    {
        if (text.Length <= max)
        {
            return text;
        }

        var cut = text.LastIndexOfAny(new[] { ' ', '\n', '\t' }, max - 1);
        return cut > max / 2 ? text[..cut] : text[..max];
    }

    /// <summary>Parses entries from log text. A <paramref name="partial"/> read drops its first line (cut mid-line by the seek). Pure.</summary>
    internal static List<(DateTime Time, string Level, string Category, string Message)> Parse(
        string text, bool partial, DateTime windowStartLocal, int maxEntryChars = MaxEntryChars)
    {
        var result = new List<(DateTime, string, string, string)>();
        var lines = text.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        var first = partial ? 1 : 0;
        DateTime? time = null;
        string level = string.Empty, category = string.Empty;
        StringBuilder? message = null;

        void Flush()
        {
            if (message is not null && time is { } t && level is "WARN" or "ERROR" or "CRIT" && t >= windowStartLocal)
            {
                var m = message.ToString();
                result.Add((t, level, category, CutAtWhitespace(m, maxEntryChars)));
            }

            message = null;
        }

        for (var i = first; i < lines.Length; i++)
        {
            var line = lines[i];
            Match match;
            try
            {
                match = s_entryStart.Match(line);
            }
            catch (RegexMatchTimeoutException)
            {
                continue;
            }

            if (match.Success)
            {
                Flush();
                time = DateTime.ParseExact(match.Groups["ts"].Value, "yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
                level = match.Groups["level"].Value.Trim();
                category = match.Groups["cat"].Value;
                message = new StringBuilder(match.Groups["msg"].Value);
            }
            else if (message is not null && line.StartsWith("    ", StringComparison.Ordinal))
            {
                if (message.Length < maxEntryChars)
                {
                    message.Append('\n').Append(line, 4, line.Length - 4);
                }
            }
        }

        Flush();
        return result;
    }
}
