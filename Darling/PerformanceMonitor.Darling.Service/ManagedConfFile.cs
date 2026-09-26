/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// One derived value <see cref="ManagedConfFile.Render"/> disagreed with what the file in force already holds
/// (#4215): the setting, the value the file on disk carries (null when the fresh render drops it), and the
/// value a fresh render would write (null when the file on disk carries a setting no current formula emits).
/// </summary>
internal readonly record struct ManagedConfKeyDiff(string Key, string? FileValue, string? RenderedValue);

/// <summary>
/// What <see cref="DarlingManagedPostgres.WriteManagedConfFile"/> did with <c>darling-managed.conf</c> on one
/// start (#4215):
/// whether it wrote a new file, whether the file it found was a hand edit (never overwritten), and — for a hand
/// edit — the keys where the edit and a fresh render disagree, for the caller to report as an override.
/// </summary>
internal readonly record struct ManagedConfWriteResult(
    bool Written,
    bool HandEdited,
    bool WriteFailed,
    string RenderedText,
    IReadOnlyList<ManagedConfKeyDiff> ChangedKeys,
    string? PreviousText = null,
    ManagedConfFile.RenderInputs? Inputs = null);

/// <summary>
/// The one service-owned settings file for a managed store (#4215): <c>darling-managed.conf</c>, rendered fresh
/// on every start from the SAME formulas the v1-v15 <c>postgresql.conf</c> blocks use today, and included from
/// <c>postgresql.conf</c> rather than appended into it. Nothing here decides WHAT a setting should be — every
/// value comes from calling <see cref="DarlingManagedPostgres"/>'s existing <c>Build*ConfAppend</c> builders and
/// reading back what they wrote, so a formula lives in exactly one place. This class owns three things a
/// per-block append never had to: a single render an operator can diff against a fresh one, a hash that tells
/// the service whether that file is still what it wrote, and an atomic replace so a running server never opens
/// a half-written file.
///
/// <para><b>Rendering, not deriving.</b> <see cref="RenderBody"/> calls the v1 through v15 builders in the same
/// order <c>EnsureConfAppended</c> appends them, concatenates their text, and reduces it with
/// <c>DarlingManagedPostgres.ParseConfText</c> the same way PostgreSQL itself would: last occurrence of a key
/// wins. That is what "one file replaces fifteen appended blocks" means in code — the REDUCTION is new, no
/// FORMULA is.</para>
/// </summary>
internal static class ManagedConfFile
{
    /// <summary>The file's name, in the data directory beside <c>postgresql.conf</c>.</summary>
    internal const string FileName = "darling-managed.conf";

    /// <summary>The last file this service wrote that PostgreSQL accepted: what a rejected
    /// render falls back to rather than failing the start outright.</summary>
    internal const string LastGoodFileName = "darling-managed.conf.last-good";

    /// <summary>The temp file <see cref="TryReplaceAtomic"/> writes and flushes before the rename that makes it
    /// <see cref="FileName"/> — so a reader never observes a partially written file (this class never writes
    /// in place).</summary>
    internal const string TempFileName = FileName + ".tmp";

    /// <summary>The plain <c>include</c> line <c>postgresql.conf</c> carries once this design is in force
    /// (plain form, not <c>include_if_exists</c>). Single-quoted so a Windows path with no
    /// special characters never needs escaping in practice, and so the form matches every other quoted value
    /// this class and <c>DarlingManagedPostgres</c> write.</summary>
    internal const string IncludeLine = "include 'darling-managed.conf'";

    /// <summary>The comment prefix that opens the line carrying the body's hash (<see cref="ComputeBodyHash"/>).
    /// Everything from the line AFTER this one to end of file is "the body" for hashing and for
    /// <see cref="DiffBodyKeys"/> alike.</summary>
    internal const string BodyHashPrefix = "# body-sha256=";

    /// <summary>Bumped whenever a render input changes shape (a header field added or removed, a builder
    /// dropped or gains a parameter) — not on every value change, which the hash already covers. Recorded in
    /// the header so a support bundle names the shape of file it is reading.</summary>
    internal const int CurrentFormulaVersion = 1;

    /// <summary>The replace's retry budget on a sharing violation (design step 1): 40 attempts, 50 ms apart —
    /// two seconds — before giving up and keeping the file that was already there.</summary>
    internal const int DefaultMaxReplaceAttempts = 40;

    /// <summary>The delay between replace attempts. See <see cref="DefaultMaxReplaceAttempts"/>.</summary>
    internal static readonly TimeSpan DefaultReplaceRetryDelay = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Every value <see cref="Render"/> needs, gathered by the caller (<c>DarlingManagedPostgres</c>, which
    /// alone holds the Windows-only readers) so this type and <see cref="Render"/> stay pure and portable: no
    /// P/Invoke, no file I/O, callable from a plain unit test with literal numbers.
    /// </summary>
    internal readonly record struct RenderInputs(
        int FormulaVersion,
        string Platform,
        long RamBytes,
        bool RamAuthoritative,
        int ProcessorCount,
        int HypertableCount,
        int PostgresMajor,
        long DataVolumeFreeBytes,
        long DataVolumeTotalBytes,
        bool DataVolumeAuthoritative,
        int Port,
        string? EffectivePreloadList);

    /// <summary>One parse of an existing <see cref="FileName"/>: whether it carries a recognizable
    /// <see cref="BodyHashPrefix"/> line at all, the hash it declares, and the body text that line's hash is
    /// supposed to cover.</summary>
    internal readonly record struct ParsedManagedConf(bool IsWellFormed, string? DeclaredHash, string Body);

    /// <summary>
    /// The full file text: the header (formula version, platform, RAM, CPUs, hypertable count, PostgreSQL
    /// major, data-volume free/total — no timestamp, so two renders of the same inputs are byte-identical),
    /// the <see cref="BodyHashPrefix"/> line, then the body. Pure: same <paramref name="inputs"/>, same bytes,
    /// every time.
    /// </summary>
    internal static string Render(RenderInputs inputs)
    {
        var body = RenderBody(inputs);
        var header = RenderHeader(inputs, ComputeBodyHash(body));
        return header + body;
    }

    /// <summary>
    /// The header alone (#4215 design section 2): a fixed explanation, then one <c>#</c> comment per input,
    /// then the <see cref="BodyHashPrefix"/> line. No timestamp — the ONLY thing that can change two renders of
    /// the same inputs is missing here on purpose, because a timestamp would make "byte-identical when nothing
    /// changed" false on every start.
    ///
    /// <para>The two data-volume figures are rounded to the nearest GiB (<see cref="RoundToGiB"/>) for display
    /// only — <see cref="RenderBody"/> still derives from the EXACT bytes in <paramref name="inputs"/>. Free
    /// space on the volume holding the data directory moves by a few kilobytes on its own (this very write
    /// included), and an exact byte count in the header would make two renders that changed nothing PostgreSQL
    /// cares about differ anyway, defeating "byte-identical when nothing changed" and the hand-edit hash alike.
    /// </para>
    /// </summary>
    internal static string RenderHeader(RenderInputs inputs, string bodyHash)
    {
        var header = new StringBuilder();
        header.Append("# Managed by PerformanceMonitor Darling -- generated fresh on every start (#4215).\n");
        header.Append("# A hand edit below this point is detected and left in force, never overwritten silently.\n");
        header.Append("# To pin a value, add a line after 'include ''darling-managed.conf''' in postgresql.conf,\n");
        header.Append("# or run ALTER SYSTEM once the store is running.\n");
        header.Append("# formula-version=").Append(inputs.FormulaVersion).Append('\n');
        header.Append("# platform=").Append(inputs.Platform).Append('\n');
        header.Append("# ram-bytes=").Append(inputs.RamBytes).Append('\n');
        header.Append("# cpus=").Append(inputs.ProcessorCount).Append('\n');
        header.Append("# hypertables=").Append(inputs.HypertableCount).Append('\n');
        header.Append("# postgres-major=").Append(inputs.PostgresMajor).Append('\n');
        header.Append("# data-volume-free-gib=").Append(RoundToGiB(inputs.DataVolumeFreeBytes)).Append('\n');
        header.Append("# data-volume-total-gib=").Append(RoundToGiB(inputs.DataVolumeTotalBytes)).Append('\n');
        header.Append(BodyHashPrefix).Append(bodyHash).Append('\n');
        return header.ToString();
    }

    /// <summary>Bytes rounded to the nearest GiB, for <see cref="RenderHeader"/>'s display-only data-volume
    /// fields. See the remarks on <see cref="RenderHeader"/> for why this is rounded and the body's own
    /// derivation is not.</summary>
    private static long RoundToGiB(long bytes)
    {
        const long gib = 1024L * 1024 * 1024;
        return (bytes + gib / 2) / gib;
    }

    /// <summary>
    /// The body alone: one <c>key = 'value'</c> line per setting the v1 through v15 blocks set today, in the
    /// order each key is FIRST introduced (v1 before v2 before v3 ...), holding the value each key has LAST —
    /// exactly what PostgreSQL itself would read from those fifteen blocks appended in order to one file. Every
    /// value is single-quoted and escaped (<c>DarlingManagedPostgres.EscapeConfValue</c>): PostgreSQL accepts a
    /// quoted string for a numeric or boolean GUC too, and quoting uniformly means no value's own content (a
    /// path with a backslash, say) needs a second escaping rule here.
    /// </summary>
    internal static string RenderBody(RenderInputs inputs)
    {
        var blocks = new StringBuilder();
        blocks.Append(DarlingManagedPostgres.BuildConfAppend(inputs.Port));
        blocks.Append(DarlingManagedPostgres.BuildWorkerSizingConfAppend());
        blocks.Append(DarlingManagedPostgres.BuildMemorySizingConfAppend(inputs.RamBytes));
        blocks.Append(DarlingManagedPostgres.BuildWriteThroughputConfAppend());
        blocks.Append(DarlingManagedPostgres.BuildColocatedSizingConfAppend(inputs.RamBytes));
        blocks.Append(DarlingManagedPostgres.BuildLogRotationConfAppend());
        blocks.Append(DarlingManagedPostgres.BuildCompressionMemoryConfAppend(inputs.RamBytes));

        /* v8 (#2845) only ever re-derives from an AUTHORITATIVE reading — see
           DarlingManagedPostgres.TryGetAuthoritativePhysicalMemoryBytes's remarks. Without one, v3/v5/v7's
           values above stay exactly as they are today when v8 skips itself. */
        if (inputs.RamAuthoritative)
        {
            var quantizedRam = DarlingManagedPostgres.QuantizeRam(inputs.RamBytes);
            blocks.Append(DarlingManagedPostgres.BuildHardwareSizingConfAppend(quantizedRam, inputs.HypertableCount));
        }

        blocks.Append(DarlingManagedPostgres.BuildTimeZoneConfAppend());
        blocks.Append(DarlingManagedPostgres.BuildMessageLocaleConfAppend());
        blocks.Append(DarlingManagedPostgres.BuildJobExecutionLoggingConfAppend());

        /* v12 (#3802) only re-derives with an authoritative free-space reading, same discipline as v8. Without
           one, max_wal_size stays at v4's fixed 4GB — nothing above sets it otherwise. */
        if (inputs.DataVolumeAuthoritative)
        {
            blocks.Append(DarlingManagedPostgres.BuildWalSizingConfAppend(
                inputs.DataVolumeFreeBytes, inputs.DataVolumeTotalBytes, inputs.PostgresMajor));
        }

        /* v13 (#3899): the preload list is the MERGE of what's in force plus timescaledb and
           pg_stat_statements — MergePreloadLibraries never shrinks the list, and BuildStatementStatisticsConfAppend
           calls it, so the merge lives in exactly the one place it always has. */
        blocks.Append(DarlingManagedPostgres.BuildStatementStatisticsConfAppend(inputs.EffectivePreloadList));

        /* v14 (#3909): capped only when the value in force after v1-v13 above would stop this PostgreSQL major
           from starting. NeedsLegacyMaintenanceWorkMemCap is the same pure predicate EnsureConfAppended's own
           heal uses — called here, not re-decided. */
        var (_, valuesSoFar) = ReduceToLastOccurrence(blocks.ToString());
        valuesSoFar.TryGetValue(DarlingManagedPostgres.MaintenanceWorkMemSetting, out var maintenanceWorkMemSoFar);
        var postgresMajorForCap = inputs.PostgresMajor > 0 ? inputs.PostgresMajor : (int?)null;
        if (DarlingManagedPostgres.NeedsLegacyMaintenanceWorkMemCap(postgresMajorForCap, maintenanceWorkMemSoFar))
        {
            blocks.Append(DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend());
        }

        blocks.Append(DarlingManagedPostgres.BuildWalVolumeConfAppend());

        var (order, values) = ReduceToLastOccurrence(blocks.ToString());
        var body = new StringBuilder();
        foreach (var key in order)
        {
            body.Append(key).Append(" = '")
                .Append(DarlingManagedPostgres.EscapeConfValue(values[key]))
                .Append("'\n");
        }

        return body.ToString();
    }

    /// <summary>
    /// Renders <c>darling-managed.conf</c> the way Step A's post-start verification needs (#4336):
    /// the SAME owned keys and order <see cref="RenderBody"/> would produce for
    /// <paramref name="inputs"/>, but each owned key's VALUE comes from <paramref name="values"/> — the raw
    /// text <c>pg_file_settings</c> reports as <c>applied</c> for that key — instead of the freshly derived
    /// one. An owned key <see cref="RenderBody"/> would have written that is missing from
    /// <paramref name="values"/> is dropped from the body entirely (it stays at whatever default is already
    /// in force; Step A never invents a value the snapshot did not report). <paramref name="extraKeys"/>
    /// (#4336) carries the keys the rewritten <c>postgresql.conf</c> lost that this render does not own —
    /// for example v12's <c>min_wal_size</c> when the disk reading is not authoritative this start — each
    /// written from <paramref name="values"/>'s snapshot value, in the order given, after every owned key;
    /// a key already written from <paramref name="values"/> above is never duplicated. The header — and
    /// therefore the body hash — is recomputed from the snapshot-valued body, so a fresh <see cref="Render"/>
    /// call over the RESULT reports <see cref="IsHandEdited"/> false: this is what Step A itself just wrote, verified, not
    /// an edit.
    /// </summary>
    internal static string RenderWithValues(
        RenderInputs inputs,
        IReadOnlyDictionary<string, string> values,
        IReadOnlyList<string>? extraKeys = null)
    {
        var derivedBody = RenderBody(inputs);
        var (order, _) = ReduceToLastOccurrence(derivedBody);

        var body = new StringBuilder();
        var written = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var key in order)
        {
            if (values.TryGetValue(key, out var snapshotValue))
            {
                body.Append(key).Append(" = '")
                    .Append(DarlingManagedPostgres.EscapeConfValue(snapshotValue))
                    .Append("'\n");
                written.Add(key);
            }
        }

        if (extraKeys is not null)
        {
            foreach (var key in extraKeys)
            {
                if (!written.Add(key) || !values.TryGetValue(key, out var snapshotValue))
                {
                    continue;
                }

                body.Append(key).Append(" = '")
                    .Append(DarlingManagedPostgres.EscapeConfValue(snapshotValue))
                    .Append("'\n");
            }
        }

        var header = RenderHeader(inputs, ComputeBodyHash(body.ToString()));
        return header + body.ToString();
    }

    /// <summary>
    /// Reduces builder-appended conf text to one value per key: <paramref name="text"/> parsed with
    /// <c>DarlingManagedPostgres.ParseConfText</c>, keys kept in FIRST-seen order, values overwritten by every
    /// later occurrence — exactly "last occurrence wins", read off in the order a person scanning v1 to v15
    /// would first meet each setting.
    /// </summary>
    private static (List<string> Order, Dictionary<string, string> Values) ReduceToLastOccurrence(string text)
    {
        var order = new List<string>();
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (_, name, value) in DarlingManagedPostgres.ParseConfText(text))
        {
            if (values.TryAdd(name, value))
            {
                order.Add(name);
            }
            else
            {
                values[name] = value;
            }
        }

        return (order, values);
    }

    /// <summary>
    /// The body's hash, as the text after <see cref="BodyHashPrefix"/>: lower-case hex SHA-256 over the body's
    /// UTF-8 bytes. Comparing this against what an existing file DECLARES is how <see cref="IsHandEdited"/>
    /// tells a file this class wrote from one an operator has since edited.
    /// </summary>
    internal static string ComputeBodyHash(string body)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(body))).ToLowerInvariant();

    /// <summary>
    /// Splits an existing <see cref="FileName"/>'s text at its <see cref="BodyHashPrefix"/> line: everything
    /// after that line is the body the declared hash is supposed to cover. Not well-formed (and the declared
    /// hash null) when no such line exists — a file from before this design, or one with nothing recognizable
    /// as ours.
    /// </summary>
    internal static ParsedManagedConf ParseExisting(string fileText)
    {
        var normalized = fileText.Replace("\r\n", "\n", StringComparison.Ordinal);
        var lines = normalized.Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            if (lines[i].StartsWith(BodyHashPrefix, StringComparison.Ordinal))
            {
                var declaredHash = lines[i][BodyHashPrefix.Length..].Trim();
                var body = lines.Length > i + 1 ? string.Join('\n', lines[(i + 1)..]) : string.Empty;
                return new ParsedManagedConf(true, declaredHash, body);
            }
        }

        return new ParsedManagedConf(false, null, normalized);
    }

    /// <summary>
    /// Whether an existing <see cref="FileName"/> is no longer what this class rendered (a "hand
    /// edit"): its body's hash no longer matches what its own header declares. A file with no
    /// recognizable <see cref="BodyHashPrefix"/> line at all counts as a hand edit too — anything this class did
    /// not provably just write is never silently overwritten.
    /// </summary>
    internal static bool IsHandEdited(string existingFileText)
    {
        var parsed = ParseExisting(existingFileText);
        return !parsed.IsWellFormed || !string.Equals(parsed.DeclaredHash, ComputeBodyHash(parsed.Body), StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Every key where <paramref name="fileBody"/> (what is on disk) and <paramref name="renderedBody"/> (what a
    /// fresh render would write) disagree — for the hand-edit log line (design step 1) to name each one, rather
    /// than say only that a difference exists.
    /// </summary>
    internal static IReadOnlyList<ManagedConfKeyDiff> DiffBodyKeys(string fileBody, string renderedBody)
    {
        var (fileOrder, fileValues) = ReduceToLastOccurrence(fileBody);
        var (renderedOrder, renderedValues) = ReduceToLastOccurrence(renderedBody);

        var keys = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var key in fileOrder.Concat(renderedOrder))
        {
            if (seen.Add(key))
            {
                keys.Add(key);
            }
        }

        var diffs = new List<ManagedConfKeyDiff>();
        foreach (var key in keys)
        {
            fileValues.TryGetValue(key, out var fileValue);
            renderedValues.TryGetValue(key, out var renderedValue);
            if (!string.Equals(fileValue, renderedValue, StringComparison.Ordinal))
            {
                diffs.Add(new ManagedConfKeyDiff(key, fileValue, renderedValue));
            }
        }

        return diffs;
    }

    /// <summary>
    /// Whether <paramref name="postgresqlConfText"/> already carries a working <c>include</c> of
    /// <see cref="FileName"/> — matched the way PostgreSQL itself would parse the line
    /// (<c>DarlingManagedPostgres.ParseConfText</c>), not by comparing raw text against
    /// <see cref="IncludeLine"/>, so a quoting style PostgreSQL also accepts is not reported missing.
    /// </summary>
    internal static bool HasManagedInclude(string postgresqlConfText)
    {
        foreach (var (_, name, value) in DarlingManagedPostgres.ParseConfText(postgresqlConfText))
        {
            if (name.Equals("include", StringComparison.OrdinalIgnoreCase)
                && string.Equals(Path.GetFileName(value), FileName, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Writes <paramref name="content"/> to <paramref name="destPath"/> without ever leaving a reader to observe
    /// a partial file (design step 1): the SAME content goes to <paramref name="destPath"/> + <c>.tmp</c> first,
    /// flushed to disk, then <see cref="File.Move(string, string, bool)"/> replaces the destination. A sharing
    /// violation on the move (another handle has <paramref name="destPath"/> open) retries
    /// <paramref name="maxAttempts"/> times, <paramref name="retryDelay"/> apart, before giving up — the caller
    /// keeps whatever was already at <paramref name="destPath"/> either way.
    /// </summary>
    internal static bool TryReplaceAtomic(
        string destPath,
        string content,
        int maxAttempts,
        TimeSpan retryDelay,
        out Exception? lastError)
    {
        lastError = null;
        var tempPath = Path.Combine(Path.GetDirectoryName(destPath) ?? ".", TempFileName);
        try
        {
            using (var stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
            using (var writer = new StreamWriter(stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false)))
            {
                writer.Write(content);
                writer.Flush();
                stream.Flush(flushToDisk: true);
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            lastError = ex;
            return false;
        }

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                File.Move(tempPath, destPath, overwrite: true);
                lastError = null;
                return true;
            }
            catch (IOException ex)
            {
                lastError = ex;
                if (attempt < maxAttempts)
                {
                    Thread.Sleep(retryDelay);
                }
            }
            catch (UnauthorizedAccessException ex)
            {
                lastError = ex;
                if (attempt < maxAttempts)
                {
                    Thread.Sleep(retryDelay);
                }
            }
        }

        /* The temp file could not be renamed onto destPath in any attempt — clean it up rather than leaving a
           stray .tmp beside the file that stays in force. Best-effort: a failure here is not itself fatal. */
        try
        {
            File.Delete(tempPath);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* Nothing more to do — the caller already has lastError from the move failures above. */
        }

        return false;
    }
}
