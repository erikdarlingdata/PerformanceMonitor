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
using System.Security.Cryptography;
using System.Text;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The pure file-I/O building blocks for the #4215 migration's wiring (#4336): backing up the original
/// <c>postgresql.conf</c>, writing the two new files in the order the crash-safety argument depends on, and
/// stamping the data directory once the write is verified. Startup integration, the <c>pg_file_settings</c>
/// snapshot, and verification itself are covered separately.
/// </summary>
internal static class ManagedConfMigrationSteps
{
    /// <summary>The rule version recorded in the verified stamp. Bump this only when the migration's write
    /// rules change in a way that makes an old stamp's promise no longer trustworthy.</summary>
    internal const int RuleVersion = 1;

    /// <summary>The stamp file's name, in the data directory alongside <c>postgresql.conf</c>.</summary>
    internal const string StampFileName = "darling-managed.conf.verified";

    private const string BackupPrefix = "postgresql.conf.pre-4215.";
    private const string BackupSuffix = ".bak";
    private const string BackupTimestampFormat = "yyyyMMddTHHmmssZ";

    /// <summary>
    /// Backs up <paramref name="postgresqlConfPath"/> to <c>postgresql.conf.pre-4215.&lt;utcNow&gt;.bak</c> in
    /// <paramref name="dataDir"/> (rule 1). If a backup matching that pattern already exists — from an earlier
    /// attempt, however far it got — this makes no second one and returns the existing path unchanged; the
    /// backup is a one-time snapshot of what the store looked like before #4215 ever touched it, and every
    /// re-run after the first must converge on that same original, not a mid-migration state.
    /// </summary>
    internal static string BackupOriginal(string dataDir, string postgresqlConfPath, DateTime utcNow)
    {
        var existing = Directory.GetFiles(dataDir, BackupPrefix + "*" + BackupSuffix);
        if (existing.Length > 0)
        {
            Array.Sort(existing, StringComparer.Ordinal);
            return existing[0];
        }

        var backupPath = Path.Combine(
            dataDir,
            BackupPrefix + utcNow.ToString(BackupTimestampFormat, System.Globalization.CultureInfo.InvariantCulture) + BackupSuffix);
        File.Copy(postgresqlConfPath, backupPath, overwrite: false);
        return backupPath;
    }

    /// <summary>
    /// Test seam: when non-null, <see cref="WriteTwoSteps"/> invokes this after step 1 (the managed file)
    /// succeeds and before step 2 (<c>postgresql.conf</c>) is attempted, so a test can force the crash the
    /// two-step design is meant to survive. Production callers leave this null.
    /// </summary>
    internal static Action? FailBetweenSteps;

    /// <summary>
    /// Writes the managed file, then <c>postgresql.conf</c>, each atomically (rule 4). The managed file goes
    /// first because it is harmless while nothing includes it yet; a crash after step 1 but before step 2
    /// leaves the old <c>postgresql.conf</c> in force and a re-run of this method converges on the same bytes
    /// in both files. If step 1 fails, this throws before touching <c>postgresql.conf</c> at all.
    /// </summary>
    internal static void WriteTwoSteps(string dataDir, string managedConfText, string newPostgresqlConfText)
    {
        var managedConfPath = Path.Combine(dataDir, ManagedConfFile.FileName);
        if (!ManagedConfFile.TryReplaceAtomic(
                managedConfPath,
                managedConfText,
                ManagedConfFile.DefaultMaxReplaceAttempts,
                ManagedConfFile.DefaultReplaceRetryDelay,
                out var managedError))
        {
            throw new IOException(
                FormattableString.Invariant($"Failed to write {managedConfPath}."),
                managedError);
        }

        FailBetweenSteps?.Invoke();

        var postgresqlConfPath = Path.Combine(dataDir, "postgresql.conf");
        if (!ManagedConfFile.TryReplaceAtomic(
                postgresqlConfPath,
                newPostgresqlConfText,
                ManagedConfFile.DefaultMaxReplaceAttempts,
                ManagedConfFile.DefaultReplaceRetryDelay,
                out var confError))
        {
            throw new IOException(
                FormattableString.Invariant($"Failed to write {postgresqlConfPath}."),
                confError);
        }
    }

    /// <summary>
    /// Writes the verified stamp atomically. Its content is the SHA-256 of <paramref name="managedConfText"/>
    /// (the bytes the caller just wrote, or is about to write, to <c>darling-managed.conf</c>) and the rule
    /// version this build stamps with, one per line, so <see cref="IsVerified"/> can tell a stale stamp from
    /// the current one without re-deriving anything.
    /// </summary>
    internal static void WriteVerifiedStamp(string dataDir, string managedConfText)
    {
        var hashHex = Sha256Hex(managedConfText);
        var stampPath = Path.Combine(dataDir, StampFileName);
        var content = FormattableString.Invariant($"sha256:{hashHex}\nrule:{RuleVersion}\n");
        if (!ManagedConfFile.TryReplaceAtomic(
                stampPath,
                content,
                ManagedConfFile.DefaultMaxReplaceAttempts,
                ManagedConfFile.DefaultReplaceRetryDelay,
                out var error))
        {
            throw new IOException(FormattableString.Invariant($"Failed to write {stampPath}."), error);
        }
    }

    /// <summary>
    /// True only if the stamp exists, parses into a sha256 and a rule line, the sha256 equals the SHA-256 of
    /// the CURRENT <c>darling-managed.conf</c> bytes on disk, and the rule equals <see cref="RuleVersion"/>.
    /// Every other case — no stamp, an unparsable stamp, a hash mismatch (the managed file was hand-edited or
    /// the stamp is stale), a rule mismatch, or no managed file to hash at all — is false. A stamp that cannot
    /// be proven current is treated exactly like a missing one.
    /// </summary>
    internal static bool IsVerified(string dataDir)
    {
        var stampPath = Path.Combine(dataDir, StampFileName);
        var managedConfPath = Path.Combine(dataDir, ManagedConfFile.FileName);

        if (!File.Exists(stampPath) || !File.Exists(managedConfPath))
        {
            return false;
        }

        string stampText;
        try
        {
            stampText = File.ReadAllText(stampPath);
        }
        catch (IOException)
        {
            return false;
        }

        var lines = stampText.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        string? stampedHash = null;
        int? stampedRule = null;
        foreach (var line in lines)
        {
            if (line.StartsWith("sha256:", StringComparison.Ordinal))
            {
                stampedHash = line["sha256:".Length..];
            }
            else if (line.StartsWith("rule:", StringComparison.Ordinal))
            {
                if (int.TryParse(
                        line["rule:".Length..],
                        System.Globalization.NumberStyles.Integer,
                        System.Globalization.CultureInfo.InvariantCulture,
                        out var parsedRule))
                {
                    stampedRule = parsedRule;
                }
            }
        }

        if (stampedHash is null || stampedRule is null || stampedRule.Value != RuleVersion)
        {
            return false;
        }

        var currentText = File.ReadAllText(managedConfPath);
        var currentHash = Sha256Hex(currentText);
        return string.Equals(stampedHash, currentHash, StringComparison.OrdinalIgnoreCase);
    }

    private static string Sha256Hex(string text)
    {
        var bytes = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false).GetBytes(text);
        var hash = SHA256.HashData(bytes);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    /// <summary>The pending file's name, in the data directory alongside <c>postgresql.conf</c> (#4336):
    /// the only record of the BEFORE <c>pg_file_settings</c> snapshot and the prior managed-file text
    /// (when one existed) once <see cref="WriteTwoSteps"/> has overwritten both files — a crash after that
    /// point has nothing else to compare the after-snapshot against, or to restore to.</summary>
    internal const string PendingFileName = "darling-managed.conf.pending";

    private const char FieldNullTag = 'N';
    private const char FieldValueTag = 'V';

    /// <summary>
    /// Writes <see cref="PendingFileName"/> atomically (#4336): the BEFORE snapshot (tab-separated,
    /// one row per line, every field escaped so a tab or newline INSIDE a setting's own value round-trips
    /// exactly — <see cref="EncodeField(string?)"/>) plus, on its own leading line, the prior managed-file text (null
    /// when none existed — a fresh initdb'd store migrating for the first time). This is the risk this
    /// method exists to cover: "the pending file is the only record of 'before' after a crash." If it cannot
    /// be written, the caller must abort before <see cref="BackupOriginal"/> — nothing has changed yet.
    /// </summary>
    internal static void WritePending(string dataDir, IReadOnlyList<FileSettingRow> before, string? priorManagedText)
    {
        var sb = new StringBuilder();
        sb.Append(EncodeField(priorManagedText)).Append('\n');
        foreach (var row in before)
        {
            sb.Append(EncodeField(row.SourceFile)).Append('\t')
              .Append(EncodeField(row.SourceLine?.ToString(System.Globalization.CultureInfo.InvariantCulture))).Append('\t')
              .Append(EncodeField(row.Name)).Append('\t')
              .Append(EncodeField(row.Setting)).Append('\t')
              .Append(row.Applied ? '1' : '0').Append('\t')
              .Append(EncodeField(row.Error)).Append('\n');
        }

        var pendingPath = Path.Combine(dataDir, PendingFileName);
        if (!ManagedConfFile.TryReplaceAtomic(
                pendingPath,
                sb.ToString(),
                ManagedConfFile.DefaultMaxReplaceAttempts,
                ManagedConfFile.DefaultReplaceRetryDelay,
                out var error))
        {
            throw new IOException(FormattableString.Invariant($"Failed to write {pendingPath}."), error);
        }
    }

    /// <summary>
    /// Reads back what <see cref="WritePending"/> wrote. False (with both out parameters empty/null) when
    /// there is no pending file at all — the normal case, or one already cleaned up by
    /// <see cref="DeletePending"/> — or when the pending file exists but its content cannot be parsed as
    /// the format <see cref="WritePending"/> writes (#4336): a corrupt or truncated field never throws here,
    /// it is reported the same as a missing file, so the caller's PendingVerify handling can name the
    /// reason itself.
    /// </summary>
    internal static bool TryReadPending(
        string dataDir,
        out IReadOnlyList<FileSettingRow> before,
        out string? priorManagedText)
    {
        before = Array.Empty<FileSettingRow>();
        priorManagedText = null;

        var pendingPath = Path.Combine(dataDir, PendingFileName);
        if (!File.Exists(pendingPath))
        {
            return false;
        }

        try
        {
            var text = File.ReadAllText(pendingPath);
            var normalized = text.Replace("\r\n", "\n", StringComparison.Ordinal);
            var lines = normalized.Split('\n');

            var parsedPriorManagedText = DecodeField(lines[0]);

            var rows = new List<FileSettingRow>();
            for (var i = 1; i < lines.Length; i++)
            {
                if (lines[i].Length == 0)
                {
                    continue;
                }

                var fields = lines[i].Split('\t');
                if (fields.Length != 6)
                {
                    return false;
                }

                var sourceLineText = DecodeField(fields[1]);
                int? sourceLine = null;
                if (sourceLineText is not null)
                {
                    if (!int.TryParse(
                            sourceLineText,
                            System.Globalization.NumberStyles.Integer,
                            System.Globalization.CultureInfo.InvariantCulture,
                            out var parsedSourceLine))
                    {
                        return false;
                    }

                    sourceLine = parsedSourceLine;
                }

                rows.Add(new FileSettingRow(
                    SourceFile: DecodeField(fields[0]),
                    SourceLine: sourceLine,
                    Name: DecodeField(fields[2]),
                    Setting: DecodeField(fields[3]),
                    Applied: fields[4] == "1",
                    Error: DecodeField(fields[5])));
            }

            priorManagedText = parsedPriorManagedText;
            before = rows;
            return true;
        }
        catch (IOException)
        {
            before = Array.Empty<FileSettingRow>();
            priorManagedText = null;
            return false;
        }
    }

    /// <summary>Deletes <see cref="PendingFileName"/>, if present. Best-effort is not appropriate here — a
    /// pending file left behind after a completed run (verified or restored) would make the NEXT start read a
    /// stale "before" that no longer describes anything, so a delete failure is allowed to throw.</summary>
    internal static void DeletePending(string dataDir)
    {
        var pendingPath = Path.Combine(dataDir, PendingFileName);
        if (File.Exists(pendingPath))
        {
            File.Delete(pendingPath);
        }
    }

    /// <summary>
    /// Restores the pre-migration state (#4336, the mismatch and the after-snapshot-throws paths
    /// alike): <paramref name="backupPath"/>'s bytes go back to <c>postgresql.conf</c> atomically, and
    /// <paramref name="priorManagedText"/> — null when no managed file existed before this run — either
    /// replaces <c>darling-managed.conf</c> (a prior migration's file, restored verbatim) or is left absent
    /// when null (nothing ever included it, so leaving it missing is the byte-identical restore; a stray file
    /// nothing references is not itself a mismatch, but not writing one when none existed keeps the directory
    /// exactly as it was). Never touches the backup or the pending file — the caller deletes those once this
    /// returns.
    /// </summary>
    internal static void RestoreOriginal(string dataDir, string backupPath, string? priorManagedText)
    {
        var postgresqlConfPath = Path.Combine(dataDir, "postgresql.conf");
        var backupText = File.ReadAllText(backupPath);
        if (!ManagedConfFile.TryReplaceAtomic(
                postgresqlConfPath,
                backupText,
                ManagedConfFile.DefaultMaxReplaceAttempts,
                ManagedConfFile.DefaultReplaceRetryDelay,
                out var confError))
        {
            throw new IOException(FormattableString.Invariant($"Failed to restore {postgresqlConfPath}."), confError);
        }

        var managedConfPath = Path.Combine(dataDir, ManagedConfFile.FileName);
        if (priorManagedText is null)
        {
            if (File.Exists(managedConfPath))
            {
                File.Delete(managedConfPath);
            }

            return;
        }

        if (!ManagedConfFile.TryReplaceAtomic(
                managedConfPath,
                priorManagedText,
                ManagedConfFile.DefaultMaxReplaceAttempts,
                ManagedConfFile.DefaultReplaceRetryDelay,
                out var managedError))
        {
            throw new IOException(FormattableString.Invariant($"Failed to restore {managedConfPath}."), managedError);
        }
    }

    /// <summary>Encodes one pending-file field (#4336): a leading <see cref="FieldNullTag"/> for a
    /// null value, or <see cref="FieldValueTag"/> followed by <paramref name="value"/> with backslash escaped
    /// first, then tab and newline — so a setting containing either round-trips through
    /// <see cref="DecodeField"/> exactly (the same tab/newline round-trip risk <c>EscapeConfValue</c> covers, here for the
    /// pending file's own tab-separated format).</summary>
    private static string EncodeField(string? value)
    {
        if (value is null)
        {
            return FieldNullTag.ToString();
        }

        var escaped = value
            .Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("\t", "\\t", StringComparison.Ordinal)
            .Replace("\n", "\\n", StringComparison.Ordinal)
            .Replace("\r", "\\r", StringComparison.Ordinal);
        return FieldValueTag + escaped;
    }

    private static string? DecodeField(string field)
    {
        if (field.Length == 0 || field[0] == FieldNullTag)
        {
            return null;
        }

        var escaped = field[1..];
        var sb = new StringBuilder(escaped.Length);
        for (var i = 0; i < escaped.Length; i++)
        {
            if (escaped[i] == '\\' && i + 1 < escaped.Length)
            {
                i++;
                sb.Append(escaped[i] switch
                {
                    '\\' => '\\',
                    't' => '\t',
                    'n' => '\n',
                    'r' => '\r',
                    _ => escaped[i],
                });
            }
            else
            {
                sb.Append(escaped[i]);
            }
        }

        return sb.ToString();
    }
}
