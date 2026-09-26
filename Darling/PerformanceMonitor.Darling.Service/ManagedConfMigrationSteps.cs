/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The pure file-I/O building blocks for the #4215 migration's wiring (#4336 lane 5a; design rule 3, ruling
/// comment-5827624802 §3): backing up the original <c>postgresql.conf</c>, writing the two new files in the
/// order the crash-safety argument depends on, and stamping the data directory once the write is verified.
/// Startup integration, the <c>pg_file_settings</c> snapshot, and verification itself are lane 5b.
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
}
