/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Why the previous install's <c>monitor.duckdb</c> could not be opened for Import Data's flush step.
/// </summary>
public enum DataImportOpenFailure
{
    /// <summary>The database opened.</summary>
    None,

    /// <summary>Another process holds the file: the previous Lite is still running. Closing it and retrying fixes this.</summary>
    Locked,

    /// <summary>The file was written by a DuckDB storage version this build does not read. Retrying cannot fix this.</summary>
    StorageVersion,

    /// <summary>The file is not a readable DuckDB database: bad header bytes or a failed block checksum. Retrying cannot fix this.</summary>
    Corrupt,

    /// <summary>A failure none of the known shapes match. The import treats it as a lock and offers the retry prompt, the one remedy a user can try.</summary>
    Unknown,
}

/// <summary>
/// Sorts a failure to open the old store into the one cause a retry can fix (a lock) and the two known causes no retry
/// can fix, so Import Data asks the user to close the previous application only when that is the remedy.
/// <para>A lock is read from the whole exception chain, in three forms. DuckDB's own text: "Could not set lock on file"
/// and "Conflicting lock" (Unix), "being used by another process" (English Windows) and "File is already open in", the
/// suffix DuckDB appends on Windows when it can name the holder, which survives an OS message in another language. A
/// Win32 sharing-violation (32) or lock-violation (33) code carried as an exception HResult. The same code on an inner
/// <see cref="Win32Exception"/>.</para>
/// <para>A storage mismatch reports "Trying to read a database file with version number", "Failed to deserialize" or
/// the bare words "storage version". Corruption reports "not a valid DuckDB database file" or "Corrupt database file".
/// The phrases are matched over the exception's full text, inner exceptions included. Anything else is
/// <see cref="DataImportOpenFailure.Unknown"/>. Pure, so a test drives it with the exception shapes alone.</para>
/// </summary>
public static class DataImportOpenFailureClassifier
{
    private const int SharingViolation = 32;
    private const int LockViolation = 33;

    public static DataImportOpenFailure Classify(Exception ex)
    {
        for (var e = ex; e != null; e = e.InnerException)
        {
            if (e is Win32Exception win32 && IsSharingOrLockViolation(win32.NativeErrorCode))
            {
                return DataImportOpenFailure.Locked;
            }

            if (IsWin32HResult(e.HResult) && IsSharingOrLockViolation(e.HResult & 0xFFFF))
            {
                return DataImportOpenFailure.Locked;
            }
        }

        var text = ex.ToString().ToLowerInvariant();
        if (text.Contains("conflicting lock") || text.Contains("could not set lock")
            || text.Contains("being used by another process") || text.Contains("already open in"))
        {
            return DataImportOpenFailure.Locked;
        }

        if (text.Contains("failed to deserialize") || text.Contains("trying to read a database file with version") || text.Contains("storage version"))
        {
            return DataImportOpenFailure.StorageVersion;
        }

        if (text.Contains("not a valid duckdb database file") || text.Contains("corrupt database file"))
        {
            return DataImportOpenFailure.Corrupt;
        }

        return DataImportOpenFailure.Unknown;
    }

    /// <summary>An HResult of the form 0x8007xxxx carries a Win32 error code in its low 16 bits.</summary>
    private static bool IsWin32HResult(int hresult) => (hresult & unchecked((int)0xFFFF0000)) == unchecked((int)0x80070000);

    private static bool IsSharingOrLockViolation(int code) => code is SharingViolation or LockViolation;

    /// <summary>
    /// The message the import reports for a failure no retry can fix, built after the archive copy so it states what
    /// the import did: the parquet files copied from the previous install's archive folder, or that there were none,
    /// and that the data the old install had not yet archived was not imported. It names the cause and carries
    /// DuckDB's own text.
    /// </summary>
    public static string UserMessage(DataImportOpenFailure failure, Exception ex, int filesCopied)
    {
        var cause = failure == DataImportOpenFailure.StorageVersion
            ? "The previous install's database was written by a DuckDB storage version this build does not read."
            : "The previous install's database could not be opened: the file is corrupt or incomplete.";
        var outcome = filesCopied switch
        {
            0 => "Its archive folder held no parquet files, so nothing was imported.",
            1 => "Its archive folder was imported: 1 parquet file was copied and the views refreshed. The data it had not yet archived was not imported.",
            _ => $"Its archive folder was imported: {filesCopied} parquet files were copied and the views refreshed. The data it had not yet archived was not imported.",
        };
        return $"{cause} {outcome} Closing the previous application will not help.\n\n{ex.Message}";
    }
}
