/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

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

    /// <summary>Any other open failure, such as a corrupt or truncated file. Retrying cannot fix this.</summary>
    Other,
}

/// <summary>
/// Tells a real file lock apart from the open failures no retry can fix, so Import Data asks the user to close the
/// previous application only when that is the cause. Matches on the exception's full text, inner exceptions included,
/// with the phrases DuckDB uses for each cause: a lock reports "Could not set lock on file" or "Conflicting lock", and a
/// storage mismatch reports "Trying to read a database file with version number" or "Failed to deserialize". Pure, so a
/// test drives it with the message shapes alone.
/// </summary>
public static class DataImportOpenFailureClassifier
{
    public static DataImportOpenFailure Classify(Exception ex)
    {
        var text = ex.ToString().ToLowerInvariant();
        if (text.Contains("conflicting lock") || text.Contains("could not set lock") || text.Contains("being used by another process"))
        {
            return DataImportOpenFailure.Locked;
        }

        if (text.Contains("failed to deserialize") || text.Contains("trying to read a database file with version") || text.Contains("storage version"))
        {
            return DataImportOpenFailure.StorageVersion;
        }

        return DataImportOpenFailure.Other;
    }

    /// <summary>
    /// The message the import reports for a failure no retry can fix. It names the cause and carries DuckDB's own text.
    /// </summary>
    public static string UserMessage(DataImportOpenFailure failure, Exception ex) => failure switch
    {
        DataImportOpenFailure.StorageVersion =>
            "The previous install's database was written by a DuckDB storage version this build does not read, so the data it has not yet archived cannot be imported. Closing the previous application will not help.\n\n" + ex.Message,
        _ =>
            "The previous install's database could not be opened. The file may be corrupt or incomplete, so the data it has not yet archived cannot be imported. Closing the previous application will not help.\n\n" + ex.Message,
    };
}
