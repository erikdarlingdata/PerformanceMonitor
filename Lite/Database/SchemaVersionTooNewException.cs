/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// #4727: the data file is stamped with a schema version newer than this build understands (an older Lite pointed
/// at the shared data root after a rollback, or an older portable copy). Schema versions 60 to 65 added columns to
/// 13 tables, so an older build that carried on would fail every batch for those tables with only a log line to
/// show for it. <see cref="DuckDbInitializer.InitializeAsync"/> throws this before it creates or alters anything,
/// and the startup handler in MainWindow shows <see cref="Exception.Message"/> to the user.
/// </summary>
public sealed class SchemaVersionTooNewException : InvalidOperationException
{
    public SchemaVersionTooNewException(string databasePath, int fileVersion, int appVersion)
        : base(
            $"The data file {databasePath} was written by a newer version of Performance Monitor Lite. " +
            $"It is at schema version {fileVersion}, and this version only understands up to schema version {appVersion}. " +
            "The file has not been changed. Update Performance Monitor Lite to the newer version, " +
            "or start this one against a different data folder.")
    {
        FileVersion = fileVersion;
        AppVersion = appVersion;
    }

    /// <summary>The schema version the data file is stamped with.</summary>
    public int FileVersion { get; }

    /// <summary>The newest schema version this build understands.</summary>
    public int AppVersion { get; }
}
