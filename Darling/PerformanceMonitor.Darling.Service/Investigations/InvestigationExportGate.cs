/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// What the web download of the investigation cases shares across requests (#6236): at most ONE export runs at a time in the
/// process, from the moment its work file exists until the file is gone, so the disk it holds is one file, and a second
/// request answers busy instead of waiting. It also names the folder the work file lives in (the service's own data folder)
/// and carries the one test seam of the route.
/// </summary>
public sealed class InvestigationExportGate(string folder)
{
    private int _running;

    /// <summary>The folder the work file is created in.</summary>
    internal string Folder { get; } = folder;

    /// <summary>A test seam: awaited after the cases are written and the read-admission slot is released, before the first byte is sent.</summary>
    internal Func<Task>? BeforeSend { get; set; }

    /// <summary>Takes the one export slot; false when an export is already running.</summary>
    internal bool TryEnter() => Interlocked.CompareExchange(ref _running, 1, 0) == 0;

    /// <summary>Gives the export slot back.</summary>
    internal void Exit() => Volatile.Write(ref _running, 0);

    /// <summary>The folder beside the store's data folder, where the service already keeps its own files (credentials, server log):
    /// <c>exports</c> under it.</summary>
    internal static string FolderFor(string storeDataDirectory)
    {
        var parent = Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(Path.GetFullPath(storeDataDirectory)));
        return Path.Combine(string.IsNullOrEmpty(parent) ? Path.GetFullPath(storeDataDirectory) : parent, "exports");
    }
}
