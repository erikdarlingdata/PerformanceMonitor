/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;

namespace PerformanceMonitorLite.Services;

/// <summary>Placeholder so the tests compile; the real store lands in the next commit.</summary>
public sealed class InstallIdStore
{
    public const string FileName = "install-id.json";

    public InstallIdStore(string directory, string machineName, string? userSid)
    {
        FilePath = Path.Combine(directory, FileName);
    }

    public string FilePath { get; }

    internal int ResolveCount => 0;

    internal Action? BeforeCreate { get; set; }

    public string GetId() => "placeholder";

    public static InstallIdStore ForCurrentUser(string directory) =>
        new(directory, Environment.MachineName, null);
}
