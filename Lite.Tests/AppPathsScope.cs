/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */
using System;
using System.IO;
using System.Reflection;
using PerformanceMonitorLite;

/// <summary>
/// The three static store paths a Lite <see cref="MainWindow"/> reads when it is built (#8434): the DuckDB file, the per-user
/// settings folder and the shared folder that holds servers.json. <see cref="App"/> assigns them once at startup and nothing else
/// in the product writes them, so a test that wants a folder of its own sets them here and puts them back through
/// <see cref="AppPathsScope"/>.
/// </summary>
public readonly record struct AppPaths(string DatabasePath, string ConfigDirectory, string SharedConfigDirectory)
{
    /// <summary>What the app's three statics hold now.</summary>
    public static AppPaths Current => new(App.DatabasePath, App.ConfigDirectory, App.SharedConfigDirectory);

    /// <summary>The three paths a test gives a window inside <paramref name="folder"/>: its database file and both config folders.</summary>
    public static AppPaths InFolder(string folder) => new(Path.Combine(folder, "monitor.duckdb"), folder, folder);

    /// <summary>Writes these three paths to the app's statics.</summary>
    public void Apply()
    {
        Set(nameof(App.DatabasePath), DatabasePath);
        Set(nameof(App.ConfigDirectory), ConfigDirectory);
        Set(nameof(App.SharedConfigDirectory), SharedConfigDirectory);
    }

    /* The setters are private to App, so a test reaches them by name. */
    private static void Set(string name, string value) =>
        typeof(App).GetProperty(name, BindingFlags.Static | BindingFlags.Public)!.SetValue(null, value);
}

/// <summary>
/// Points the app's three store paths at a folder of the test's own for as long as the scope lives, and puts back what they held
/// when it is disposed (#8434). The statics are process-wide: a test that set them and did not put them back left every later test
/// that builds a <see cref="MainWindow"/> in the same process reading and writing a folder the earlier test had deleted. Dispose the
/// scope before the folder is deleted, so no window is ever pointed at a folder that is gone.
/// </summary>
public sealed class AppPathsScope : IDisposable
{
    private readonly AppPaths _saved;

    /// <summary>The folder the three paths point at while the scope lives.</summary>
    public string Folder { get; }

    public AppPathsScope(string folder)
    {
        Folder = folder;
        _saved = AppPaths.Current;
        AppPaths.InFolder(folder).Apply();
    }

    public void Dispose() => _saved.Apply();
}
