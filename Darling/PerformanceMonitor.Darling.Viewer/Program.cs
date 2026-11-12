/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Ui;
using Velopack;
using Velopack.Locators;

namespace PerformanceMonitor.Darling.Viewer
{
    /// <summary>
    /// Explicit entry point (wired via &lt;StartupObject&gt; in the csproj) so
    /// <see cref="VelopackApp"/>.Build().Run() runs before any WPF window is created — exactly as
    /// Lite's and Dashboard's Program.Main do. This is what makes the viewer a first-class Velopack
    /// app for the remote-seat Setup.exe (#1555): it handles the install / update / uninstall hooks
    /// Velopack invokes the exe with. On the co-located viewer that ships inside the service zip
    /// (not a Velopack install) it is a no-op, so the same binary serves both deployment modes.
    /// </summary>
    public static class Program
    {
        [STAThread]
        public static void Main(string[] args)
        {
            VelopackApp.Build().Run();

            // #8430: Settings > Apps shows this install's full version, a nightly's tag included. The Setup.exe
            // and the updater write major.minor.patch, and each normal start writes the installed version over it.
            var install = VelopackLocator.Current;
            InstalledAppsVersion.StampCurrentUser(install.AppId, install.CurrentlyInstalledVersion?.ToFullString(), install.IsPortable);

            var app = new App();
            app.InitializeComponent();
            app.Run();
        }
    }
}
