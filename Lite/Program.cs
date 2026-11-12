/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Ui;
using Velopack;
using Velopack.Locators;

namespace PerformanceMonitorLite
{
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
