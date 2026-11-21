/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Velopack;
using Velopack.Locators;

namespace PerformanceMonitorLite
{
    public static class Program
    {
        [STAThread]
        public static int Main(string[] args)
        {
            /* #9185: --print-mcp-token reads one Credential Manager entry and prints it. It runs before anything else: no Velopack
               hook, no registry stamp, no WPF application, no log file, no single-instance lock, so a running Lite is not disturbed
               and the token reaches nothing but standard output. */
            if (LiteStartupPlan.FromArgs(args).PrintMcpToken)
            {
                return PrintMcpTokenCommand.RunProcess();
            }

            /* #9237: --add-server writes servers.json, this user's credential and host confirmation, and exits. Same early exit as the
               token print: no Velopack hook, no WPF application, no log file, no single-instance lock. */
            if (LiteStartupPlan.FromArgs(args).AddServer)
            {
                return AddServerCommand.RunProcess(args);
            }

            VelopackApp.Build().Run();

            // #8430: Settings > Apps shows this install's full version, a nightly's tag included. The Setup.exe
            // and the updater write major.minor.patch, and each normal start writes the installed version over it.
            var install = VelopackLocator.Current;
            InstalledAppsVersion.StampCurrentUser(install.AppId, install.CurrentlyInstalledVersion?.ToFullString(), install.IsPortable);

            var app = new App();
            app.InitializeComponent();
            /* #9142: Run's return value is the process exit code (Shutdown(code)). --stop's 0/1/2 and a failed headless start's
               nonzero code reach %ERRORLEVEL% and Task Scheduler's last run result only if Main returns it. */
            return app.Run();
        }
    }
}
