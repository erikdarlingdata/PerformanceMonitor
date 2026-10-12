/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// The header of a grid column that prints times in the "Show timestamps in" choice (D5 of the final walk): the
    /// column's name plus the clock it reads on, so a list that mixes servers says how its times are to be read.
    /// Pure, so a unit test names the mode instead of setting the process-wide one.
    /// </summary>
    public static class TimeColumnTitle
    {
        /// <summary>"Time (UTC)", "Time (local)" or "Time (server)". In Server mode every row reads on ITS server's
        /// own clock, so a list of several servers shows each server's hour, and the header says so.</summary>
        public static string For(string name, TimeDisplayMode mode) => mode switch
        {
            TimeDisplayMode.UTC => name + " (UTC)",
            TimeDisplayMode.LocalTime => name + " (local)",
            _ => name + " (server)",
        };
    }
}
