/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// The clock a grid that lists several servers reads its times on (#6137). "Server time" means each row's own
    /// server's wall clock, which is a different zone on every row of a mixed list: the cells carry no zone, so a
    /// 20:27 row from a UTC server looked later than a 13:26 row from a server at UTC-4 that actually ran later. A list
    /// of several servers therefore reads on one clock for every row: the viewer's own (Local) when Server time is
    /// chosen, and UTC or Local as chosen. A list of one server keeps its server's clock. Pure, so a test names the mode.
    /// </summary>
    public static class MultiServerTime
    {
        /// <summary>True when <paramref name="serverIds"/> holds more than one distinct server.</summary>
        public static bool IsMixed(IEnumerable<int> serverIds)
        {
            int? first = null;
            foreach (var id in serverIds)
            {
                if (first is null)
                {
                    first = id;
                }
                else if (first.Value != id)
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>The mode the times of a list are drawn in: Server time becomes Local for a list of several servers;
        /// every other case is the mode chosen.</summary>
        public static TimeDisplayMode EffectiveMode(TimeDisplayMode chosen, bool mixedServers)
            => mixedServers && chosen == TimeDisplayMode.ServerTime ? TimeDisplayMode.LocalTime : chosen;

        /// <summary>The UTC offset as one label, "UTC-04:00" or "UTC+00:00": the spelling the time range picker uses, so
        /// every label that names a server's offset reads the same (#6137).</summary>
        public static string OffsetLabel(int utcOffsetMinutes)
        {
            var magnitude = System.Math.Abs(utcOffsetMinutes);
            return string.Create(
                System.Globalization.CultureInfo.InvariantCulture,
                $"UTC{(utcOffsetMinutes < 0 ? '-' : '+')}{magnitude / 60:00}:{magnitude % 60:00}");
        }
    }
}
