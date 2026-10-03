/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Viewer;

public partial class ViewerServerTab
{
    /// <summary>
    /// Awaits a load's main read, started beside its data-start probe, and watches the probe if that read throws (#5022). A load
    /// awaits its probe only at the banner step, after the read; a read that throws unwinds past that step, and the probe task is then
    /// never awaited, so a probe that fails later reaches <c>App.OnUnobservedTaskException</c> and is logged as an Error. The probe is
    /// only the "Showing since" note, so its failure is a warning (<see cref="DataStartAnswerAsync"/> logs it as one), and the read's
    /// own exception goes on to the shell's handler as before. A read that succeeds leaves the probe to the banner step.
    /// </summary>
    /// <param name="read">The load's main read: one read, or the join of several.</param>
    /// <param name="probe">The data-start probe the tab started beside the read.</param>
    /// <param name="surface">The surface the probe's warning names.</param>
    /// <param name="warn">Takes the log source and message of a failed probe; <see cref="ViewerLogger.Warn"/> by default.</param>
    internal static async Task AwaitReadWatchingProbeAsync(Task read, Task<DateTime?> probe, string surface, Action<string, string>? warn = null)
    {
        ArgumentNullException.ThrowIfNull(read);
        ArgumentNullException.ThrowIfNull(probe);

        try
        {
            await read;
        }
        catch
        {
            /* Not awaited: DataStartAnswerAsync catches the probe's failure and logs it, so the task it returns cannot fault. */
            _ = DataStartAnswerAsync(probe, surface, warn);
            throw;
        }
    }
}
