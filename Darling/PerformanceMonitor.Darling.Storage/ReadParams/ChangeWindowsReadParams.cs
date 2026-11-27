/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>The Custom Views parameters of <c>get_change_windows</c> (#9485): one window by id, or the windows in the last <c>hours_back</c> hours,
/// paged by <c>limit</c> and <c>offset</c>. The Service's own file for the tool holds its handler and catalog entry.</summary>
internal sealed class ChangeWindowsReadParams : IReadParamsRegistration
{
    public string Read => "get_change_windows";

    public IReadOnlyList<ReadPanelParam> Params { get; } = new ReadPanelParam[]
    {
        new("window_id", false), new("hours_back", false), new("limit", false), new("offset", false),
    };
}
