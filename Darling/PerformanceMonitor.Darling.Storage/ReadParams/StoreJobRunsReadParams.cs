/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>The Custom Views parameters of <c>get_store_job_runs</c> (#9477). The Service's own file for the tool holds its handler and
/// catalog entry.</summary>
internal sealed class StoreJobRunsReadParams : IReadParamsRegistration
{
    public string Read => "get_store_job_runs";

    public IReadOnlyList<ReadPanelParam> Params { get; } = new ReadPanelParam[]
    {
        new("hours", false), new("job_kind", false), new("hypertable", false), new("limit", false), new("as_of", false),
    };
}
