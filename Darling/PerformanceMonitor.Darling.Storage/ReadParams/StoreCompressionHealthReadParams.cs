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

/// <summary>The Custom Views parameters of <c>get_store_compression_health</c> (#9477): it takes none. The Service's own file for the
/// tool holds its handler and catalog entry.</summary>
internal sealed class StoreCompressionHealthReadParams : IReadParamsRegistration
{
    public string Read => "get_store_compression_health";

    public IReadOnlyList<ReadPanelParam> Params { get; } = Array.Empty<ReadPanelParam>();
}
