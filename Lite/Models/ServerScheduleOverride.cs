/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace PerformanceMonitorLite.Models;

/// <summary>
/// Per-server schedule override. Contains the full collector list for one server.
/// When present, replaces the default schedule entirely for that server.
/// </summary>
public class ServerScheduleOverride
{
    [JsonPropertyName("collectors")]
    public List<CollectorSchedule> Collectors { get; set; } = new();
}
