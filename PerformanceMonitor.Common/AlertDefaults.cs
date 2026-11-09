/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// Alert settings whose shipped default both apps share, named once so neither edition carries its own
/// literal. Each app's settings type initializes its property from here, and each app's test project pins
/// that property to the same name, so a copy with a different number fails a test instead of drifting.
/// </summary>
public static class AlertDefaults
{
    /// <summary>
    /// Per-event delivery's cap on the incidents one cycle reports as individual notifications before the
    /// trailing "+N more" batch, and the most analysis PAGE incidents one hold-back window delivers as
    /// individual messages. Lite reads it into <c>App.AlertPerEventMaxPerCycle</c>, the Darling service into
    /// <c>AlertsConfig.PerEventMax</c> (darling.json <c>alerts.perEventMax</c>), the Darling viewer into its settings file and its
    /// alert-settings row. The store's <c>config_alert_settings.per_event_max</c> column default is the same
    /// figure. Every reader clamps the configured value to 1-100.
    /// </summary>
    public const int PerEventMax = 5;
}
