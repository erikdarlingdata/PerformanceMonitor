using System;

namespace PerformanceMonitor.Ui;

/// <summary>
/// App-supplied conversion from a server-time DateTime to the user's current
/// display-mode time. Set once at app startup; defaults to identity so the lib
/// is usable without wiring. Mirrors the existing static-singleton style of
/// ServerTimeHelper.CurrentDisplayMode / ThemeManager.CurrentTheme.
/// </summary>
public static class UiTimeContext
{
    public static Func<DateTime, DateTime> ConvertForDisplay { get; set; } = static t => t;
}
