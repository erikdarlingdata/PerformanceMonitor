using System;
using System.IO;

namespace UiSmokeWpf;

/// <summary>
/// The check that keeps the Lite walk's seed off a real Lite data folder. The seed overwrites servers.json and settings.json and
/// adds rows to monitor.duckdb in <c>%LOCALAPPDATA%\PerformanceMonitorLite-Data</c>, and Lite has no data-folder override, so on
/// a developer machine that uses Lite it would replace the real server list and settings. The walk is meant for the CI runner,
/// which starts with no such folder and sets the CI environment variable. This file has no other dependency so Lite.Tests
/// compiles it too and tests the check with a temp folder.
/// </summary>
internal static class LiteSeedGuard
{
    /// <summary>Null when the seed may run; otherwise the message to stop with. It may run when the folder does not exist yet or the CI variable is set.</summary>
    /// <param name="dataRoot">The folder the seed would write to.</param>
    /// <param name="ciValue">The value of the CI environment variable, or null when it is not set.</param>
    public static string? Refusal(string dataRoot, string? ciValue)
    {
        if (!string.IsNullOrEmpty(ciValue) || !Directory.Exists(dataRoot))
            return null;

        return $"The Lite walk will not seed {dataRoot}: that folder already exists, and the seed replaces its servers.json and " +
               "settings.json and adds rows to its monitor.duckdb. Run the walk on the CI runner (the CI environment variable is " +
               "set there), or on a machine where that folder does not exist.";
    }
}
