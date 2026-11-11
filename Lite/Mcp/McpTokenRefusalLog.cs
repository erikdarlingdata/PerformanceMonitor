using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The rate limit on the MCP token gate's refusal log line (#8611). Darling's MCP host writes one line per refusal and holds back a
/// run of them (<c>DarlingHttpRefusalLog</c>: the first refusal is logged at once, later ones inside the window are counted, and the
/// first one after the window is logged with the count). Lite's listener is loopback only, so every client is one source and the
/// limiter keeps one stamp, not a table per source. It remembers a time and a count, never a token or any part of one.
/// </summary>
internal sealed class McpTokenRefusalLog
{
    /// <summary>The window Darling's refusal log uses.</summary>
    internal static readonly TimeSpan DefaultWindow = TimeSpan.FromMinutes(10);

    private readonly TimeSpan _window;
    private readonly object _sync = new();
    private DateTime? _lastLoggedUtc;
    private int _suppressed;

    internal McpTokenRefusalLog(TimeSpan? window = null) => _window = window ?? DefaultWindow;

    /// <summary>Counts a refusal at <paramref name="nowUtc"/>. True, with the number held back since the last line, when this one is to be
    /// logged; false when it falls inside the window of the last line and is only counted.</summary>
    internal bool TryLog(DateTime nowUtc, out int suppressedSinceLastLog)
    {
        lock (_sync)
        {
            suppressedSinceLastLog = 0;
            if (_lastLoggedUtc is { } last)
            {
                /* A last-logged time ahead of the clock (it stepped back since the line) is replaced by this reading, as Darling's log does. */
                last = LastFiredStamp.Settle(last, nowUtc);
                if (nowUtc - last < _window)
                {
                    _lastLoggedUtc = last;
                    _suppressed++;
                    return false;
                }

                suppressedSinceLastLog = _suppressed;
            }

            _suppressed = 0;
            _lastLoggedUtc = nowUtc;
            return true;
        }
    }
}
