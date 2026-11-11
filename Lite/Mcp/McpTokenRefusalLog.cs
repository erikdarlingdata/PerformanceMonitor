using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The rate limit on the MCP token gate's refusal log line (#8611). Darling's MCP host writes one line per refusal and holds back a
/// run of them (<c>DarlingHttpRefusalLog</c>: the first refusal is logged at once, later ones inside the window are counted, and the
/// first one after the window is logged with the count). Lite's listener is loopback only, so every client is one source and the
/// limiter keeps one slot per refusal state (no header, not a Bearer, a Bearer that does not match), not a table per source. A slot
/// holds a time and a count, never a token or any part of one. A slot per state means a "no header" line never hides the first
/// wrong-token refusal that follows it: while a person sets a client up, the state changes and the new state is logged at once.
/// </summary>
internal sealed class McpTokenRefusalLog
{
    /// <summary>The window Darling's refusal log uses.</summary>
    internal static readonly TimeSpan DefaultWindow = TimeSpan.FromMinutes(10);

    private readonly TimeSpan _window;
    private readonly object _sync = new();
    private readonly Dictionary<string, Slot> _slots = new(StringComparer.Ordinal);

    internal McpTokenRefusalLog(TimeSpan? window = null) => _window = window ?? DefaultWindow;

    /// <summary>Counts a refusal in <paramref name="state"/> (one of the few fixed descriptions of what the client sent, never a token)
    /// at <paramref name="nowUtc"/>. True, with the number held back in that state since its last line, when this one is to be logged;
    /// false when it falls inside the window of the last line for that state and is only counted.</summary>
    internal bool TryLog(DateTime nowUtc, string state, out int suppressedSinceLastLog)
    {
        lock (_sync)
        {
            suppressedSinceLastLog = 0;
            if (_slots.TryGetValue(state, out var slot))
            {
                /* A last-logged time ahead of the clock (it stepped back since the line) is replaced by this reading, as Darling's log does. */
                slot.LastLoggedUtc = LastFiredStamp.Settle(slot.LastLoggedUtc, nowUtc);
                if (nowUtc - slot.LastLoggedUtc < _window)
                {
                    slot.Suppressed++;
                    return false;
                }

                suppressedSinceLastLog = slot.Suppressed;
            }
            else
            {
                slot = new Slot();
                _slots[state] = slot;
            }

            slot.Suppressed = 0;
            slot.LastLoggedUtc = nowUtc;
            return true;
        }
    }

    private sealed class Slot
    {
        public DateTime LastLoggedUtc;
        public int Suppressed;
    }
}
