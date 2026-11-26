namespace PerformanceMonitor.Exporter;

/// <summary>
/// The exporter's diagnostic output on stderr. A line that explains the run to its reader (a warning about a limit the
/// login cannot set) is written once per process. A line that only helps someone debugging the exporter (the session
/// guards applied on each connection) is written only when <see cref="Verbose"/> is on, which the `--verbose` flag of
/// `export` and `run` turns on. A failure's stack trace is written to stdout whenever stdout is redirected, and to an
/// interactive console only with <see cref="Verbose"/>.
/// </summary>
public static class ExporterLog
{
    private static readonly HashSet<string> Warned = new(StringComparer.Ordinal);
    private static readonly object Gate = new();

    /// <summary>True when `--verbose` was given. Set once at startup.</summary>
    public static bool Verbose { get; set; }

    /// <summary>Writes <paramref name="message"/> to stderr the first time <paramref name="key"/> is seen in this process.</summary>
    public static void WarnOnce(string key, string message)
    {
        lock (Gate)
        {
            if (!Warned.Add(key))
            {
                return;
            }
        }

        Console.Error.WriteLine(message);
    }

    /// <summary>Writes a `DEBUG:` line to stderr, only with <see cref="Verbose"/>.</summary>
    public static void Debug(string message)
    {
        if (Verbose)
        {
            Console.Error.WriteLine("DEBUG: " + message);
        }
    }

    /// <summary>Forgets every warning written so far and turns verbose off. For tests.</summary>
    public static void Reset()
    {
        lock (Gate)
        {
            Warned.Clear();
        }

        Verbose = false;
    }

    /// <summary>
    /// The text of a failure. The full exception, stack frames included, when stdout is not an interactive console (the
    /// scheduler keeps a child's stdout in the run's log, so a bug in a scheduled run stays diagnosable) or with
    /// <see cref="Verbose"/>. An interactive console gets each exception in the chain as its type and message alone, which
    /// names the layer that raised the error without printing frames.
    /// </summary>
    /// <param name="ex">The failure.</param>
    /// <param name="stdoutIsConsole">True when stdout is an interactive console, false when it is redirected to a pipe or a file.</param>
    public static string DescribeFailure(Exception ex, bool stdoutIsConsole)
    {
        if (Verbose || !stdoutIsConsole)
        {
            return ex.ToString();
        }

        var lines = new List<string>();
        for (Exception? current = ex; current is not null; current = current.InnerException)
        {
            lines.Add($"{current.GetType().Name}: {current.Message}");
        }

        return string.Join(Environment.NewLine + "  caused by ", lines);
    }
}
