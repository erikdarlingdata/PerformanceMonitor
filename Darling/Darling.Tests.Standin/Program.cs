using System;
using System.Globalization;
using System.Threading;

namespace Darling.Tests.Standin;

/// <summary>
/// #8847: the process Darling.Tests copies under another file name and starts when a test needs a live process or a
/// command that fails. <c>sleep &lt;seconds&gt;</c> waits that long and exits 0. Any other arguments exit 1 at once,
/// which is how it serves as a pg_ctl that must fail.
/// </summary>
internal static class Program
{
    private static int Main(string[] args)
    {
        if (args.Length == 2
            && string.Equals(args[0], "sleep", StringComparison.Ordinal)
            && int.TryParse(args[1], NumberStyles.None, CultureInfo.InvariantCulture, out var seconds))
        {
            Thread.Sleep(TimeSpan.FromSeconds(seconds));
            return 0;
        }

        Console.Error.WriteLine("Darling.Tests.Standin: only 'sleep <seconds>' is understood.");
        return 1;
    }
}
