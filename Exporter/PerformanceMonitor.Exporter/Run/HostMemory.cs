using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;

namespace PerformanceMonitor.Exporter.Run;

/// <summary>Free physical memory in MB, or null when this host cannot say (the guard is then skipped).</summary>
public interface IHostMemory
{
    int? FreeMb();
}

/// <summary>
/// Windows: GlobalMemoryStatusEx.ullAvailPhys. Linux: MemAvailable from /proc/meminfo. Anything else warns once and returns null.
/// DARLING_EXPORT_TEST_FREE_MB_FILE (a file holding a number, wins) and DARLING_EXPORT_TEST_FREE_MB fake the value for tests.
/// </summary>
public sealed class SystemHostMemory(TextWriter warn) : IHostMemory
{
    private bool warned;

    public int? FreeMb()
    {
        var file = Environment.GetEnvironmentVariable("DARLING_EXPORT_TEST_FREE_MB_FILE");
        if (!string.IsNullOrEmpty(file) && File.Exists(file)
            && int.TryParse(File.ReadAllText(file).Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var fromFile))
        {
            return fromFile;
        }
        var env = Environment.GetEnvironmentVariable("DARLING_EXPORT_TEST_FREE_MB");
        if (!string.IsNullOrEmpty(env) && int.TryParse(env, NumberStyles.Integer, CultureInfo.InvariantCulture, out var fromEnv)) return fromEnv;

        try
        {
            if (OperatingSystem.IsWindows()) return WindowsFreeMb();
            if (OperatingSystem.IsLinux()) return LinuxFreeMb();
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or FormatException)
        {
            return null;
        }

        if (!warned)
        {
            warned = true;
            warn.WriteLine("warning: free memory is not available on this platform; the memory guard is skipped");
        }
        return null;
    }

    internal static int? ParseMemAvailable(IEnumerable<string> meminfoLines)
    {
        foreach (var line in meminfoLines)
        {
            if (!line.StartsWith("MemAvailable:", StringComparison.Ordinal)) continue;
            var digits = new string(line["MemAvailable:".Length..].Where(char.IsAsciiDigit).ToArray());
            return digits.Length == 0 ? null : (int)(long.Parse(digits, CultureInfo.InvariantCulture) / 1024);
        }
        return null;
    }

    private static int? LinuxFreeMb() => ParseMemAvailable(File.ReadLines("/proc/meminfo"));

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
    private struct MemoryStatusEx
    {
        public uint Length;
        public uint MemoryLoad;
        public ulong TotalPhys;
        public ulong AvailPhys;
        public ulong TotalPageFile;
        public ulong AvailPageFile;
        public ulong TotalVirtual;
        public ulong AvailVirtual;
        public ulong AvailExtendedVirtual;
    }

    [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GlobalMemoryStatusEx(ref MemoryStatusEx buffer);

    private static int? WindowsFreeMb()
    {
        var status = new MemoryStatusEx { Length = (uint)Marshal.SizeOf<MemoryStatusEx>() };
        return GlobalMemoryStatusEx(ref status) ? (int)(status.AvailPhys / (1024 * 1024)) : null;
    }
}

/// <summary>A fixed value for tests, optionally changing over time.</summary>
public sealed class FakeHostMemory(Func<int?> next) : IHostMemory
{
    public FakeHostMemory(int? fixedMb) : this(() => fixedMb) { }
    public int? FreeMb() => next();
}
