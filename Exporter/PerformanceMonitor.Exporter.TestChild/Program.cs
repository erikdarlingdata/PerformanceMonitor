using System.Diagnostics;
using System.Globalization;

// Behaviour comes from `--tc-*` flags anywhere in the argument list; every other argument is only recorded.
//   --tc-record <file>      append the whole argument list (space-joined, one line) to the file
//   --tc-sleep-ms <n>       sleep n ms before exiting
//   --tc-alloc-mb <n>       allocate and touch n MB, held until exit
//   --tc-print <text>       print a line to stdout            (repeatable)
//   --tc-stderr <text>      print a line to stderr            (repeatable)
//   --tc-exit <code>        the exit code (default 0)
//   --tc-exit-if <s>=<code> exit with code when the joined arguments contain s (first match wins; before --tc-exit)
//   --tc-sleep-if <s>=<ms>  sleep ms when the joined arguments contain s
//   --tc-print-priority x   after any sleep, print "priority=<ProcessPriorityClass>"
//   --tc-write-file <path>  write the pid to the file at start (the lock test reads it)
var tc = new List<(string Key, string Value)>();
for (var i = 0; i < args.Length; i++)
{
    if (args[i].StartsWith("--tc-", StringComparison.Ordinal) && i + 1 < args.Length)
    {
        tc.Add((args[i], args[i + 1]));
        i++;
    }
}
string Joined() => string.Join(' ', args);
string? Get(string key) => tc.FirstOrDefault(t => t.Key == key).Value;

if (Get("--tc-record") is { } record) File.AppendAllText(record, Joined() + "\n");
if (Get("--tc-write-file") is { } pidFile) File.WriteAllText(pidFile, Environment.ProcessId.ToString(CultureInfo.InvariantCulture));

byte[]? held = null;
if (Get("--tc-alloc-mb") is { } alloc)
{
    held = new byte[int.Parse(alloc, CultureInfo.InvariantCulture) * 1024 * 1024];
    for (var i = 0; i < held.Length; i += 4096) held[i] = 1;
}

foreach (var (k, v) in tc.Where(t => t.Key == "--tc-print")) Console.Out.WriteLine(v);
foreach (var (k, v) in tc.Where(t => t.Key == "--tc-stderr")) Console.Error.WriteLine(v);
Console.Out.Flush();
Console.Error.Flush();

var sleep = Get("--tc-sleep-ms") is { } s ? int.Parse(s, CultureInfo.InvariantCulture) : 0;
foreach (var (k, v) in tc.Where(t => t.Key == "--tc-sleep-if"))
{
    var eq = v.LastIndexOf('=');
    if (Joined().Contains(v[..eq], StringComparison.Ordinal)) sleep = Math.Max(sleep, int.Parse(v[(eq + 1)..], CultureInfo.InvariantCulture));
}
if (sleep > 0) Thread.Sleep(sleep);

if (Get("--tc-print-priority") is not null)
{
    Console.Out.WriteLine($"priority={Process.GetCurrentProcess().PriorityClass}");
    Console.Out.Flush();
}

foreach (var (k, v) in tc.Where(t => t.Key == "--tc-exit-if"))
{
    var eq = v.LastIndexOf('=');
    if (Joined().Contains(v[..eq], StringComparison.Ordinal)) return int.Parse(v[(eq + 1)..], CultureInfo.InvariantCulture);
}
GC.KeepAlive(held);
return Get("--tc-exit") is { } code ? int.Parse(code, CultureInfo.InvariantCulture) : 0;
