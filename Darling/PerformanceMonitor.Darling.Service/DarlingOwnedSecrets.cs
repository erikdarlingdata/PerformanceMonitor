/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace PerformanceMonitor.Darling.Service;

/// <summary>The files and environment variables that belong to Darling itself.</summary>
public sealed record DarlingOwnedSet(IReadOnlyList<string> Paths, IReadOnlyList<string> EnvNames)
{
    public static DarlingOwnedSet Empty { get; } = new(Array.Empty<string>(), Array.Empty<string>());
}

/// <summary>
/// Process-wide holder for the owned set. A static is used because <c>DarlingConfig.Load</c> is called
/// separately by the worker, the MCP host and the web host, and the server-add core is a static method with
/// no dependency injection: each host populates the holder the same way from <c>Load</c>, and the add core
/// reads it. Written once at startup (<see cref="Set"/>), then read; the reference swap is atomic.
/// </summary>
public static class DarlingOwnedSecrets
{
    /// <summary>The state before any configuration has loaded. Compared by REFERENCE: an <see cref="DarlingOwnedSet.Empty"/>
    /// set is a populated set that owns nothing, which is not the same thing and is not refused wholesale.</summary>
    internal static DarlingOwnedSet Unpopulated { get; } = new(Array.Empty<string>(), Array.Empty<string>());

    private static volatile DarlingOwnedSet s_current = Unpopulated;
    private const int MaxDepth = 8;

    /// <summary>Environment variables the service itself reads. A census test keeps this complete.</summary>
    internal static readonly IReadOnlySet<string> ServiceEnvNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "DARLING_CONFIG",
        "DARLING_OUTPUT_FORMAT",
        "DARLING_STOPPED_MARKER",
        "DOTNET_RUNNING_IN_CONTAINER",
        "SystemDrive",
        "USERPROFILE",
    };

    /// <summary>The current owned set. Until a configuration has loaded this is <see cref="Unpopulated"/>, and every
    /// <c>env:</c>/<c>file:</c> reference is refused: a guard that exists only when the configuration happened to load
    /// is not a guard.</summary>
    public static DarlingOwnedSet Current => s_current;

    public static void Set(DarlingOwnedSet set) => s_current = set ?? Unpopulated;

    /// <summary>One sentence for every refusal; it names no path and no variable.</summary>
    internal const string ReferenceRefusalText =
        "That password reference points at this service's own configuration or secrets.";

    private const int MaxLinkHops = 40;

    private static StringComparison EnvComparison =>
        OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    /* Windows and macOS volumes are case-insensitive by default. Identity (below) is the real answer for a file that
       exists; this is the text fallback for one that does not, so it errs toward refusing on macOS. */
    private static StringComparison PathComparison =>
        OperatingSystem.IsWindows() || OperatingSystem.IsMacOS() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    /// <summary>Null when <paramref name="password"/> is not an <c>env:</c>/<c>file:</c> reference or the reference
    /// points at nothing Darling owns; otherwise <see cref="ReferenceRefusalText"/>.
    /// Identity comparison (volume + file ID of each owned path, a small known set) catches a symlink, a case variant and a
    /// Windows short name. A hard link placed outside every owned directory is out of scope: making one needs local read
    /// access to the target, which already defeats this control, and finding one would mean statting every file under the
    /// owned directories (thousands, in the database data directory) on an interactive path.
    /// When the identity of the referenced path, or of a directory above it, cannot be determined, the answer depends on
    /// WHY. A path that does not exist (no file, no directory) has no identity and is accepted: the reference may name a
    /// file that is not there yet, and the connection attempt reports the real failure. A path that exists but cannot be
    /// examined (access denied, or any other failure that is not absence) is REFUSED, because nothing proves it is not one
    /// of Darling's own files; the refusal text is the same sentence. The identity source reports absence by returning
    /// null and every other failure by throwing.
    /// An owned path whose own identity cannot be read contributes no identity to the comparison; the text comparison
    /// still covers it.</summary>
    internal static string? ReferenceRefusal(string? password) => ReferenceRefusal(password, s_current);

    internal static string? ReferenceRefusal(string? password, DarlingOwnedSet owned)
        => ReferenceRefusal(password, owned, FileIdentity.Of);

    internal static string? ReferenceRefusal(string? password, DarlingOwnedSet owned, Func<string, FileId?> identityOf)
    {
        if (string.IsNullOrWhiteSpace(password))
        {
            return null;
        }

        var value = password.Trim();
        var isFile = value.StartsWith("file:", StringComparison.Ordinal);
        var isEnv = value.StartsWith("env:", StringComparison.Ordinal);
        if ((isFile || isEnv) && ReferenceEquals(owned, Unpopulated))
        {
            return ReferenceRefusalText; /* No configuration has loaded, so nothing can be said to be safe. */
        }

        if (isFile)
        {
            return FileRefused(value["file:".Length..].Trim(), owned, identityOf) ? ReferenceRefusalText : null;
        }

        if (isEnv)
        {
            var name = value["env:".Length..].Trim();
            return owned.EnvNames.Any(n => string.Equals(n, name, EnvComparison)) ? ReferenceRefusalText : null;
        }

        return null;
    }

    internal static bool FileRefused(string path, DarlingOwnedSet owned, Func<string, FileId?> identityOf)
    {
        if (IsRefusedForm(path))
        {
            return true;
        }

        var real = RealPath(path);
        if (real is null)
        {
            return true;
        }

        var ownedIds = new HashSet<FileId>();
        foreach (var ownedPath in owned.Paths)
        {
            if (string.IsNullOrWhiteSpace(ownedPath) || ownedPath.Contains('\0'))
            {
                continue;
            }

            /* Text first: it covers an owned path that does not exist yet (a log or key the store has not written),
               where identity has nothing to read. */
            var ownedReal = RealPath(ownedPath);
            if (ownedReal is not null && Covers(ownedReal, real))
            {
                return true;
            }

            if (Safe(identityOf, ownedPath) is { } id)
            {
                ownedIds.Add(id);
            }

            if (ownedReal is not null && Safe(identityOf, ownedReal) is { } realId)
            {
                ownedIds.Add(realId);
            }
        }

        if (ownedIds.Count > 0 && ReachesOwned(path, real, ownedIds, identityOf))
        {
            return true;
        }

        return false;
    }

    /// <summary>True when the file, or any directory above it, IS an owned file or directory — by volume and file ID,
    /// so a hard link, a bind mount, a Windows 8.3 short name and a case variant all arrive at the same identity. A file
    /// that does not exist has no identity of its own; its nearest existing ancestor still does. Also true when the
    /// identity of the file or of an ancestor exists but cannot be read (see <see cref="Safe"/>): with no way to prove the
    /// path is not owned, it is treated as owned. Absence is not that case and does not refuse.</summary>
    private static bool ReachesOwned(string path, string real, HashSet<FileId> ownedIds, Func<string, FileId?> identityOf)
    {
        foreach (var start in new[] { path, real })
        {
            string? current;
            try
            {
                current = Path.GetFullPath(start);
            }
            catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
            {
                return true;
            }

            for (var i = 0; i < 256 && current is not null; i++)
            {
                var lookup = Lookup(identityOf, current);
                if (lookup.Undeterminable || (lookup.Id is { } id && ownedIds.Contains(id)))
                {
                    return true;
                }

                current = Path.GetDirectoryName(current);
            }
        }

        return false;
    }

    /// <summary>The result of one identity lookup. <see cref="Id"/> null with <see cref="Undeterminable"/> false means the
    /// path does not exist; <see cref="Undeterminable"/> true means it could not be examined, for a reason other than absence.</summary>
    private readonly record struct IdentityLookup(FileId? Id, bool Undeterminable);

    /// <summary>Identity of a path, keeping the reason a lookup produced nothing. The seam contract: null is absence;
    /// an exception is a failure to examine something that may exist.</summary>
    private static IdentityLookup Lookup(Func<string, FileId?> identityOf, string path)
    {
        try
        {
            return new IdentityLookup(identityOf(path), false);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException
                                       or DllNotFoundException or EntryPointNotFoundException or MarshalDirectiveException)
        {
            return new IdentityLookup(null, true);
        }
    }

    /// <summary>For the owned side only: an owned path with no readable identity simply adds none to the set.</summary>
    private static FileId? Safe(Func<string, FileId?> identityOf, string path) => Lookup(identityOf, path).Id;

    /// <summary>Forms no real secret path needs, refused before any comparison.</summary>
    private static bool IsRefusedForm(string path)
    {
        if (path.Length == 0 || path.Contains('\0') || !Path.IsPathRooted(path) || path[0] == '~')
        {
            return true;
        }

        if (path.StartsWith("\\\\", StringComparison.Ordinal) || path.StartsWith("//", StringComparison.Ordinal))
        {
            return true; /* UNC root, and the \\?\ and \\.\ prefixes */
        }

        if (path.Split('/', '\\').Contains(".."))
        {
            return true;
        }

        var hasDrive = path.Length >= 2 && char.IsAsciiLetter(path[0]) && path[1] == ':';
        if (path.IndexOf(':', hasDrive ? 2 : 0) >= 0)
        {
            return true; /* alternate data stream */
        }

        return path.Equals("/proc", StringComparison.Ordinal) || path.StartsWith("/proc/", StringComparison.Ordinal)
            || path.Equals("/sys", StringComparison.Ordinal) || path.StartsWith("/sys/", StringComparison.Ordinal);
    }

    /// <summary>The path with every symbolic link followed (capped; null on a cycle or an unresolvable path).</summary>
    private static string? RealPath(string path)
    {
        try
        {
            var full = Path.GetFullPath(path);
            var root = Path.GetPathRoot(full) ?? "";
            var queue = new LinkedList<string>(full[root.Length..].Split(
                new[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries));
            var current = root;
            var hops = 0;
            while (queue.First is { } head)
            {
                queue.RemoveFirst();
                var part = head.Value;
                if (part == ".")
                {
                    continue;
                }

                if (part == "..")
                {
                    current = Path.GetDirectoryName(current) ?? root;
                    continue;
                }

                var next = Path.Combine(current, part);
                var target = new DirectoryInfo(next).LinkTarget;
                if (target is null)
                {
                    current = next;
                    continue;
                }

                if (++hops > MaxLinkHops)
                {
                    return null;
                }

                var targetFull = Path.IsPathRooted(target) ? target : Path.Combine(current, target);
                var targetRoot = Path.GetPathRoot(targetFull) ?? "";
                current = targetRoot;
                var parts = targetFull[targetRoot.Length..].Split(
                    new[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries);
                for (var i = parts.Length - 1; i >= 0; i--)
                {
                    queue.AddFirst(parts[i]);
                }
            }

            return Path.GetFullPath(current);
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException or IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    private static bool Covers(string directory, string path)
    {
        var dir = Path.TrimEndingDirectorySeparator(directory);
        if (path.Equals(dir, PathComparison) || path.Equals(directory, PathComparison))
        {
            return true;
        }

        return path.Length > dir.Length
            && path.StartsWith(dir, PathComparison)
            && (path[dir.Length] == Path.DirectorySeparatorChar || path[dir.Length] == Path.AltDirectorySeparatorChar);
    }

    /// <summary>Walks string properties of the graph by reflection and returns every env:/file: value as written.</summary>
    internal static List<string> CollectReferences(object root)
    {
        var found = new List<string>();
        Walk(root, found, new HashSet<object>(ReferenceEqualityComparer.Instance), 0, pathsOnly: false, paths: null);
        return found;
    }

    private static bool IsOurs(Type t) => t.Assembly == typeof(DarlingOwnedSecrets).Assembly;

    private static void Walk(object? node, List<string> refs, HashSet<object> seen, int depth, bool pathsOnly, List<string>? paths)
    {
        if (node is null || depth > MaxDepth || !seen.Add(node))
        {
            return;
        }

        if (node is string)
        {
            return;
        }

        if (node is IEnumerable items)
        {
            foreach (var item in items)
            {
                if (item is string str)
                {
                    Visit(str, null, refs, paths);
                }
                else if (item is not null && IsOurs(item.GetType()))
                {
                    Walk(item, refs, seen, depth + 1, pathsOnly, paths);
                }
            }

            return;
        }

        if (!IsOurs(node.GetType()))
        {
            return;
        }

        foreach (var prop in node.GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
        {
            if (prop.GetIndexParameters().Length != 0 || !prop.CanRead)
            {
                continue;
            }

            if (Attribute.IsDefined(prop, typeof(System.Text.Json.Serialization.JsonIgnoreAttribute)))
            {
                continue;
            }

            object? value;
            try
            {
                value = prop.GetValue(node);
            }
            catch (Exception ex) when (ex is TargetInvocationException or InvalidOperationException)
            {
                continue;
            }

            if (value is string s)
            {
                Visit(s, prop.Name, refs, paths);
            }
            else if (value is not null && !value.GetType().IsPrimitive && !value.GetType().IsEnum)
            {
                Walk(value, refs, seen, depth + 1, pathsOnly, paths);
            }
        }
    }

    private static void Visit(string value, string? propName, List<string> refs, List<string>? paths)
    {
        if (DarlingSecretSource.IsReference(value))
        {
            refs.Add(value);
        }
        else if (paths is not null && propName is not null && value.Length > 0
                 && (propName.EndsWith("Path", StringComparison.Ordinal) || propName.EndsWith("Directory", StringComparison.Ordinal)))
        {
            paths.Add(value);
        }
    }

    /// <summary>Builds the owned set for a loaded config: config directory, every referenced file/env var,
    /// path-valued settings, the store data directory, and the managed store's credential/key/log files that
    /// sit in that directory's PARENT.</summary>
    internal static DarlingOwnedSet Compute(DarlingConfig config, string configPath)
    {
        var paths = new List<string>();
        var envNames = new List<string>(ServiceEnvNames);

        try
        {
            var dir = Path.GetDirectoryName(Path.GetFullPath(configPath));
            if (!string.IsNullOrEmpty(dir))
            {
                paths.Add(dir);
            }
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
        {
            /* An unresolvable config path owns nothing we can name. */
        }

        foreach (var r in config.SecretReferencesAsWritten)
        {
            if (r.StartsWith("file:", StringComparison.Ordinal))
            {
                paths.Add(r["file:".Length..].Trim());
            }
            else
            {
                envNames.Add(r["env:".Length..].Trim());
            }
        }

        var pathValues = new List<string>();
        Walk(config, new List<string>(), new HashSet<object>(ReferenceEqualityComparer.Instance), 0, false, pathValues);
        paths.AddRange(pathValues);

        /* A managed store with no dataDirectory lives at the DEFAULT directory, so resolve it exactly as the store does;
           reading the setting as written would leave the default install owning none of its own files. */
        var data = config.Postgres is { Managed: true } managed
            ? DarlingManagedPostgres.ResolveDataDirectory(managed)
            : config.Postgres?.DataDirectory;

        /* The compose distribution's credential directory (plaintext role passwords) and the log-hash key's directory
           for this configuration. Both are directories; everything under them is owned. */
        paths.Add(DarlingManagedRoles.ComposeStoreCredentialDirectory);
        try
        {
            paths.Add(DarlingLogHashKeyFile.DirectoryFor(config, configPath));
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException or InvalidOperationException)
        {
            /* An unresolvable key directory owns nothing we can name. */
        }

        if (!string.IsNullOrWhiteSpace(data))
        {
            paths.Add(data);
            try
            {
                var parent = Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(Path.GetFullPath(data)));
                if (!string.IsNullOrEmpty(parent))
                {
                    foreach (var name in new[]
                    {
                        DarlingManagedPostgres.CredentialFileName,
                        DarlingManagedPostgres.AdminCredentialFileName,
                        DarlingManagedPostgres.ViewerCredentialFileName,
                        DarlingManagedPostgres.McpCredentialFileName,
                        DarlingManagedPostgres.ServerCertFileName,
                        DarlingManagedPostgres.ServerKeyFileName,
                        DarlingManagedPostgres.ServerLogFileName,
                    })
                    {
                        paths.Add(Path.Combine(parent, name));
                    }
                }
            }
            catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
            {
                /* Unresolvable data directory: nothing nameable beyond the raw value above. */
            }
        }

        return new DarlingOwnedSet(
            paths.Where(p => !string.IsNullOrWhiteSpace(p)).Distinct(StringComparer.Ordinal).ToList(),
            envNames.Where(e => !string.IsNullOrWhiteSpace(e)).Distinct(StringComparer.OrdinalIgnoreCase).ToList());
    }
}

/// <summary>A file's identity: the volume (or device) and the file's ID on it.</summary>
internal readonly record struct FileId(ulong Volume, ulong Index);

/// <summary>Reads <see cref="FileId"/> from the operating system: volume serial plus file index on Windows,
/// <c>st_dev</c>/<c>st_ino</c> on Unix. Null ONLY for a path that does not exist. Every other failure throws
/// (<see cref="UnauthorizedAccessException"/> for a denial, <see cref="IOException"/> otherwise, and the platform
/// exceptions when the native call is unavailable), so a caller can tell "nothing there" from "could not look".</summary>
internal static class FileIdentity
{
    public static FileId? Of(string path) =>
        OperatingSystem.IsWindows() ? OfWindows(path) : OfUnix(path);

    private const uint FileFlagBackupSemantics = 0x02000000;
    private const uint FileShareAll = 7;
    private const uint OpenExisting = 3;
    private const int ErrorFileNotFound = 2;
    private const int ErrorPathNotFound = 3;
    private const int ErrorAccessDenied = 5;
    private const int ErrorInvalidName = 123; /* a name that cannot exist: absence, not a failure to look */
    private const int Enoent = 2;
    private const int Eacces = 13;
    private const int Enotdir = 20; /* a path component is a file, so nothing can be under it: absence */

    private static FileId? OfWindows(string path)
    {
        using var handle = CreateFileW(path, 0, FileShareAll, IntPtr.Zero, OpenExisting, FileFlagBackupSemantics, IntPtr.Zero);
        if (handle.IsInvalid)
        {
            var error = Marshal.GetLastPInvokeError();
            return error switch
            {
                ErrorFileNotFound or ErrorPathNotFound or ErrorInvalidName => null,
                ErrorAccessDenied => throw new UnauthorizedAccessException("Access to the path was denied (Win32 error 5)."),
                _ => throw new IOException("The path could not be examined (Win32 error " + error + ")."),
            };
        }

        if (!GetFileInformationByHandle(handle, out var info))
        {
            throw new IOException("The path could not be examined (Win32 error " + Marshal.GetLastPInvokeError() + ").");
        }

        return new FileId(info.VolumeSerialNumber, ((ulong)info.FileIndexHigh << 32) | info.FileIndexLow);
    }

    /* st_dev is at offset 0 and st_ino at offset 8 in struct stat on 64-bit Linux (x86-64 and arm64) and on macOS with
       64-bit inodes; st_dev is 8 bytes on Linux and 4 on macOS. A buffer larger than any of those structs is used. */
    private static FileId? OfUnix(string path)
    {
        var buffer = new byte[512];
        if (IntPtr.Size != 8)
        {
            throw new PlatformNotSupportedException("The stat layout is only known for 64-bit processes.");
        }

        /* DllNotFoundException / EntryPointNotFoundException propagate: the native call being unavailable is a failure
           to look, not absence. */
        var result = OperatingSystem.IsMacOS() && RuntimeInformation.ProcessArchitecture == Architecture.X64
            ? StatMacIntel(path, buffer)
            : Stat(path, buffer);
        if (result != 0)
        {
            var errno = Marshal.GetLastPInvokeError();
            return errno switch
            {
                Enoent or Enotdir => null,
                Eacces => throw new UnauthorizedAccessException("Access to the path was denied (errno 13)."),
                _ => throw new IOException("The path could not be examined (errno " + errno + ")."),
            };
        }

        var dev = OperatingSystem.IsMacOS() ? (ulong)BitConverter.ToUInt32(buffer, 0) : BitConverter.ToUInt64(buffer, 0);
        return new FileId(dev, BitConverter.ToUInt64(buffer, 8));
    }

    /* CA2101 wants CharSet.Unicode on a P/Invoke that takes a string, but libc's stat takes a UTF-8 path:
       CharSet.Unicode would marshal UTF-16 and the call would fail on every non-ASCII path. LPUTF8Str is the
       correct marshaling here, so the rule is suppressed for these two declarations only. */
#pragma warning disable CA2101 // libc takes UTF-8; LPUTF8Str is correct and CharSet.Unicode would be wrong
    [DllImport("libc", EntryPoint = "stat", SetLastError = true)]
    private static extern int Stat([MarshalAs(UnmanagedType.LPUTF8Str)] string path, byte[] buffer);

    [DllImport("libc", EntryPoint = "stat$INODE64", SetLastError = true)]
    private static extern int StatMacIntel([MarshalAs(UnmanagedType.LPUTF8Str)] string path, byte[] buffer);
#pragma warning restore CA2101

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern SafeFileHandle CreateFileW(
        string fileName, uint desiredAccess, uint shareMode, IntPtr securityAttributes,
        uint creationDisposition, uint flagsAndAttributes, IntPtr templateFile);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetFileInformationByHandle(SafeFileHandle file, out ByHandleInfo info);

    [StructLayout(LayoutKind.Sequential)]
    private struct ByHandleInfo
    {
        public uint FileAttributes;
        public uint CreationLow;
        public uint CreationHigh;
        public uint AccessLow;
        public uint AccessHigh;
        public uint WriteLow;
        public uint WriteHigh;
        public uint VolumeSerialNumber;
        public uint SizeHigh;
        public uint SizeLow;
        public uint NumberOfLinks;
        public uint FileIndexHigh;
        public uint FileIndexLow;
    }
}
