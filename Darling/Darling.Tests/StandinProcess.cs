/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;

namespace Darling.Tests;

/// <summary>
/// #8847: puts the built <c>Darling.Tests.Standin</c> program under a file name and in a folder the test chooses, for a test
/// that needs a live process the product finds by image name and folder (a fake postmaster, a sleeper) or a pg_ctl that fails.
///
/// <para>Windows only. These tests used to copy <c>PING.EXE</c> under another name, which the Windows Defender rule "Block use
/// of copied or impersonated system tools" refuses to run ("Access is denied"). The stand-in is built from this repository
/// with the tests, so it is not a system tool. Run as <c>sleep &lt;seconds&gt;</c> it waits that long and exits 0. With any
/// other arguments it exits 1 at once, so a copy named <c>pg_ctl.exe</c> fails every command it is given.</para>
///
/// <para>The renamed executable is the stand-in's apphost, which finds its assembly by the name embedded in it, so the new
/// file name does not matter. The assembly, <c>runtimeconfig.json</c> and <c>deps.json</c> go beside it.</para>
/// </summary>
internal static class StandinProcess
{
    private const string StandinName = "Darling.Tests.Standin";

    /// <summary>Copies the stand-in into <paramref name="directory"/> as <paramref name="fileName"/> (the executable, with its
    /// extension) and returns that path. The companion files keep the stand-in's own names, and an existing one is left in place
    /// because a running copy has it loaded.</summary>
    public static string CopyTo(string directory, string fileName)
    {
        Directory.CreateDirectory(directory);
        var built = AppContext.BaseDirectory;
        var image = Path.Combine(directory, fileName);
        File.Copy(Path.Combine(built, StandinName + ".exe"), image, overwrite: true);

        foreach (var companion in new[] { ".dll", ".runtimeconfig.json", ".deps.json" })
        {
            var target = Path.Combine(directory, StandinName + companion);
            if (!File.Exists(target))
            {
                File.Copy(Path.Combine(built, StandinName + companion), target);
            }
        }

        return image;
    }

    /// <summary>The arguments that make a copy wait for roughly <paramref name="seconds"/> and then exit 0.</summary>
    public static string SleepArguments(int seconds) =>
        "sleep " + seconds.ToString(CultureInfo.InvariantCulture);
}
