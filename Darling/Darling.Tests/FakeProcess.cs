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
/// The stand-in live process a test starts on Windows under the image name it needs (postgres.exe,
/// pg_ctl.exe, sleeper.exe): the <c>fake-process</c> console app built from
/// <c>Darling/Darling.Tests.FakeProcess</c>, which the test project's <c>CopyFakeProcess</c> target puts in
/// <c>fake-process\</c> beside the tests.
///
/// <para>Given one whole number it stays alive that many seconds and exits 0; given anything else it exits
/// 1 at once. A PID and an image name are all a liveness check can see of a real process, so a copy under
/// another name is enough to stand in for one.</para>
///
/// <para>It is this repository's own build output rather than a copy of a Windows system tool, because
/// endpoint protection blocks a system tool that runs from a copy under another name.</para>
/// </summary>
internal static class FakeProcess
{
    private const string Name = "fake-process";

    /// <summary>The files the launcher loads from its own folder. The launcher looks for them under the
    /// name it was built with, whatever the launcher itself is called, so they keep their names.</summary>
    private static readonly string[] s_companions =
    [
        Name + ".dll",
        Name + ".runtimeconfig.json",
        Name + ".deps.json",
    ];

    private static string SourceDirectory => Path.Combine(AppContext.BaseDirectory, Name);

    /// <summary>
    /// Copies the stand-in into <paramref name="directory"/> as <paramref name="fileName"/>, with the files
    /// it loads beside it, and returns the full path of the copy. A second copy into the same folder reuses
    /// the files already beside the first, which a running first copy holds open.
    /// </summary>
    public static string CopyAs(string directory, string fileName)
    {
        var launcher = Path.Combine(SourceDirectory, Name + ".exe");
        if (!File.Exists(launcher))
        {
            throw new FileNotFoundException(
                "The fake-process stand-in is not beside the tests. Build Darling.Tests.csproj, whose " +
                "CopyFakeProcess target puts it there.", launcher);
        }

        var image = Path.Combine(directory, fileName);
        File.Copy(launcher, image, overwrite: true);
        foreach (var companion in s_companions)
        {
            var target = Path.Combine(directory, companion);
            if (!File.Exists(target)) { File.Copy(Path.Combine(SourceDirectory, companion), target); }
        }

        return image;
    }

    /// <summary>The argument string that keeps the stand-in alive for <paramref name="seconds"/> seconds
    /// before it exits 0 on its own.</summary>
    public static string RunFor(int seconds) => seconds.ToString(CultureInfo.InvariantCulture);
}
