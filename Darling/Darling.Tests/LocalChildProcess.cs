/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Diagnostics;
using System.IO;

namespace Darling.Tests;

/// <summary>The working folder every child process a test starts is given (#9789). A test host run from a
/// network share (a <c>\\server\share</c> working folder) hands that folder down, and <c>cmd.exe</c> refuses a
/// UNC working folder ("UNC paths are not supported"), so the child, or a <c>cmd.exe</c> the child starts in turn,
/// fails before it does what the test asks. The temp folder is local on every machine the suite runs on, and no
/// test reads a relative path from the folder it starts in. The modules a Node harness imports from a share are
/// <see cref="ShareNodeLoader"/>'s concern, not the working folder's.</summary>
internal static class LocalChildProcess
{
    /// <summary>The local folder a child process starts in.</summary>
    internal static string Folder => Path.GetTempPath();

    /// <summary>Starts <paramref name="startInfo"/>'s process in <see cref="Folder"/> and returns it for chaining.</summary>
    internal static ProcessStartInfo InLocalFolder(this ProcessStartInfo startInfo)
    {
        startInfo.WorkingDirectory = Folder;
        return startInfo;
    }
}
