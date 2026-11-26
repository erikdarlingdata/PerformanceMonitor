/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Darling.Tests;
using Xunit;

[assembly: AssemblyFixture(typeof(ShareNodeLoader))]

namespace Darling.Tests;

/// <summary>
/// Lets every Node harness import the shipped web modules when the suite runs from a network share (#9789). Node reads the
/// package.json files above a <c>.js</c> file to learn whether it is a module, and beside a share that read walks up to the server
/// and fails ("Cannot read package config"), so each harness that imports <c>wwwroot/js</c> from a share died on its first import.
/// The repository root is a share path when the suite is started from one, and then <c>NODE_OPTIONS</c> gains
/// <c>--import share-node-preload.mjs</c> (it registers <c>share-node-hooks.mjs</c>, which loads a <c>.js</c> or <c>.mjs</c> file
/// on a share as a module without the package.json read). This assembly fixture runs once, before the first test, so every Node
/// process any test starts inherits the variable. A local checkout leaves it alone. The repository root is resolved here, not when
/// the assembly loads, because it is found through the working folder, which the runner sets after load.
/// </summary>
/* #1776 own-store: this class never connects to any store, shared or its own. It sets one environment variable, once, before any
   test starts Node. */
public sealed class ShareNodeLoader
{
    public ShareNodeLoader()
    {
        if (!RepoFile.Root.StartsWith(@"\\", StringComparison.Ordinal))
        {
            return;
        }

        var preload = new Uri(RepoFile.PathTo("Darling", "Darling.Tests", "share-node-preload.mjs")).AbsoluteUri;
        var existing = Environment.GetEnvironmentVariable("NODE_OPTIONS");
        var option = "--import=" + preload;

        Environment.SetEnvironmentVariable("NODE_OPTIONS", string.IsNullOrEmpty(existing) ? option : existing + " " + option);
    }
}
