/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Darling.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Every Darling.Tests file that Lite.Tests.csproj compiles into itself must be in the filter that sends a diff to
/// the whole Lite suite, or a pull request that edits only that file skips the Lite classes that run it.
/// <para>#5833 removed this class's census of the <c>Reads=Darling</c> trait (a trait roster); this check of the
/// linked-file filter stays because a gap in it skips a suite.</para>
/// </summary>
[Trait("Reads", "Darling")]
[Trait("Stage", "Guard")]
public sealed class DarlingReadsTraitGuardTests
{
    /// <summary>
    /// Every Darling.Tests file that Lite.Tests.csproj compiles into itself is named in
    /// <c>.github/lite-paths-filter.yml</c>'s <c>lite_linked_shard</c> filter, which sends a diff that edits one to the WHOLE Lite suite. Such a file
    /// is code the Lite classes run, so the narrow Reads=Darling selection would miss what an edit breaks.
    /// </summary>
    [Fact]
    public void EveryDarlingFileLiteTestsCompiles_IsInTheFullSuiteFilter()
    {
        var root = ParitySource.RepoRoot();
        var project = File.ReadAllText(Path.Combine(root, "Lite.Tests", "Lite.Tests.csproj"));
        var linked = Regex.Matches(project, @"<Compile\s+Include=""\.\.\\(?<path>Darling\\[^""]+)""")
            .Select(m => m.Groups["path"].Value.Replace('\\', '/'))
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToList();
        Assert.NotEmpty(linked);

        /* #5669: the set lives in .github/lite-paths-filter.yml, the one file the shard legs, lite-scope and
           ci-select.py read. */
        var named = CrossAppGuardCiGateTests.LiteFilterFilePatterns("lite_linked_shard")
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToList();
        Assert.True(named.Count > 0, ".github/lite-paths-filter.yml's 'lite_linked_shard' path filter is gone");

        Assert.Equal(linked, named);
    }
}
