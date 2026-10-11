/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Xunit;

namespace Darling.Tests;

/// <summary>Planted on purpose for #5669 to measure the fast failure on a pull request. Reverted in the next commit.</summary>
[Trait("Stage", "Guard")]
public sealed class PlantedGuardFailure5669Tests
{
    [Fact]
    public void ThisGuardFailureIsPlantedOnPurposeFor5669()
    {
        Assert.Fail("Planted on purpose for #5669: this guard failure measures the fast failure of the jobs that start beside Guard tests. It is reverted in the next commit.");
    }
}
