/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Xunit;

namespace Darling.Tests;

/// <summary>A claim about real elapsed time (#5369): "this came back inside N ms". A wall-clock bound fails on a
/// loaded machine for reasons that have nothing to do with the code, so it must not be able to fail a normal run.
/// Without <c>DARLING_TIMING_TESTS=1</c> the measurement is only written to the test output; with it set the bound
/// is asserted. The deterministic claim (the verdict, or a fake clock's arithmetic) stays a plain assertion in the
/// test that calls this.</summary>
internal static class TimingClaim
{
    /// <summary>The opt-in variable. Set it to <c>1</c> on a quiet machine to enforce the wall-clock bounds.</summary>
    internal const string EnvironmentVariable = "DARLING_TIMING_TESTS";

    internal static bool Enforced =>
        string.Equals(Environment.GetEnvironmentVariable(EnvironmentVariable), "1", StringComparison.Ordinal);

    /// <summary>Asserts <paramref name="actualMs"/> is at most <paramref name="limitMs"/> when the opt-in is set;
    /// otherwise records the reading and passes.</summary>
    internal static void AtMost(double actualMs, double limitMs, string what)
    {
        var message = $"{what}: {actualMs:F0} ms against a {limitMs:F0} ms bound";
        if (Enforced)
        {
            Assert.True(actualMs <= limitMs, message);
        }
        else
        {
            TestContext.Current.SendDiagnosticMessage($"timing claim not enforced ({EnvironmentVariable} is not 1): {message}");
        }
    }
}
