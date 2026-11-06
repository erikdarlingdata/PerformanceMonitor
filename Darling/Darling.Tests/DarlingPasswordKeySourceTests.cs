/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the <c>passwordKey</c> setting (#8006 — <see cref="DarlingPasswordKeySource"/>): it accepts only a
/// <c>file:</c> or <c>env:</c> reference, a literal key is a validation problem whose sentence never repeats the
/// value, the reference is kept as written at load (the key is read later, never into the config object), and a
/// <c>file:</c> reference joins the owned set so no configured path can name the key file.
/// </summary>
public sealed class DarlingPasswordKeySourceTests
{
    [Fact]
    public void ALiteralPasswordKey_IsAValidationProblem_ThatNeverRepeatsTheValue()
    {
        var config = DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"},\"passwordKey\":\"MIIEvAIBADANBgkqhkiG9w0BAQEFAASC\"}");
        var problems = config.Validate();
        Assert.Contains(DarlingPasswordKeySource.LiteralRefusal, problems);
        Assert.DoesNotContain(problems, p => p.Contains("MIIEvAIB", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("file:/run/secrets/password_key")]
    [InlineData("env:DARLING_PASSWORD_KEY")]
    public void AReferencePasswordKey_IsNotAValidationProblem_AndIsNotResolvedAtLoad(string reference)
    {
        var config = DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"},\"passwordKey\":\"" + reference + "\"}");
        Assert.DoesNotContain(DarlingPasswordKeySource.LiteralRefusal, config.Validate());
        Assert.Equal(reference, config.PasswordKey);   // never replaced by the key text
    }

    [Fact]
    public void ThePasswordKeyReference_IsInTheOwnedSet()
    {
        var dir = Directory.CreateTempSubdirectory().FullName;
        var config = DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"},\"passwordKey\":\"file:" + Path.Combine(dir, "pk").Replace("\\", "\\\\") + "\"}");
        var set = DarlingOwnedSecrets.Compute(config, Path.Combine(dir, "darling.json"));
        Assert.Contains(Path.Combine(dir, "pk"), set.Paths);
    }
}
