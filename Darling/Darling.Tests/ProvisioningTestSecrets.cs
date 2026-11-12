/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Security.Cryptography;
using PerformanceMonitor.Darling.Service;

namespace Darling.Tests;

/// <summary>
/// Role secrets for tests that build the provisioning batch (#3910). The batch takes SCRAM-SHA-256
/// verifiers and refuses a password, so these are random passwords made once per test process, each with
/// the verifier computed from it under its own random salt. A live test that creates the admin, viewer and
/// mcp login roles on a cluster therefore never leaves roles whose password is written in the repository;
/// a run that is killed before its cleanup leaves logins nobody can guess. Nothing in the tests relies on a
/// fixed value: they read the password and the verifier from here and never the other way round.
/// </summary>
internal static class ProvisioningTestSecrets
{
    public static readonly string AdminPassword = DarlingManagedPostgres.GeneratePassword();

    public static readonly string ViewerPassword = DarlingManagedPostgres.GeneratePassword();

    public static readonly string McpPassword = DarlingManagedPostgres.GeneratePassword();

    public static readonly string Admin = Verifier(AdminPassword);

    public static readonly string Viewer = Verifier(ViewerPassword);

    public static readonly string Mcp = Verifier(McpPassword);

    public static readonly string WebPassword = DarlingManagedPostgres.GeneratePassword();

    /// <summary>A fixed web login verifier, for a test that builds the batch twice and compares the texts: the
    /// batch makes a random one when the caller passes none.</summary>
    public static readonly string Web = Verifier(WebPassword);

    private static string Verifier(string password) =>
        ScramSha256Verifier.Compute(
            password,
            RandomNumberGenerator.GetBytes(ScramSha256Verifier.SaltLength),
            ScramSha256Verifier.DefaultIterations);
}
