/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Darling.Service;

namespace Darling.Tests;

/// <summary>
/// Fixed role secrets for tests that build the provisioning batch (#3910). The batch takes SCRAM-SHA-256
/// verifiers and refuses a password, so these are verifiers of the three passwords the tests used to pass,
/// each under its own fixed salt, and so byte-stable across runs.
/// </summary>
internal static class ProvisioningTestSecrets
{
    public const string AdminPassword = "AdminPassword01";

    public const string ViewerPassword = "ViewerPassword02";

    public const string McpPassword = "McpPassword03";

    public static readonly string Admin = Verifier(AdminPassword, seed: 1);

    public static readonly string Viewer = Verifier(ViewerPassword, seed: 2);

    public static readonly string Mcp = Verifier(McpPassword, seed: 3);

    private static string Verifier(string password, int seed) =>
        ScramSha256Verifier.Compute(
            password,
            Enumerable.Range(0, ScramSha256Verifier.SaltLength).Select(i => (byte)((seed * 31) + i)).ToArray(),
            ScramSha256Verifier.DefaultIterations);
}
