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
/// Pins the cross-platform secret indirection (#1804 — <see cref="DarlingSecretSource"/>): the reference
/// grammar (<c>env:</c>/<c>file:</c>, prefix-only, case-sensitive), the dereference behavior (env read,
/// file read trimmed — compose <c>secrets:</c> mounts end with a newline), and the failure contract (a
/// missing/empty target is a configuration ERROR naming both the setting and the target, never a silent
/// empty secret). Plus the plaintext-warning seam: a reference is NOT plaintext-in-config, so
/// <see cref="DarlingSecretSource.IsReference"/> is what the callers' warnings key on.
/// </summary>
public sealed class DarlingSecretSourceTests
{
    [Theory]
    [InlineData("env:MY_SECRET", true)]
    [InlineData("file:/run/secrets/pw", true)]
    [InlineData("hunter2", false)]
    [InlineData("ENV:MY_SECRET", false)]      /* case-sensitive by design — a password may legally start with ENV: */
    [InlineData("my env: note", false)]       /* prefix-only */
    [InlineData(null, false)]
    [InlineData("", false)]
    public void IsReference_PrefixOnly_CaseSensitive(string? value, bool expected)
        => Assert.Equal(expected, DarlingSecretSource.IsReference(value));

    [Fact]
    public void Resolve_Literal_PassesThroughUntouched()
        => Assert.Equal("hunter2", DarlingSecretSource.Resolve("hunter2", "test.setting"));

    [Fact]
    public void Resolve_Env_ReadsTheVariable_AndErrorsNameSettingAndTarget()
    {
        var name = "DARLING_TEST_SECRET_" + Guid.NewGuid().ToString("N");
        try
        {
            Environment.SetEnvironmentVariable(name, "from-the-environment");
            Assert.Equal("from-the-environment", DarlingSecretSource.Resolve($"env:{name}", "smtp.password"));

            Environment.SetEnvironmentVariable(name, null);
            var ex = Assert.Throws<InvalidOperationException>(
                () => DarlingSecretSource.Resolve($"env:{name}", "smtp.password"));
            Assert.Contains("smtp.password", ex.Message, StringComparison.Ordinal);
            Assert.Contains(name, ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    /// <summary>
    /// #3914: a variable that is set to whitespace resolves to nothing, exactly like an unset one — the file branch
    /// already trims to nothing and refuses. It used to come back as the whitespace itself, which a caller that
    /// reads blank as "not configured" (the web and MCP store logins) then treated as unset, and the surface
    /// landed on the owner login it was configured to avoid.
    /// </summary>
    [Theory]
    [InlineData(" ")]
    [InlineData("\t")]
    [InlineData("  \r\n ")]
    public void Resolve_Env_SetToWhitespace_IsAConfigurationError_NotABlankSecret(string blank)
    {
        var name = "DARLING_TEST_SECRET_" + Guid.NewGuid().ToString("N");
        try
        {
            Environment.SetEnvironmentVariable(name, blank);
            var ex = Assert.Throws<InvalidOperationException>(
                () => DarlingSecretSource.Resolve($"env:{name}", "postgres.webConnectionString"));
            Assert.Contains("postgres.webConnectionString", ex.Message, StringComparison.Ordinal);
            Assert.Contains(name, ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Fact]
    public void Resolve_File_ReadsTrimmed_TheComposeSecretsShape()
    {
        var path = Path.Combine(Path.GetTempPath(), "darling-secret-" + Guid.NewGuid().ToString("N"));
        try
        {
            /* Compose secrets mounts and hand-written credential files end with a newline; a trailing
               newline inside a password is never what the operator meant. */
            File.WriteAllText(path, "from-a-file\n");
            Assert.Equal("from-a-file", DarlingSecretSource.Resolve($"file:{path}", "servers['x'].password"));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Resolve_File_MissingOrEmpty_IsAConfigurationError_NamingSettingAndPath()
    {
        var missing = Path.Combine(Path.GetTempPath(), "darling-missing-" + Guid.NewGuid().ToString("N"));
        var ex = Assert.Throws<InvalidOperationException>(
            () => DarlingSecretSource.Resolve($"file:{missing}", "web.network.token"));
        Assert.Contains("web.network.token", ex.Message, StringComparison.Ordinal);
        Assert.Contains(missing, ex.Message, StringComparison.Ordinal);

        var empty = Path.Combine(Path.GetTempPath(), "darling-empty-" + Guid.NewGuid().ToString("N"));
        try
        {
            File.WriteAllText(empty, "  \n");
            var emptyEx = Assert.Throws<InvalidOperationException>(
                () => DarlingSecretSource.Resolve($"file:{empty}", "web.network.token"));
            Assert.Contains("empty", emptyEx.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            File.Delete(empty);
        }
    }

    [Theory]
    [InlineData("env:")]
    [InlineData("file: ")]
    public void Resolve_ReferenceNamingNoTarget_IsAConfigurationError(string value)
        => Assert.Throws<InvalidOperationException>(() => DarlingSecretSource.Resolve(value, "test.setting"));

    [Fact]
    public void TokenSlots_ResolveReferences_WithoutThePlaintextFlag()
    {
        var name = "DARLING_TEST_TOKEN_" + Guid.NewGuid().ToString("N");
        try
        {
            Environment.SetEnvironmentVariable(name, "sesame");

            var mcp = new McpNetworkConfig { Token = $"env:{name}" };
            Assert.Equal("sesame", mcp.ResolveToken(out var mcpPlaintext));
            Assert.False(mcpPlaintext, "an env: reference is not plaintext-in-config");

            var web = new WebNetworkConfig { Token = $"env:{name}" };
            Assert.Equal("sesame", web.ResolveToken(out var webPlaintext));
            Assert.False(webPlaintext, "an env: reference is not plaintext-in-config");

            var literal = new McpNetworkConfig { Token = "sesame" };
            Assert.Equal("sesame", literal.ResolveToken(out var literalPlaintext));
            Assert.True(literalPlaintext, "a literal token still warns");
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }
}
