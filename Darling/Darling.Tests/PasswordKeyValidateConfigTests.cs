/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8006: <c>--validate-config</c> run the usual way, with no <c>--config</c> and darling.json found through
/// <c>DARLING_CONFIG</c>, checks a <c>passwordKey</c> <c>file:</c> path against the real credentials directory, so the
/// pre-flight gate refuses the key the service start would refuse. In <c>darling-config-env</c> because it sets
/// <c>DARLING_CONFIG</c> for the process.
/// </summary>
[Collection("darling-config-env")]
public sealed class PasswordKeyValidateConfigTests
{
    [Fact]
    public async Task WithoutConfigOption_AKeyInsideTheCredentialsDirectory_IsAWarningNamingIt()
    {
        var dir = Directory.CreateTempSubdirectory("password-key-validate-").FullName;
        var previousConfig = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        byte[]? pkcs8 = null;
        try
        {
            using (var key = PasswordPrivateKey.Generate())
            {
                pkcs8 = key.ExportPkcs8();
            }

            var configPath = Path.Combine(dir, "darling.json");
            /* A closed local port: the store probe fails at once and the verb falls back to the (empty) file list. */
            var connectionString = $"Host=127.0.0.1;Port={StoreParseFailureFixtures.GetClosedPort()};Username=u;Password=p;Database=d";
            var keys = DarlingLogHashKeyFile.DirectoryFor(
                DarlingConfig.Parse("{\"postgres\":{\"connectionString\":" + JsonSerializer.Serialize(connectionString) + "}}"), configPath);
            Directory.CreateDirectory(keys);
            var inside = Path.Combine(keys, "password-key");
            File.WriteAllText(inside, Convert.ToBase64String(pkcs8));
            File.WriteAllText(
                configPath,
                "{\"postgres\":{\"connectionString\":" + JsonSerializer.Serialize(connectionString) + "},"
                + "\"passwordKey\":" + JsonSerializer.Serialize("file:" + inside) + "}");
            Environment.SetEnvironmentVariable("DARLING_CONFIG", configPath);

            using var output = new StringWriter();
            using var error = new StringWriter();
            await DarlingCliCommands.ValidateConfigAsync(null, output, error, TestContext.Current.CancellationToken);

            var line = Array.Find(
                output.ToString().Split('\n'),
                l => l.Contains(DarlingPasswordKeySource.SettingName + ":", StringComparison.Ordinal));
            Assert.NotNull(line);
            Assert.StartsWith("WARNING: passwordKey", line, StringComparison.Ordinal);
            Assert.Contains("credentials directory", line, StringComparison.Ordinal);
            Assert.DoesNotContain(Convert.ToBase64String(pkcs8)[..16], output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", previousConfig);
            if (pkcs8 is not null)
            {
                CryptographicOperations.ZeroMemory(pkcs8);
            }

            Directory.Delete(dir, recursive: true);
        }
    }
}
