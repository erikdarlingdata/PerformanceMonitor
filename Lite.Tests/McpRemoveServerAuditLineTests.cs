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
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9640: the line <c>remove_server</c> writes to Lite's log names the tool and the removed server's storage name, and never its login.
///
/// <para>Collection <c>app-logger-statics</c>: the assertion drains the process-wide log buffer, which the other tests that do the same share.</para>
/// </summary>
[Collection("app-logger-statics")]
[InMemoryCredentials]
public sealed class McpRemoveServerAuditLineTests : IDisposable
{
    private const string Username = "svc_audit_login";
    private const string Password = "not-a-real-secret-value";
    private readonly string _root = Path.Combine(Path.GetTempPath(), "McpRemoveServerAuditLineTests_" + Guid.NewGuid().ToString("N"));

    public McpRemoveServerAuditLineTests() => Directory.CreateDirectory(_root);

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch (IOException) { /* a temp folder left behind is harmless */ }
    }

    [Fact]
    public async Task RemoveServer_LogsTheToolAndTheStorageName_ButNeitherTheLoginNorThePassword()
    {
        var manager = new ServerManager(_root, null, Path.Combine(_root, "user", ServerHostBindings.FileName), new CredentialService(new InMemoryCredentialStore()));
        var server = new ServerConnection { ServerName = "audit-host01", DisplayName = "Audit", AuthenticationType = AuthenticationTypes.SqlServer };
        manager.AddServer(server, Username, Password);
        var hooks = new McpServerAdminHooks { RemoveServer = s => { manager.DeleteServer(s.Id); return Task.FromResult(true); } };

        AppLogger.DrainBufferedLines();
        await McpServerRegistryTools.RemoveServerCore(manager, hooks, "Audit");

        var lines = AppLogger.DrainBufferedLines().Where(l => l.Contains("remove_server", StringComparison.Ordinal)).ToList();
        var line = Assert.Single(lines);
        Assert.Contains("audit-host01", line, StringComparison.Ordinal);
        Assert.DoesNotContain(Username, line, StringComparison.Ordinal);
        Assert.DoesNotContain(Password, line, StringComparison.Ordinal);
    }
}
