/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6365: a SQL Server in a database mirroring session tells SqlClient its partner's name (ENVCHANGE 13), and SqlClient
/// connects to that name when the principal stops answering. The name gets no <c>tcp:</c>, so it can fall back to a named
/// pipe over SMB with the process's Windows credentials. <see cref="ServerHostSetting"/> turns that off with an AppContext
/// switch that SqlClient reads once per process and caches, so a test in the shared test process cannot tell whether it was
/// set early enough. The probe therefore runs in a child process (this test executable, started with one filtered test),
/// against a fake TDS server on loopback and a fake "partner" listener on loopback that counts connections.
/// </summary>
public class ServerHostFailoverPartnerTests
{
    private const string ChildEnv = "DARLING_6365_PROBE";
    private const string ChildTestName = "FailoverPartnerProbeChildBody";

    [Fact]
    public async Task AServerReportedMirroringPartner_IsNeverConnectedTo()
    {
        var (exit, output) = await RunChildAsync("guard");
        Assert.True(exit == 0, "the probe child failed:\n" + output);
        Assert.Contains("PARTNER-ACCEPTS remembered=0 sameLogin=0", output);
    }

    /// <summary>With the switch turned back off the same probe does reach the partner, so the test above is not vacuous.</summary>
    [Fact]
    public async Task WithTheSwitchOff_TheSameProbeReachesThePartner()
    {
        var (exit, output) = await RunChildAsync("control");
        Assert.True(exit == 0, "the control child failed:\n" + output);
        Assert.Matches(@"PARTNER-ACCEPTS remembered=[1-9]", output);
    }

    private static async Task<(int Exit, string Output)> RunChildAsync(string mode)
    {
        var host = Environment.ProcessPath!;
        var psi = new ProcessStartInfo
        {
            FileName = host,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        if (string.Equals(Path.GetFileNameWithoutExtension(host), "dotnet", StringComparison.OrdinalIgnoreCase))
        {
            psi.ArgumentList.Add(typeof(ServerHostFailoverPartnerTests).Assembly.Location);
        }
        psi.ArgumentList.Add("-method");
        psi.ArgumentList.Add("*" + ChildTestName + "*");
        psi.Environment[ChildEnv] = mode;

        using var child = Process.Start(psi)!;
        var stdout = child.StandardOutput.ReadToEndAsync();
        var stderr = child.StandardError.ReadToEndAsync();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        try
        {
            await child.WaitForExitAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            child.Kill(true);
            throw new TimeoutException("the probe child did not finish in 90 seconds");
        }
        return (child.ExitCode, await stdout + await stderr);
    }

    /// <summary>The body the child runs; a normal test run skips it. It never runs in the shared test process.</summary>
    [Fact]
    public async Task FailoverPartnerProbeChildBody()
    {
        var mode = Environment.GetEnvironmentVariable(ChildEnv);
        Assert.SkipWhen(string.IsNullOrEmpty(mode), "runs only as the child of the failover-partner probe");

        // The first TcpDataSource call is what sets the switch in a real process; nothing else in this child touched SqlClient.
        _ = ServerHostSetting.TcpDataSource("sql01");
        if (mode == "control")
        {
            AppContext.SetSwitch(ServerHostSetting.IgnoreServerProvidedFailoverPartnerSwitch, false);
        }

        using var partnerA = new CountingListener();
        using var partnerB = new CountingListener();

        // (a) a partner remembered from an earlier login, then the principal is down on the next connect.
        int downPort;
        using (var principalA = new FakePrincipal($"127.0.0.1,{partnerA.Port}", dropRightAfterLogin: false))
        {
            downPort = principalA.Port;
            await TryOpenAsync(downPort);
        }
        await TryOpenAsync(downPort);

        // (b) the principal reports its partner and then drops the connection within the same login.
        using (var principalB = new FakePrincipal($"127.0.0.1,{partnerB.Port}", dropRightAfterLogin: true))
        {
            await TryOpenAsync(principalB.Port);
        }

        Console.WriteLine($"PARTNER-ACCEPTS remembered={partnerA.Accepts} sameLogin={partnerB.Accepts}");
        if (mode == "guard")
        {
            Assert.Equal(0, partnerA.Accepts);
            Assert.Equal(0, partnerB.Accepts);
        }
    }

    private static async Task TryOpenAsync(int port)
    {
        try
        {
            using var cn = new SqlConnection(
                $"Data Source=tcp:127.0.0.1,{port};User Id=x;Password=y;Encrypt=false;Connect Timeout=8;Pooling=false");
            await cn.OpenAsync();
        }
        catch (Exception e) when (e is SqlException or InvalidOperationException or NullReferenceException)
        {
            // expected: the second connect (principal down) and the dropped login fail; what matters is who was dialled.
        }
    }

    private sealed class CountingListener : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private int _accepts;
        public int Port { get; }
        public int Accepts => Volatile.Read(ref _accepts);

        public CountingListener()
        {
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _ = Task.Run(async () =>
            {
                while (true)
                {
                    TcpClient c;
                    try { c = await _listener.AcceptTcpClientAsync(); }
                    catch (Exception e) when (e is SocketException or ObjectDisposedException or InvalidOperationException) { return; }
                    Interlocked.Increment(ref _accepts);
                    c.Close();
                }
            });
        }

        public void Dispose() => _listener.Stop();
    }

    /// <summary>A minimal TDS server: answers prelogin and login, reporting <c>partner</c> in ENVCHANGE 13.</summary>
    private sealed class FakePrincipal : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        public int Port { get; }

        public FakePrincipal(string partner, bool dropRightAfterLogin)
        {
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _ = Task.Run(async () =>
            {
                while (true)
                {
                    TcpClient c;
                    try { c = await _listener.AcceptTcpClientAsync(); }
                    catch (Exception e) when (e is SocketException or ObjectDisposedException or InvalidOperationException) { return; }
                    _ = Task.Run(() => Serve(c, partner, dropRightAfterLogin));
                }
            });
        }

        public void Dispose() => _listener.Stop();

        private static async Task Serve(TcpClient c, string partner, bool drop)
        {
            try
            {
                using (c)
                {
                    var ns = c.GetStream();
                    if (await ReadMessage(ns) == null) { return; }
                    // prelogin response: VERSION and ENCRYPTION = not supported
                    Send(ns, new byte[] { 0, 0, 11, 0, 6, 1, 0, 17, 0, 1, 0xFF, 0, 0, 0, 0, 0, 0, 2 });
                    if (await ReadMessage(ns) == null) { return; }
                    var resp = new List<byte>();
                    resp.AddRange(Env(1, "master"));
                    resp.AddRange(Env(13, partner));
                    if (drop)
                    {
                        Send(ns, resp.ToArray());
                        await Task.Delay(200);
                        return;
                    }
                    var ack = new List<byte> { 1, 0x74, 0, 0, 4, 7 };
                    ack.AddRange(Encoding.Unicode.GetBytes("FakeSQL"));
                    ack.AddRange(new byte[] { 16, 0, 0, 0 });
                    resp.Add(0xAD); resp.Add((byte)(ack.Count & 255)); resp.Add((byte)(ack.Count >> 8)); resp.AddRange(ack);
                    resp.AddRange(new byte[] { 0xFD, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
                    Send(ns, resp.ToArray());
                    await Task.Delay(500);
                }
            }
            catch (Exception e) when (e is IOException or SocketException or ObjectDisposedException)
            {
                // the client went away
            }
        }

        private static async Task<byte[]?> ReadMessage(NetworkStream ns)
        {
            var all = new List<byte>();
            while (true)
            {
                var h = new byte[8];
                if (!await ReadFully(ns, h)) { return null; }
                var len = (h[2] << 8) | h[3];
                var body = new byte[len - 8];
                if (!await ReadFully(ns, body)) { return null; }
                all.AddRange(body);
                if ((h[1] & 1) != 0) { return all.ToArray(); }
            }
        }

        private static async Task<bool> ReadFully(NetworkStream ns, byte[] buf)
        {
            var got = 0;
            while (got < buf.Length)
            {
                var n = await ns.ReadAsync(buf.AsMemory(got));
                if (n <= 0) { return false; }
                got += n;
            }
            return true;
        }

        private static void Send(NetworkStream ns, byte[] payload)
        {
            var p = new byte[payload.Length + 8];
            p[0] = 4; p[1] = 1; p[2] = (byte)(p.Length >> 8); p[3] = (byte)(p.Length & 255); p[6] = 1;
            payload.CopyTo(p, 8);
            ns.Write(p, 0, p.Length);
        }

        private static byte[] Env(byte type, string newValue)
        {
            var body = new List<byte> { type, (byte)newValue.Length };
            body.AddRange(Encoding.Unicode.GetBytes(newValue));
            body.Add(0);
            var r = new List<byte> { 0xE3, (byte)(body.Count & 255), (byte)(body.Count >> 8) };
            r.AddRange(body);
            return r.ToArray();
        }
    }
}
