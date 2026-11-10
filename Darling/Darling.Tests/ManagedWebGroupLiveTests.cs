/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

// #1776 own-store: an isolated database and a uniquely named metadata-only role.
public sealed class ManagedWebGroupLiveTests
{
    private const string Shared="shared-managed-test-token-0123456789";
    private const string Legacy="legacy-managed-test-token-0123456789";
    private const string Mcp="mcp-managed-test-token-0123456789";
    private const string Host="127.0.0.1";

    [Fact]
    public async Task StoredGroupsSignInAndEnforceCurrentScopeGenerationAndAvailability()
    {
        var baseConnection=Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnection),"Set DARLING_TEST_PG to test managed web groups.");
        var ct=TestContext.Current.CancellationToken;
        await using var scratch=await ScratchPostgres.CreateAsync(baseConnection!,ct);
        var settings=new NpgsqlConnectionStringBuilder(scratch.ConnectionString){SearchPath=PgSchemaGenerator.SearchPath};
        await using var postgres=NpgsqlDataSource.Create(settings.ConnectionString);
        var role="group_metadata_"+Guid.NewGuid().ToString("N")[..12];
        var password=DarlingManagedPostgres.GeneratePassword();
        await Exec("CREATE ROLE "+role+" LOGIN NOSUPERUSER PASSWORD '"+ScramSha256Verifier.Create(password)+"'");
        try
        {
            const string columns="group_id,name,scope_kind,enabled,token_generation,revision,activation_state,refusal_code,created_at,updated_at";
            await Exec("GRANT CONNECT ON DATABASE \""+scratch.DatabaseName+"\" TO "+role
                +"; GRANT USAGE ON SCHEMA config TO "+role
                +"; GRANT SELECT ("+columns+") ON config.web_access_groups TO "+role
                +"; GRANT SELECT ON config.web_access_group_tags, config.web_access_advanced TO "+role);
            await using var metadata=NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(settings.ConnectionString){Username=role,Password=password}.ConnectionString);
            await Assert.ThrowsAsync<PostgresException>(()=>new WebAccessGroupStore(metadata).ReadCredentialsAsync(ct));
            var store=new WebAccessGroupStore(postgres);
            using var key=PasswordPrivateKey.Generate();
            IPasswordKeyRing ring=DarlingPasswordKey.FromPrivateKey(key);
            NpgsqlDataSource? published=postgres;
            var logger=new CapturingTestLogger();
            var file=DarlingWebTokenSet.Resolve(new WebNetworkConfig
            {Tokens=[new WebTokenEntry{Name="legacy",Token=Legacy,Role="reader"}]},Shared,null,null);
            using var groups=new DarlingWebGroupTokens(()=>published,metadata,()=>ring,Shared,file,new[]{Mcp},logger);

            await using(var connection=await postgres.OpenConnectionAsync(ct))
            {
                foreach(var (serverId,name) in new[]{(41,"uat-server"),(42,"operations-server"),(43,"untagged-server")})
                {
                    await DarlingMcpTestData.RegisterServerAsync(connection,serverId,name,ct);
                    await DarlingMcpTestData.ExecAsync(connection,ct,
                        "INSERT INTO config.config_monitored_servers(server_id,name,host,is_enabled) VALUES($1,$2,$2,TRUE) ON CONFLICT DO NOTHING",serverId,name);
                    if(serverId==43) continue;
                    var at=DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-2));
                    await DarlingMcpTestData.ExecAsync(connection,ct,
                        "INSERT INTO cpu_utilization_stats(collection_id,collection_time,server_id,server_name,sample_time,sqlserver_cpu_utilization,other_process_cpu_utilization) VALUES($1,$2,$3,$4,$2,$5,0)",
                        CollectionIdGenerator.Next(),at,serverId,name,serverId==41?11:92);
                    await DarlingMcpTestData.ExecAsync(connection,ct,
                        "INSERT INTO wait_stats(collection_id,collection_time,server_id,server_name,wait_type,delta_wait_time_ms,delta_signal_wait_time_ms,delta_waiting_tasks) VALUES($1,$2,$3,$4,$5,1000,100,10)",
                        CollectionIdGenerator.Next(),at,serverId,name,serverId==41?"UAT_WAIT":"OPERATIONS_WAIT");
                }
            }
            var prod=await Tag("Production",null);
            var uat=await Tag("UAT",prod);
            var operations=await Tag("Operations",prod);
            await Exec("INSERT INTO config.server_tag_map(server_id,tag_id) VALUES(41,"+uat+"),(42,"+operations+")");
            await using var viewer = new PerformanceMonitor.Darling.Viewer.ViewerDataService(settings.ConnectionString);
            await viewer.DetectReadOnlyAsync(ct);
            var created = await viewer.CreateWebAccessGroupAsync("UAT", WebAccessGroupScope.Tags, new[] { uat },
                new PerformanceMonitor.Darling.Viewer.ViewerPasswordSealer(key.PublicKey), ct);
            var id = created.GroupId;
            var token = created.Plain;
            await groups.RefreshAsync(ct);
            Assert.Null(groups.FindMatch(token));
            Assert.Equal("pending", (await store.ReadAsync(id, ct))!.ActivationState);
            await store.SetAdvancedEnabledAsync(true, ct);
            await groups.RefreshAsync(ct);
            Assert.Equal("active",(await store.ReadAsync(id,ct))!.ActivationState);
            var activeGroup = (await viewer.GetWebAccessGroupAsync(id, ct))!;
            Assert.Equal("UAT", activeGroup.Name);

            var builder=WebApplication.CreateBuilder();
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton(postgres);
            DarlingWebHostService.ConfigureResponseCompression(builder.Services);
            await using var app=builder.Build();
            var host=new DarlingWebHostService(NullLogger<DarlingWebHostService>.Instance,
                new WebRuntimeState(),new CollectorRuntimeState(),new WebTlsCertificateState(),new BaselineCache());
            host.ConfigurePipeline(app,postgres,true,IPAddress.Loopback,IPNetwork.Parse("10.0.0.0/8"),Shared,null,
                webTokens:file,managedGroups:groups);
            await app.StartAsync(ct);
            var server=app.GetTestServer();
            var attempt=10;
            async Task<HttpContext> Login(string value) =>
                await DarlingWebHostGateLiveTests.PostSignIn(server,Host,IPAddress.Parse("10.2.3."+(attempt++)),value);
            async Task<string> Cookie(string value)
            {
                var response=await Login(value);
                Assert.Equal(302,response.Response.StatusCode);
                var header=Assert.Single(response.Response.Headers.SetCookie)!;
                Assert.Contains("httponly",header,StringComparison.OrdinalIgnoreCase);
                Assert.Contains("samesite=strict",header,StringComparison.OrdinalIgnoreCase);
                return header[..header.IndexOf(';')];
            }
            async Task<(int Status,string Body)> Read(string path,string? cookie)
            {
                var response=await server.SendAsync(context=>
                {
                    var query=path.IndexOf('?');
                    context.Request.Path=query<0?path:path[..query];
                    context.Request.QueryString=query<0?QueryString.Empty:new QueryString(path[query..]);
                    context.Request.Headers.Host=Host;
                    context.Connection.RemoteIpAddress=IPAddress.Parse("10.2.3.4");
                    if(cookie is not null) context.Request.Headers.Cookie=cookie;
                });
                return(response.Response.StatusCode,await new StreamReader(response.Response.Body).ReadToEndAsync(ct));
            }
            async Task Fleet(string cookie,int count)
            {
                var response=await Read("/api/fleet",cookie);
                Assert.Equal(200,response.Status);
                using var json=JsonDocument.Parse(response.Body);
                Assert.Equal(count,json.RootElement.GetProperty("total_servers").GetInt32());
            }
            var cookie=await Cookie(token);
            await store.SetAdvancedEnabledAsync(false, ct);
            Assert.Equal(401, (await Read("/api/session", cookie)).Status);
            Assert.Equal(200, (await Login(token)).Response.StatusCode);
            Assert.Equal(302, (await Login(Shared)).Response.StatusCode);
            Assert.Equal(302, (await Login(Legacy)).Response.StatusCode);
            await store.SetAdvancedEnabledAsync(true, ct);
            await groups.RefreshAsync(ct);
            Assert.Equal(401, (await Read("/api/session", cookie)).Status);
            cookie = await Cookie(token);
            await Fleet(cookie,1);
            var scopedFleet = await Read("/api/fleet", cookie);
            Assert.Contains("uat-server", scopedFleet.Body, StringComparison.Ordinal);
            Assert.DoesNotContain("operations-server", scopedFleet.Body, StringComparison.Ordinal);
            Assert.DoesNotContain("untagged-server", scopedFleet.Body, StringComparison.Ordinal);
            var session=await Read("/api/session",cookie);
            Assert.Contains("\"can_edit\":false",session.Body,StringComparison.Ordinal);
            Assert.Contains("\"scoped\":true",session.Body,StringComparison.Ordinal);
            Assert.Equal(200,(await Read("/api/read/get_cpu_utilization?server=uat-server",cookie)).Status);
            Assert.Equal(403,(await Read("/api/read/get_cpu_utilization?server=operations-server",cookie)).Status);
            Assert.Equal(403,(await Read("/api/admin/servers",cookie)).Status);
            var urlToken=await Read("/?token="+token,null);
            Assert.DoesNotContain("\"can_edit\"",urlToken.Body,StringComparison.Ordinal);
            var crossOrigin=await DarlingWebHostGateLiveTests.PostSignIn(server,Host,IPAddress.Parse("10.2.3.200"),token,
                configure:context=>context.Request.Headers.Origin="https://outside.invalid");
            Assert.Equal(403,crossOrigin.Response.StatusCode);

            // All servers includes untagged registrations, but grants no management capability.
            await Update(WebAccessGroupScope.AllServers,Array.Empty<int>(),true);
            await Fleet(cookie,3);
            Assert.Equal(403,(await Read("/api/admin/servers",cookie)).Status);
            Assert.Contains("untagged-server",(await Read("/api/read/list_servers",cookie)).Body,StringComparison.Ordinal);
            using(var fleet=JsonDocument.Parse((await Read("/api/read/get_fleet_overview",cookie)).Body))
                Assert.Equal(3,fleet.RootElement.GetProperty("total_servers").GetInt32());
            const string panel="""{"hours":4,"panel":{"source":"wait_stats","measure":"wait_time_ms","aggregate":"sum","topN":20,"groupBy":["server","wait_type"],"viz":"bar"}}""";
            using(var body=new MemoryStream(Encoding.UTF8.GetBytes(panel)))
            {
                var response=await server.SendAsync(context=>
                {
                    context.Request.Method="POST"; context.Request.Path="/api/compose/run";
                    context.Request.Headers.Host=Host;context.Request.Headers.Cookie=cookie;
                    context.Connection.RemoteIpAddress=IPAddress.Parse("10.2.3.4");
                    context.Request.ContentType="application/json";context.Request.ContentLength=body.Length;context.Request.Body=body;
                });
                Assert.Equal(200,response.Response.StatusCode);
                var composed=await new StreamReader(response.Response.Body).ReadToEndAsync(ct);
                Assert.Contains("UAT_WAIT",composed,StringComparison.Ordinal);
                Assert.Contains("OPERATIONS_WAIT",composed,StringComparison.Ordinal);
            }

            // No credential reload or new cookie: current DB grants decide the next request.
            await Update(WebAccessGroupScope.Tags,new[]{operations},true);
            Assert.Equal(403,(await Read("/api/read/get_cpu_utilization?server=uat-server",cookie)).Status);
            Assert.Equal(200,(await Read("/api/read/get_cpu_utilization?server=operations-server",cookie)).Status);
            await Exec("DELETE FROM config.server_tags WHERE id="+operations);
            await Fleet(cookie,0);
            Assert.Equal(403,(await Read("/api/read/get_cpu_utilization?server=operations-server",cookie)).Status);
            await Update(WebAccessGroupScope.Tags,new[]{uat},false);
            Assert.Equal(401,(await Read("/api/session",cookie)).Status);
            Assert.Equal(0,(await Login(token)).Response.Headers.SetCookie.Count);
            await Update(WebAccessGroupScope.Tags,new[]{uat},true);
            await Fleet(cookie,1);

            // Stale candidate snapshots cannot issue sessions for an old generation.
            const string rotated="rotated-managed-token-0123456789";
            var next=Guid.NewGuid();
            var current=(await store.ReadAsync(id,ct))!;
            await store.RotateAsync(id,current.Revision,next,Seal(id,next,rotated),"admin",ct);
            Assert.Equal(401,(await Read("/api/session",cookie)).Status);
            Assert.Equal(0,(await Login(token)).Response.Headers.SetCookie.Count);
            await groups.RefreshAsync(ct);
            var rotatedCookie=await Cookie(rotated);
            await Fleet(rotatedCookie,1);
            current=(await store.ReadAsync(id,ct))!;
            await store.DeleteAsync(id,current.Revision,"admin",ct);
            Assert.Equal(401,(await Read("/api/session",rotatedCookie)).Status);
            Assert.Equal(0,(await Login(rotated)).Response.Headers.SetCookie.Count);
            var replacement=Guid.NewGuid();
            var replacementGeneration=Guid.NewGuid();
            const string replacementToken="replacement-managed-token-0123456789";
            await store.CreateAsync(replacement,"UAT",WebAccessGroupScope.Tags,new[]{uat},replacementGeneration,
                Seal(replacement,replacementGeneration,replacementToken),true,"admin",ct);
            await groups.RefreshAsync(ct);
            var replacementCookie=await Cookie(replacementToken);
            Assert.Equal(401,(await Read("/api/session",rotatedCookie)).Status);

            // Metadata failure refuses rather than carrying cached authorization forward.
            await Exec("REVOKE SELECT(group_id) ON config.web_access_groups FROM "+role);
            Assert.Equal(503,(await Read("/api/fleet",replacementCookie)).Status);
            await Exec("GRANT SELECT(group_id) ON config.web_access_groups TO "+role);
            await Fleet(replacementCookie,1);
            published=null;
            await groups.RefreshAsync(ct);
            Assert.Equal(503,(await Read("/api/session",replacementCookie)).Status);
            var sharedCookie=await Cookie(Shared);
            Assert.Contains("\"can_edit\":true",(await Read("/api/session",sharedCookie)).Body,StringComparison.Ordinal);
            var legacyCookie=await Cookie(Legacy);
            Assert.Equal(200,(await Read("/api/session",legacyCookie)).Status);
            published=postgres;
            await groups.RefreshAsync(ct);
            ring=DarlingPasswordKey.Refusing("test key temporarily unavailable");
            Assert.Equal(503,(await Read("/api/session",replacementCookie)).Status);
            ring=DarlingPasswordKey.FromPrivateKey(key);
            await Fleet(replacementCookie,1);

            // Rejected groups never disable the existing file/shared/MCP credentials.
            var bad=await Add("SharedCollision",Shared);
            var named=await Add("legacy","different-managed-token-0123456789");
            var mcp=await Add("McpCollision",Mcp);
            var duplicateA=await Add("DuplicateA","duplicate-managed-token-0123456789");
            var duplicateB=await Add("DuplicateB","duplicate-managed-token-0123456789");
            var shortToken=await Add("Short","short");
            var wrongId=Guid.NewGuid();var wrongGen=Guid.NewGuid();
            await store.CreateAsync(wrongId,"WrongBinding",WebAccessGroupScope.AllServers,Array.Empty<int>(),wrongGen,
                Seal(Guid.NewGuid(),wrongGen,"wrong-binding-managed-token-0123456789"),true,"admin",ct);
            await groups.RefreshAsync(ct);
            foreach(var rejected in new[]{bad,named,mcp,duplicateA,duplicateB,shortToken,wrongId})
                Assert.Equal("refused",(await store.ReadAsync(rejected,ct))!.ActivationState);
            Assert.Equal(302,(await Login(Shared)).Response.StatusCode);
            Assert.Equal(302,(await Login(Legacy)).Response.StatusCode);
            foreach(var secret in new[]{Shared,Legacy,Mcp,token,rotated,replacementToken,"duplicate-managed-token-0123456789"})
                Assert.DoesNotContain(secret,logger.Joined,StringComparison.Ordinal);

            async Task Update(WebAccessGroupScope scope,int[] tags,bool enabled)
            {
                var live=(await store.ReadAsync(id,ct))!;
                await store.UpdateAsync(id,live.Revision,scope,tags,enabled,"admin",ct);
            }
            string Seal(Guid group,Guid gen,string value) => PasswordSeal.Seal(value,key.PublicKey,PasswordBinding.ForWebAccessToken(group,gen));
            async Task<Guid> Add(string name,string value)
            {
                var group=Guid.NewGuid();var gen=Guid.NewGuid();
                await store.CreateAsync(group,name,WebAccessGroupScope.AllServers,Array.Empty<int>(),gen,Seal(group,gen,value),true,"admin",ct);
                return group;
            }
        }
        finally
        {
            NpgsqlConnection.ClearAllPools();
            await Exec("DROP OWNED BY "+role+"; DROP ROLE "+role);
        }

        async Task Exec(string sql)
        {
            await using var command=postgres.CreateCommand(sql);
            await command.ExecuteNonQueryAsync(ct);
        }
        async Task<int> Tag(string name,int? parent)
        {
            await using var command=postgres.CreateCommand("INSERT INTO config.server_tags(name,parent_id) VALUES($1,$2) RETURNING id");
            command.Parameters.AddWithValue(name);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Integer,(object?)parent??DBNull.Value);
            return (int)(await command.ExecuteScalarAsync(ct))!;
        }
    }
}
