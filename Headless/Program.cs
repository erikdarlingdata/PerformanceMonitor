using PerformanceMonitor.Headless.Models;
using PerformanceMonitor.Headless.Services;
using PerformanceMonitor.Headless.Storage;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseWindowsService();
builder.Services.Configure<MonitorOptions>(builder.Configuration.GetSection("Monitor"));
builder.Services.AddSingleton<HeadlessStore>();
builder.Services.AddHostedService<SqlEstateCollectorService>();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet("/api/health", () => Results.Ok(new { status = "ok", generated_at = DateTime.UtcNow }));

app.MapGet("/api/storage", (HeadlessStore store) => Results.Ok(new
{
    duckdb = store.DatabasePath,
    parquet = store.ArchiveDirectory
}));

app.MapGet("/api/summary", async (HeadlessStore store, CancellationToken cancellationToken)
    => Results.Ok(await store.GetEstateSummaryAsync(cancellationToken)));

app.MapGet("/api/servers", async (HeadlessStore store, CancellationToken cancellationToken)
    => Results.Ok(await store.GetServersAsync(cancellationToken)));

app.MapGet("/api/collection-log", async (HeadlessStore store, int? limit, CancellationToken cancellationToken)
    => Results.Ok(await store.GetCollectionLogAsync(limit ?? 200, cancellationToken)));

app.MapGet("/api/servers/{serverId}/waits", async (
    string serverId,
    HeadlessStore store,
    int? hours,
    int? limit,
    CancellationToken cancellationToken) =>
{
    var rows = await store.GetTopWaitsAsync(serverId, hours ?? 1, limit ?? 20, cancellationToken);
    return Results.Ok(rows);
});

app.MapGet("/api/servers/{serverId}/cpu", async (
    string serverId,
    HeadlessStore store,
    int? hours,
    CancellationToken cancellationToken) =>
{
    var rows = await store.GetCpuSamplesAsync(serverId, hours ?? 1, cancellationToken);
    return Results.Ok(rows);
});

app.MapFallbackToFile("index.html");

app.Run();
