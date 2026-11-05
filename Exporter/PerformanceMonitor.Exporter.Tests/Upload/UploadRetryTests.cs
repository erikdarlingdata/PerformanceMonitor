using System.Net;
using Amazon.S3;
using PerformanceMonitor.Exporter.Upload;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests.Upload;

public class UploadRetryTests
{
    private static readonly Func<int, TimeSpan> NoDelay = _ => TimeSpan.Zero;
    private static readonly Func<Exception, bool> NeverPermanent = _ => false;

    [Fact]
    public async Task A_transient_failure_is_retried_and_the_upload_succeeds()
    {
        var calls = 0;
        var log = new StringWriter();
        await UploadRetry.WithRetryAsync(() =>
        {
            calls++;
            if (calls < 3)
            {
                throw new AmazonS3Exception("The SHA256 you specified did not match the calculated checksum.") { StatusCode = HttpStatusCode.BadRequest };
            }
            return Task.CompletedTask;
        }, "k", log, CancellationToken.None, NeverPermanent, NoDelay);

        Assert.Equal(3, calls);
        Assert.Contains("upload attempt 1 of 3 for k failed", log.ToString());
        Assert.Contains("upload attempt 2 of 3 for k failed", log.ToString());
    }

    [Fact]
    public async Task The_last_failure_is_thrown_after_every_attempt()
    {
        var calls = 0;
        var ex = await Assert.ThrowsAsync<IOException>(() => UploadRetry.WithRetryAsync(() =>
        {
            calls++;
            throw new IOException($"attempt {calls}");
        }, "k", TextWriter.Null, CancellationToken.None, NeverPermanent, NoDelay));

        Assert.Equal(UploadRetry.MaxUploadAttempts, calls);
        Assert.Equal(3, UploadRetry.MaxUploadAttempts);
        Assert.Equal("attempt 3", ex.Message);
    }

    [Fact]
    public async Task A_permanent_error_is_not_retried()
    {
        var calls = 0;
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => UploadRetry.WithRetryAsync(() =>
        {
            calls++;
            throw new UnauthorizedAccessException("no");
        }, "k", TextWriter.Null, CancellationToken.None, ex => ex is UnauthorizedAccessException, NoDelay));

        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task Cancellation_is_not_retried()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var calls = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => UploadRetry.WithRetryAsync(() =>
        {
            calls++;
            throw new OperationCanceledException();
        }, "k", TextWriter.Null, cts.Token, NeverPermanent, NoDelay));

        Assert.Equal(1, calls);
    }

    [Fact]
    public void The_default_delays_are_10_then_30_seconds()
    {
        Assert.Equal(TimeSpan.FromSeconds(10), UploadRetry.DefaultDelay(1));
        Assert.Equal(TimeSpan.FromSeconds(30), UploadRetry.DefaultDelay(2));
    }

    [Fact]
    public async Task The_delay_callback_gets_the_attempt_number_that_just_failed()
    {
        var seen = new List<int>();
        await Assert.ThrowsAsync<IOException>(() => UploadRetry.WithRetryAsync(
            () => throw new IOException("x"), "k", TextWriter.Null, CancellationToken.None, NeverPermanent,
            attempt =>
            {
                seen.Add(attempt);
                return TimeSpan.Zero;
            }));

        Assert.Equal([1, 2], seen);
    }
}
