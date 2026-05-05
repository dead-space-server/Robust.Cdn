using System.Net.Http.Headers;
using System.Text;
using Dapper;
using Microsoft.Extensions.Options;
using Quartz;
using Robust.Cdn.Config;

namespace Robust.Cdn.Jobs;

/// <summary>
/// Job responsible for notifying <c>SS14.Watchdog</c> instances that a new update is available.
/// </summary>
/// <remarks>
/// This job is triggered by <see cref="MakeNewManifestVersionsAvailableJob"/>.
/// </remarks>
[DisallowConcurrentExecution]
public sealed class NotifyWatchdogUpdateJob(
    ManifestDatabase database,
    IHttpClientFactory httpClientFactory,
    ISchedulerFactory schedulerFactory,
    ILogger<NotifyWatchdogUpdateJob> logger,
    IOptions<ManifestOptions> manifestOptions) : IJob
{
    public static readonly JobKey Key = new(nameof(NotifyWatchdogUpdateJob));

    public const string KeyForkName = "ForkName";
    public const string KeyRetryCount = "RetryCount";

    public const string HttpClientName = "NotifyWatchdogUpdateJob";

    public static JobDataMap Data(string fork, int retryCount = 0) => new()
    {
        { KeyForkName, fork },
        { KeyRetryCount, retryCount },
    };

    public async Task Execute(IJobExecutionContext context)
    {
        var fork = context.MergedJobDataMap.GetString(KeyForkName) ?? throw new InvalidDataException();
        var retryCount = context.MergedJobDataMap.GetIntValue(KeyRetryCount);
        var config = manifestOptions.Value.Forks[fork];
        var forkId = database.Connection.QuerySingle<int>(
            "SELECT Id FROM Fork WHERE Name = @ForkName",
            new { ForkName = fork });
        var pendingVersionIds = GetPendingNotifyVersionIds(forkId);
        if (pendingVersionIds.Length == 0)
            return;

        if (config.NotifyWatchdogs.Length == 0)
        {
            ClearPendingNotify(forkId, pendingVersionIds);
            await UnscheduleRetry(fork, context.CancellationToken);
            return;
        }

        logger.LogInformation("Notifying watchdogs of update for fork {Fork}", fork);

        var httpClient = httpClientFactory.CreateClient(HttpClientName);

        var results = await Task.WhenAll(
            config.NotifyWatchdogs.Select(notify => SendNotify(notify, httpClient, context.CancellationToken)));

        if (results.All(static success => success))
        {
            ClearPendingNotify(forkId, pendingVersionIds);
            await UnscheduleRetry(fork, context.CancellationToken);
        }
        else if (pendingVersionIds.Length != 0)
        {
            await ScheduleRetry(fork, retryCount, context.CancellationToken);
        }
    }

    private async Task<bool> SendNotify(
        ManifestForkNotifyWatchdog watchdog,
        HttpClient client,
        CancellationToken cancel)
    {
        logger.LogDebug(
            "Sending watchdog update notify to {WatchdogUrl} instance {Instance}",
            watchdog.WatchdogUrl,
            watchdog.Instance);

        var url = NormalizeTrailingSlash(watchdog.WatchdogUrl) + $"instances/{watchdog.Instance}/update";
        var request = new HttpRequestMessage(HttpMethod.Post, url);
        request.Headers.Authorization = new AuthenticationHeaderValue(
            "Basic",
            FormatBasicAuth(watchdog.Instance, watchdog.ApiToken));

        try
        {
            using var response = await client.SendAsync(request, cancel);

            if (!response.IsSuccessStatusCode)
            {
                var responseContent = await response.Content.ReadAsStringAsync(cancel);
                logger.LogWarning(
                    "Update notify to {WatchdogUrl} instance {Instance} did not indicate success ({Status}): {ResponseContent}",
                    watchdog.WatchdogUrl, watchdog.Instance, response.StatusCode, responseContent);
                return false;
            }

            return true;
        }
        catch (OperationCanceledException) when (cancel.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception e)
        {
            logger.LogWarning(
                e,
                "Error while notifying watchdog {WatchdogUrl} instance {Instance} of update",
                watchdog.WatchdogUrl,
                watchdog.Instance);
            return false;
        }
    }

    private int[] GetPendingNotifyVersionIds(int forkId)
    {
        return database.Connection.Query<int>("""
            SELECT Id
            FROM ForkVersion
            WHERE ForkId = @ForkId
              AND NotifyPending
            """,
            new { ForkId = forkId }).ToArray();
    }

    private void ClearPendingNotify(int forkId, int[] versionIds)
    {
        if (versionIds.Length == 0)
            return;

        database.Connection.Execute("""
            UPDATE ForkVersion
            SET NotifyPending = FALSE
            WHERE ForkId = @ForkId
              AND Id IN @VersionIds
              AND NotifyPending
            """,
            new { ForkId = forkId, VersionIds = versionIds });
    }

    private async Task ScheduleRetry(string fork, int retryCount, CancellationToken cancel)
    {
        var delay = TimeSpan.FromMinutes(Math.Min(30, 1 << Math.Min(retryCount, 5)));
        var triggerKey = RetryTriggerKey(fork);
        var trigger = TriggerBuilder.Create()
            .WithIdentity(triggerKey)
            .ForJob(Key)
            .UsingJobData(Data(fork, retryCount + 1))
            .StartAt(DateTimeOffset.UtcNow.Add(delay))
            .Build();

        logger.LogWarning(
            "Watchdog update notify for fork {Fork} failed; retrying in {Delay}",
            fork,
            delay);

        var scheduler = await schedulerFactory.GetScheduler(cancel);
        await scheduler.UnscheduleJob(triggerKey, cancel);
        await scheduler.ScheduleJob(trigger, cancel);
    }

    private async Task UnscheduleRetry(string fork, CancellationToken cancel)
    {
        var scheduler = await schedulerFactory.GetScheduler(cancel);
        await scheduler.UnscheduleJob(RetryTriggerKey(fork), cancel);
    }

    private static TriggerKey RetryTriggerKey(string fork)
    {
        return new TriggerKey($"{nameof(NotifyWatchdogUpdateJob)}:{fork}:retry");
    }

    private static string NormalizeTrailingSlash(string url)
    {
        return url.EndsWith('/') ? url : url + '/';
    }

    private static string FormatBasicAuth(string user, string password)
    {
        return Convert.ToBase64String(Encoding.UTF8.GetBytes($"{user}:{password}"));
    }
}
