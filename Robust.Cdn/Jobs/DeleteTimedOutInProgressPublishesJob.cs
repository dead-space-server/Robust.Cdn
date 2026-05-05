using Dapper;
using Microsoft.Extensions.Options;
using Quartz;
using Robust.Cdn.Config;
using Robust.Cdn.Services;

namespace Robust.Cdn.Jobs;

/// <summary>
/// Job that periodically goes through and deletes old in-progress publishes that have "timed out".
/// </summary>
/// <remarks>
/// This deletes old in-progress publishes that have taken too long since being initiated,
/// which likely indicates that the publish encountered an error and will never be completed.
/// </remarks>
/// <seealso cref="ManifestOptions.InProgressPublishTimeoutMinutes"/>
public sealed class DeleteInProgressPublishesJob(
    PublishManager publishManager,
    ManifestDatabase manifestDatabase,
    TimeProvider timeProvider,
    IOptions<ManifestOptions> options,
    ILogger<DeleteInProgressPublishesJob> logger) : IJob
{
    public Task Execute(IJobExecutionContext context)
    {
        var opts = options.Value;

        logger.LogTrace("Checking for timed out in-progress publishes");

        var db = manifestDatabase.Connection;

        var deleteBefore = timeProvider.GetUtcNow() - TimeSpan.FromMinutes(opts.InProgressPublishTimeoutMinutes);

        var totalDeleted = 0;

        var inProgress = db.Query<(int, string, string, DateTime)>("""
            SELECT PublishInProgress.Id, Version, Fork.Name, StartTime
            FROM PublishInProgress
            INNER JOIN Fork ON Fork.Id = PublishInProgress.ForkId
            """);

        foreach (var (_, name, forkName, startTime) in inProgress)
        {
            if (startTime >= deleteBefore)
                continue;

            using var publishLock = PublishLockManager.Acquire(forkName, name);

            using var tx = db.BeginTransaction();

            var shouldDelete = db.QuerySingleOrDefault<bool>("""
                SELECT 1
                FROM PublishInProgress
                INNER JOIN Fork ON Fork.Id = PublishInProgress.ForkId
                WHERE PublishInProgress.Version = @Version
                  AND Fork.Name = @Fork
                  AND PublishInProgress.StartTime = @StartTime
                  AND PublishInProgress.StartTime < @DeleteBefore
                  AND NOT EXISTS (
                      SELECT 1
                      FROM ForkVersion
                      WHERE ForkVersion.ForkId = PublishInProgress.ForkId
                        AND ForkVersion.Name = PublishInProgress.Version
                  )
                """,
                new { Version = name, Fork = forkName, StartTime = startTime, DeleteBefore = deleteBefore },
                tx);

            if (!shouldDelete)
            {
                tx.Rollback();
                continue;
            }

            logger.LogInformation("Deleting timed out publish for fork {Fork} version {Version}", forkName, name);

            publishManager.AbortMultiPublish(forkName, name, tx, commit: false);

            tx.Commit();

            totalDeleted += 1;
        }

        logger.LogInformation("Deleted {TotalDeleted} timed out publishes", totalDeleted);

        return Task.CompletedTask;
    }
}
