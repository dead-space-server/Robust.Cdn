using Dapper;
using Microsoft.Extensions.Options;
using Quartz;
using Robust.Cdn.Config;

namespace Robust.Cdn.Jobs;

/// <summary>
/// Job that periodically deletes old builds from the server manifest and client CDN storage.
/// </summary>
/// <remarks>
/// This job runs every 24 hours automatically.
/// </remarks>
/// <seealso cref="ManifestForkOptions.PruneBuildsDays"/>
[DisallowConcurrentExecution]
public sealed class PruneOldManifestBuilds(
    Database cdnDatabase,
    ManifestDatabase manifestDatabase,
    IOptions<ManifestOptions> options,
    BuildDirectoryManager buildDirectoryManager,
    TimeProvider timeProvider,
    ISchedulerFactory schedulerFactory,
    ILogger<PruneOldManifestBuilds> logger) : IJob
{
    public async Task Execute(IJobExecutionContext context)
    {
        var opts = options.Value;

        logger.LogInformation("Pruning old builds");

        var totalManifestBuildsPruned = 0;
        var totalCdnVersionsPruned = 0;
        var totalCdnBlobsPruned = 0;
        var scheduler = await schedulerFactory.GetScheduler();
        var now = timeProvider.GetUtcNow().UtcDateTime;

        foreach (var (forkName, forkConfig) in opts.Forks)
        {
            context.CancellationToken.ThrowIfCancellationRequested();

            var result = await PruneFork(forkName, forkConfig, now, scheduler, context.CancellationToken);
            totalManifestBuildsPruned += result.ManifestBuilds;
            totalCdnVersionsPruned += result.CdnVersions;
            totalCdnBlobsPruned += result.CdnBlobs;
        }

        logger.LogInformation(
            "Pruned {ManifestBuildsPruned} old manifest builds, {CdnVersionsPruned} CDN versions, {CdnBlobsPruned} CDN blobs",
            totalManifestBuildsPruned,
            totalCdnVersionsPruned,
            totalCdnBlobsPruned);
    }

    private async Task<PruneForkResult> PruneFork(
        string forkName,
        ManifestForkOptions forkConfig,
        DateTime now,
        IScheduler scheduler,
        CancellationToken cancel)
    {
        if (forkConfig.PruneBuildsDays <= 0)
        {
            logger.LogDebug("Not pruning fork {Fork}: pruning is disabled", forkName);
            return default;
        }

        logger.LogDebug("Pruning old builds for fork {Fork}", forkName);

        var pruneFrom = now - TimeSpan.FromDays(forkConfig.PruneBuildsDays);

        var manifestPruned = PruneManifestBuilds(forkName, pruneFrom, cancel);
        if (manifestPruned.Count > 0)
        {
            await scheduler.TriggerJob(
                UpdateForkManifestJob.Key,
                UpdateForkManifestJob.Data(forkName));
        }

        var cdnPruned = PruneCdnBuilds(forkName, manifestPruned, pruneFrom, cancel);

        return new PruneForkResult(manifestPruned.Count, cdnPruned.Versions, cdnPruned.Blobs);
    }

    private List<string> PruneManifestBuilds(string forkName, DateTime pruneFrom, CancellationToken cancel)
    {
        var builds = manifestDatabase.Connection.Query<ManifestVersionData>("""
            SELECT FV.Id, FV.Name
            FROM ForkVersion FV, Fork
            WHERE FV.ForkId = Fork.Id
              AND Fork.Name = @ForkName
              AND FV.PublishedTime < @PruneFrom
            """, new { ForkName = forkName, PruneFrom = pruneFrom }).ToList();

        var prunedVersions = new List<string>(builds.Count);
        foreach (var versionData in builds)
        {
            cancel.ThrowIfCancellationRequested();
            logger.LogDebug("Pruning fork version {Version}", versionData.Name);

            var directory = buildDirectoryManager.GetBuildVersionPath(forkName, versionData.Name);
            if (!IsSafeBuildPath(forkName, directory))
            {
                logger.LogError(
                    "Refusing to delete build directory outside fork path for fork {Fork}, version {Version}: {Directory}",
                    forkName,
                    versionData.Name,
                    directory);
                continue;
            }

            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
                logger.LogTrace("Version directory deleted: {Directory}", directory);
            }
            else
            {
                logger.LogTrace("Version directory didn't exist when cleaning it up ({Directory})", directory);
            }

            manifestDatabase.Connection.Execute("DELETE FROM ForkVersion WHERE Id = @Id", versionData);
            prunedVersions.Add(versionData.Name);
        }

        return prunedVersions;
    }

    private CdnPruneResult PruneCdnBuilds(
        string forkName,
        IReadOnlyCollection<string> manifestPrunedVersions,
        DateTime pruneFrom,
        CancellationToken cancel)
    {
        var cdnConnection = cdnDatabase.Connection;
        var cdnForkId = cdnConnection.QuerySingleOrDefault<int?>(
            "SELECT Id FROM Fork WHERE Name = @Name",
            new { Name = forkName });

        if (cdnForkId == null)
        {
            logger.LogTrace("No CDN content fork exists for manifest fork {Fork}", forkName);
            return default;
        }

        var currentManifestVersions = manifestDatabase.Connection.Query<string>("""
            SELECT FV.Name
            FROM ForkVersion FV
            INNER JOIN Fork ON Fork.Id = FV.ForkId
            WHERE Fork.Name = @ForkName
            """, new { ForkName = forkName }).ToHashSet(StringComparer.Ordinal);

        var manifestPrunedVersionSet = manifestPrunedVersions.ToHashSet(StringComparer.Ordinal);
        // Older versions of this job only removed manifest rows, so also catch old CDN-only rows.
        var versionsToPrune = cdnConnection.Query<CdnVersionData>("""
            SELECT Id, Version AS Name, TimeAdded
            FROM ContentVersion
            WHERE ForkId = @ForkId
            """, new { ForkId = cdnForkId })
            .Where(version =>
                manifestPrunedVersionSet.Contains(version.Name)
                || (version.TimeAdded < pruneFrom && !currentManifestVersions.Contains(version.Name)))
            .ToDictionary(version => version.Id);

        if (versionsToPrune.Count == 0)
            return default;

        using var tx = cdnConnection.BeginTransaction();

        var versionsPruned = 0;
        foreach (var version in versionsToPrune.Values)
        {
            cancel.ThrowIfCancellationRequested();
            logger.LogDebug("Pruning CDN content version {Version}", version.Name);

            versionsPruned += cdnConnection.Execute(
                "DELETE FROM ContentVersion WHERE Id = @Id",
                version,
                tx);
        }

        var blobsPruned = cdnConnection.Execute("""
            DELETE FROM Content
            WHERE NOT EXISTS (
                SELECT 1
                FROM ContentManifestEntry
                WHERE ContentManifestEntry.ContentId = Content.Id
            )
            """, transaction: tx);

        tx.Commit();

        return new CdnPruneResult(versionsPruned, blobsPruned);
    }

    private bool IsSafeBuildPath(string forkName, string directory)
    {
        var forkPath = Path.GetFullPath(buildDirectoryManager.GetForkPath(forkName));
        var fullDirectory = Path.GetFullPath(directory);
        var comparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;

        forkPath = Path.TrimEndingDirectorySeparator(forkPath) + Path.DirectorySeparatorChar;

        return fullDirectory.StartsWith(forkPath, comparison);
    }

    private readonly record struct PruneForkResult(int ManifestBuilds, int CdnVersions, int CdnBlobs);

    private readonly record struct CdnPruneResult(int Versions, int Blobs);

    private sealed class ManifestVersionData
    {
        public required int Id { get; set; }
        public required string Name { get; set; }
    }

    private sealed class CdnVersionData
    {
        public required int Id { get; set; }
        public required string Name { get; set; }
        public required DateTime TimeAdded { get; set; }
    }
}
