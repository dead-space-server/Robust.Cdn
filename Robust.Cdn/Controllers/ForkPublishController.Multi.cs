using Dapper;
using Microsoft.AspNetCore.Mvc;
using Robust.Cdn.Helpers;
using Robust.Cdn.Services;

namespace Robust.Cdn.Controllers;

public sealed partial class ForkPublishController
{
    // Code for "multi-request" publishes.
    // i.e. start, followed by files, followed by finish call.

    [HttpPost("start")]
    public async Task<IActionResult> MultiPublishStart(
        string fork,
        [FromBody] PublishMultiRequest request,
        CancellationToken cancel)
    {
        if (!authHelper.IsAuthValid(fork, out _, out var failureResult))
            return failureResult;

        baseUrlManager.ValidateBaseUrl();

        if (!ValidVersionRegex.IsMatch(request.Version))
            return BadRequest("Invalid version name");

        using var publishLock = await PublishLockManager.AcquireAsync(fork, request.Version, cancel);

        if (VersionAlreadyExists(fork, request.Version))
            return Conflict("Version already exists");

        var dbCon = manifestDatabase.Connection;

        await using var tx = await dbCon.BeginTransactionAsync(cancel);

        logger.LogInformation("Starting multi publish for fork {Fork} version {Version}", fork, request.Version);

        var forkId = dbCon.QuerySingle<int>("SELECT Id FROM Fork WHERE Name = @Name", new { Name = fork });
        var hasExistingPublish = dbCon.QuerySingleOrDefault<bool>(
            "SELECT 1 FROM PublishInProgress WHERE Version = @Version AND ForkId = @ForkId",
            new { request.Version, ForkId = forkId });
        if (hasExistingPublish)
        {
            logger.LogWarning("Publish already in progress for fork {Fork} version {Version}", fork, request.Version);
            return Conflict("Version publish already in progress");
        }

        var publishId = Guid.NewGuid().ToString("N");

        await dbCon.ExecuteAsync("""
            INSERT INTO PublishInProgress (Version, ForkId, StartTime, EngineVersion, PublishId)
            VALUES (@Version, @ForkId, @StartTime, @EngineVersion, @PublishId)
            """,
            new
            {
                request.Version,
                request.EngineVersion,
                PublishId = publishId,
                ForkId = forkId,
                StartTime = DateTime.UtcNow
            });

        var versionDir = buildDirectoryManager.GetBuildVersionPath(fork, request.Version);
        if (Directory.Exists(versionDir))
        {
            logger.LogWarning("Deleting stale publish directory for fork {Fork} version {Version}", fork, request.Version);
            Directory.Delete(versionDir, recursive: true);
        }

        Directory.CreateDirectory(versionDir);

        await tx.CommitAsync(cancel);

        Response.Headers["Robust-Cdn-Publish-Id"] = publishId;

        logger.LogInformation("Multi publish initiated. Waiting for subsequent API requests...");

        return NoContent();
    }

    [HttpPost("file")]
    [RequestSizeLimit(2048L * 1024 * 1024)]
    public async Task<IActionResult> MultiPublishFile(
        string fork,
        [FromHeader(Name = "Robust-Cdn-Publish-File")]
        string fileName,
        [FromHeader(Name = "Robust-Cdn-Publish-Version")]
        string version,
        [FromHeader(Name = "Robust-Cdn-Publish-Id")]
        string publishId,
        CancellationToken cancel)
    {
        if (!authHelper.IsAuthValid(fork, out _, out var failureResult))
            return failureResult;

        if (!ValidFileRegex.IsMatch(fileName))
            return BadRequest("Invalid artifact file name");

        if (!ValidVersionRegex.IsMatch(version))
            return BadRequest("Invalid version name");

        if (string.IsNullOrWhiteSpace(publishId))
            return BadRequest("Missing publish id");

        using var publishLock = await PublishLockManager.AcquireAsync(fork, version, cancel);

        var dbCon = manifestDatabase.Connection;
        await using var tx = await dbCon.BeginTransactionAsync(cancel);

        var forkId = dbCon.QuerySingle<int>("SELECT Id FROM Fork WHERE Name = @Name", new { Name = fork });
        var versionId = dbCon.QuerySingleOrDefault<int?>("""
            SELECT Id
            FROM PublishInProgress
            WHERE Version = @Name AND ForkId = @Fork
              AND PublishId = @PublishId
            """,
            new { Name = version, Fork = forkId, PublishId = publishId });

        if (versionId == null)
            return NotFound("Unknown in-progress version");

        var versionDir = buildDirectoryManager.GetBuildVersionPath(fork, version);
        var filePath = Path.Combine(versionDir, fileName);
        var tempFilePath = Path.Combine(versionDir, $".{fileName}.{Guid.NewGuid():N}.tmp");

        if (System.IO.File.Exists(filePath))
            return Conflict("File already published");

        logger.LogDebug("Receiving file {FileName} for multi-publish version {Version}", fileName, version);

        try
        {
            await using (var file = System.IO.File.Create(tempFilePath, 4096, FileOptions.Asynchronous))
            {
                await Request.Body.CopyToAsync(file, cancel);
                await file.FlushAsync(cancel);
            }

            try
            {
                System.IO.File.Move(tempFilePath, filePath);
            }
            catch (IOException) when (System.IO.File.Exists(filePath))
            {
                System.IO.File.Delete(tempFilePath);
                return Conflict("File already published");
            }
        }
        catch
        {
            if (System.IO.File.Exists(tempFilePath))
                System.IO.File.Delete(tempFilePath);

            throw;
        }

        logger.LogDebug("Successfully Received file {FileName}", fileName);

        return NoContent();
    }

    [HttpPost("finish")]
    public async Task<IActionResult> MultiPublishFinish(
        string fork,
        [FromBody] PublishFinishRequest request,
        [FromHeader(Name = "Robust-Cdn-Publish-Id")]
        string publishId,
        CancellationToken cancel)
    {
        if (!authHelper.IsAuthValid(fork, out var forkConfig, out var failureResult))
            return failureResult;

        if (!ValidVersionRegex.IsMatch(request.Version))
            return BadRequest("Invalid version name");

        if (string.IsNullOrWhiteSpace(publishId))
            return BadRequest("Missing publish id");

        using var publishLock = await PublishLockManager.AcquireAsync(fork, request.Version, cancel);

        var dbCon = manifestDatabase.Connection;
        await using var tx = await dbCon.BeginTransactionAsync(cancel);

        var forkId = dbCon.QuerySingle<int>("SELECT Id FROM Fork WHERE Name = @Name", new { Name = fork });
        var versionMetadata = dbCon.QuerySingleOrDefault<VersionMetadata>("""
            SELECT Version, EngineVersion
            FROM PublishInProgress
            WHERE Version = @Name AND ForkId = @Fork
              AND PublishId = @PublishId
            """,
            new { Name = request.Version, Fork = forkId, PublishId = publishId });

        if (versionMetadata == null)
            return NotFound("Unknown in-progress version");

        logger.LogInformation("Finishing multi publish {Version} for fork {Fork}", request.Version, fork);

        var versionDir = buildDirectoryManager.GetBuildVersionPath(fork, request.Version);

        logger.LogDebug("Classifying entries...");

        var artifacts = ClassifyEntries(
            forkConfig,
            Directory.GetFiles(versionDir),
            item => Path.GetRelativePath(versionDir, item));

        var clientArtifact = artifacts.SingleOrNull(art => art.artifact.Type == ArtifactType.Client);
        if (clientArtifact == null)
        {
            publishManager.AbortMultiPublish(fork, request.Version, tx, commit: true);
            return UnprocessableEntity("Publish failed: no client zip was provided");
        }

        var diskFiles = artifacts.ToDictionary(i => i.artifact, i => i.key);

        var buildJson = GenerateBuildJson(diskFiles, clientArtifact.Value.artifact, versionMetadata, fork);
        InjectBuildJsonIntoServers(diskFiles, buildJson);

        AddVersionToDatabase(clientArtifact.Value.artifact, diskFiles, fork, versionMetadata);

        dbCon.Execute(
            "DELETE FROM PublishInProgress WHERE Version = @Name AND ForkId = @Fork AND PublishId = @PublishId",
            new { Name = request.Version, Fork = forkId, PublishId = publishId });

        tx.Commit();

        await QueueIngestJobAsync(fork);

        logger.LogInformation("Publish succeeded!");

        return NoContent();
    }

    [HttpPost("abort")]
    public async Task<IActionResult> MultiPublishAbort(
        string fork,
        [FromBody] PublishFinishRequest request,
        [FromHeader(Name = "Robust-Cdn-Publish-Id")]
        string publishId,
        CancellationToken cancel)
    {
        if (!authHelper.IsAuthValid(fork, out _, out var failureResult))
            return failureResult;

        if (!ValidVersionRegex.IsMatch(request.Version))
            return BadRequest("Invalid version name");

        if (string.IsNullOrWhiteSpace(publishId))
            return BadRequest("Missing publish id");

        using var publishLock = await PublishLockManager.AcquireAsync(fork, request.Version, cancel);

        var dbCon = manifestDatabase.Connection;
        await using var tx = await dbCon.BeginTransactionAsync(cancel);

        var forkId = dbCon.QuerySingle<int>("SELECT Id FROM Fork WHERE Name = @Name", new { Name = fork });
        var hasPublish = dbCon.QuerySingleOrDefault<bool>("""
            SELECT 1
            FROM PublishInProgress
            WHERE Version = @Name
              AND ForkId = @Fork
              AND PublishId = @PublishId
            """,
            new { Name = request.Version, Fork = forkId, PublishId = publishId });

        if (!hasPublish)
            return NotFound("Unknown in-progress version");

        publishManager.AbortMultiPublish(fork, request.Version, tx, commit: false);
        await tx.CommitAsync(cancel);

        return NoContent();
    }

    public sealed class PublishMultiRequest
    {
        public required string Version { get; set; }
        public required string EngineVersion { get; set; }
    }

    public sealed class PublishFinishRequest
    {
        public required string Version { get; set; }
    }
}
