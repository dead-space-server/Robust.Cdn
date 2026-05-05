using System.Buffers;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using Dapper;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;
using Quartz;
using Robust.Cdn.Config;
using Robust.Cdn.Helpers;
using Robust.Cdn.Lib;
using SpaceWizards.Sodium;
using SQLitePCL;

namespace Robust.Cdn.Jobs;

[DisallowConcurrentExecution]
public sealed class IngestNewCdnContentJob(
    Database cdnDatabase,
    ManifestDatabase manifestDatabase,
    IOptions<CdnOptions> cdnOptions,
    ISchedulerFactory schedulerFactory,
    BuildDirectoryManager buildDirectoryManager,
    ILogger<IngestNewCdnContentJob> logger) : IJob
{
    public static readonly JobKey Key = new(nameof(IngestNewCdnContentJob));
    public const string KeyForkName = "ForkName";

    public static JobDataMap Data(string fork) => new()
    {
        { KeyForkName, fork }
    };

    public async Task Execute(IJobExecutionContext context)
    {
        var fork = context.MergedJobDataMap.GetString(KeyForkName) ?? throw new InvalidDataException();

        logger.LogInformation("Ingesting new versions for fork: {Fork}", fork);

        var connection = cdnDatabase.Connection;
        var transaction = connection.BeginTransaction();

        List<string> versionsToIngest;
        List<string> versionsToMakeAvailable;
        try
        {
            var forkId = EnsureForkCreated(fork, connection);

            (versionsToIngest, versionsToMakeAvailable) = FindNewVersions(fork, forkId, connection);

            if (versionsToIngest.Count == 0 && versionsToMakeAvailable.Count == 0)
            {
                await QueueManifestCacheUpdate(fork);
                return;
            }

            if (versionsToIngest.Count > 0)
            {
                IngestNewVersions(
                    fork,
                    connection,
                    versionsToIngest,
                    ref transaction,
                    forkId,
                    context.CancellationToken);
            }

            logger.LogDebug("Committing database");

            transaction.Commit();
        }
        finally
        {
            transaction.Dispose();
        }

        if (versionsToMakeAvailable.Count > 0)
            await QueueManifestAvailable(fork, versionsToMakeAvailable);
    }

    private async Task QueueManifestCacheUpdate(string fork)
    {
        var scheduler = await schedulerFactory.GetScheduler();
        await scheduler.TriggerJob(UpdateForkManifestJob.Key, UpdateForkManifestJob.Data(fork));
    }

    private async Task QueueManifestAvailable(string fork, IEnumerable<string> newVersions)
    {
        var scheduler = await schedulerFactory.GetScheduler();
        await scheduler.TriggerJob(
            MakeNewManifestVersionsAvailableJob.Key,
            MakeNewManifestVersionsAvailableJob.Data(fork, newVersions));
    }

    private void IngestNewVersions(
        string fork,
        SqliteConnection connection,
        List<string> newVersions,
        ref SqliteTransaction transaction,
        int forkId,
        CancellationToken cancel)
    {
        var cdnOpts = cdnOptions.Value;

        using var stmtLookupContent = connection.Handle!.Prepare("SELECT Id FROM Content WHERE Hash = ?");
        using var stmtInsertContent = connection.Handle!.Prepare(
            "INSERT INTO Content (Hash, Size, Compression, Data) " +
            "VALUES (@Hash, @Size, @Compression, @Data) " +
            "RETURNING Id");

        using var stmtInsertContentManifestEntry = connection.Handle!.Prepare(
            "INSERT INTO ContentManifestEntry (VersionId, ManifestIdx, ContentId) " +
            "VALUES (@VersionId, @ManifestIdx, @ContentId) ");

        var hash = new byte[32];

        var readBuffer = ArrayPool<byte>.Shared.Rent(1024);
        var compressBuffer = ArrayPool<byte>.Shared.Rent(1024);

        using var compressor = new ZStdCompressionContext();
        SqliteBlobStream? blob = null;

        try
        {
            var versionIdx = 0;
            foreach (var version in newVersions)
            {
                if (versionIdx % 5 == 0)
                {
                    logger.LogDebug("Doing interim commit");

                    blob?.Dispose();
                    blob = null;

                    transaction.Commit();
                    transaction = connection.BeginTransaction();
                }

                cancel.ThrowIfCancellationRequested();

                logger.LogInformation("Ingesting new version: {Version}", version);

                connection.Execute(
                    "DELETE FROM ContentVersion WHERE ForkId = @ForkId AND Version = @Version",
                    new { ForkId = forkId, Version = version });

                var versionId = connection.ExecuteScalar<long>(
                    "INSERT INTO ContentVersion (ForkId, Version, TimeAdded, ManifestHash, ManifestData, CountDistinctBlobs) " +
                    "VALUES (@ForkId, @Version, datetime('now'), zeroblob(0), zeroblob(0), 0) " +
                    "RETURNING Id",
                    new { Version = version, ForkId = forkId });

                stmtInsertContentManifestEntry.BindInt64(1, versionId);

                var clientFileName = GetClientFileName(fork, version);
                if (!IsSafeFileName(clientFileName))
                    throw new InvalidDataException($"Unsafe client file name in manifest DB: {clientFileName}");

                var zipFilePath = buildDirectoryManager.GetBuildVersionFilePath(fork, version, clientFileName);

                using var zipFile = ZipFile.OpenRead(zipFilePath);

                // TODO: hash incrementally without buffering in-memory
                var manifestStream = new MemoryStream();
                var manifestWriter = new StreamWriter(manifestStream, new UTF8Encoding(false));
                manifestWriter.Write("Robust Content Manifest 1\n");

                var newBlobCount = 0;

                var idx = 0;
                foreach (var entry in zipFile.Entries.OrderBy(e => e.FullName, StringComparer.Ordinal))
                {
                    cancel.ThrowIfCancellationRequested();

                    // Ignore directory entries.
                    if (entry.Name == "")
                        continue;

                    var dataLength = (int)entry.Length;

                    BufferHelpers.EnsurePooledBuffer(ref readBuffer, ArrayPool<byte>.Shared, dataLength);

                    var readData = readBuffer.AsSpan(0, dataLength);
                    using (var stream = entry.Open())
                    {
                        stream.ReadExact(readData);
                    }

                    // Hash the data.
                    CryptoGenericHashBlake2B.Hash(hash, readData, ReadOnlySpan<byte>.Empty);

                    // Look up if we already have this blob.
                    stmtLookupContent.BindBlob(1, hash);

                    long contentId;
                    if (stmtLookupContent.Step() == raw.SQLITE_DONE)
                    {
                        stmtLookupContent.Reset();

                        // Don't have this blob yet, add a new one!
                        newBlobCount += 1;

                        ReadOnlySpan<byte> writeData;
                        var compression = ContentCompression.None;

                        // Try compression maybe.
                        if (cdnOpts.BlobCompress)
                        {
                            BufferHelpers.EnsurePooledBuffer(
                                ref compressBuffer,
                                ArrayPool<byte>.Shared,
                                ZStd.CompressBound(dataLength));

                            var compressedLength = compressor.Compress(
                                compressBuffer,
                                readData,
                                cdnOpts.BlobCompressLevel);

                            if (compressedLength + cdnOpts.BlobCompressSavingsThreshold < dataLength)
                            {
                                compression = ContentCompression.ZStd;
                                writeData = compressBuffer.AsSpan(0, compressedLength);
                            }
                            else
                            {
                                writeData = readData;
                            }
                        }
                        else
                        {
                            writeData = readData;
                        }

                        // Insert blob database.

                        stmtInsertContent.BindBlob(1, hash); // @Hash
                        stmtInsertContent.BindInt(2, dataLength); // @Size
                        stmtInsertContent.BindInt(3, (int)compression); // @Compression
                        stmtInsertContent.BindZeroBlob(4, writeData.Length); // @Data

                        stmtInsertContent.Step();

                        contentId = stmtInsertContent.ColumnInt64(0);

                        stmtInsertContent.Reset();

                        if (blob == null)
                        {
                            blob = SqliteBlobStream.Open(
                                connection.Handle!,
                                "main",
                                "Content",
                                "Data",
                                contentId,
                                true);
                        }
                        else
                        {
                            blob.Reopen(contentId);
                        }

                        blob.Write(writeData);
                    }
                    else
                    {
                        contentId = stmtLookupContent.ColumnInt64(0);

                        stmtLookupContent.Reset();
                    }

                    // Insert into ContentManifestEntry
                    stmtInsertContentManifestEntry.BindInt64(2, idx); // @ManifestIdx
                    stmtInsertContentManifestEntry.BindInt64(3, contentId); // @ContentId

                    stmtInsertContentManifestEntry.Step();
                    stmtInsertContentManifestEntry.Reset();

                    // Write manifest entry.
                    manifestWriter.Write($"{Convert.ToHexString(hash)} {entry.FullName}\n");

                    idx += 1;
                }

                logger.LogDebug("Ingested {NewBlobCount} new blobs", newBlobCount);

                // Handle manifest hashing and compression.
                {
                    manifestWriter.Flush();
                    manifestStream.Position = 0;

                    var manifestData = manifestStream.GetBuffer().AsSpan(0, (int)manifestStream.Length);

                    var manifestHash = CryptoGenericHashBlake2B.Hash(32, manifestData, ReadOnlySpan<byte>.Empty);

                    logger.LogDebug("New manifest hash: {ManifestHash}", Convert.ToHexString(manifestHash));

                    BufferHelpers.EnsurePooledBuffer(
                        ref compressBuffer,
                        ArrayPool<byte>.Shared,
                        ZStd.CompressBound(manifestData.Length));

                    var compressedLength = compressor.Compress(
                        compressBuffer,
                        manifestData,
                        cdnOpts.ManifestCompressLevel);

                    var compressedData = compressBuffer.AsSpan(0, compressedLength);

                    connection.Execute(
                        "UPDATE ContentVersion " +
                        "SET ManifestHash = @ManifestHash, ManifestData = zeroblob(@ManifestDataSize) " +
                        "WHERE Id = @VersionId",
                        new
                        {
                            VersionId = versionId,
                            ManifestHash = manifestHash,
                            ManifestDataSize = compressedLength
                        });

                    using var manifestBlob = SqliteBlobStream.Open(
                        connection.Handle!,
                        "main",
                        "ContentVersion",
                        "ManifestData",
                        versionId,
                        true);

                    manifestBlob.Write(compressedData);
                }

                // Calculate CountBlobsDeduplicated on ContentVersion

                connection.Execute(
                    "UPDATE ContentVersion AS cv " +
                    "SET CountDistinctBlobs = " +
                    "   (SELECT COUNT(DISTINCT cme.ContentId) FROM ContentManifestEntry cme WHERE cme.VersionId = cv.Id) " +
                    "WHERE cv.Id = @VersionId",
                    new { VersionId = versionId }
                );

                versionIdx += 1;
            }
        }
        finally
        {
            blob?.Dispose();

            ArrayPool<byte>.Shared.Return(readBuffer);
            ArrayPool<byte>.Shared.Return(compressBuffer);
        }
    }

    private (List<string> versionsToIngest, List<string> versionsToMakeAvailable) FindNewVersions(
        string fork,
        int forkId,
        SqliteConnection con)
    {
        using var stmtCheckVersion = con.Handle!.Prepare(
            "SELECT 1 FROM ContentVersion WHERE ForkId = ? AND Version = ?");

        var versionsToIngest = new List<(string, DateTime)>();
        var versionsToMakeAvailable = new List<(string, DateTime)>();

        var versions = manifestDatabase.Connection.Query<(string name, DateTime publishedTime, bool available, string clientFileName, byte[] clientSha256)>(
            """
            SELECT ForkVersion.Name, ForkVersion.PublishedTime, ForkVersion.Available, ForkVersion.ClientFileName, ForkVersion.ClientSha256
            FROM ForkVersion
            INNER JOIN Fork ON Fork.Id = ForkVersion.ForkId
            WHERE Fork.Name = @Fork
            """,
            new { Fork = fork });

        foreach (var (version, publishedTime, available, clientFileName, clientSha256) in versions)
        {
            var versionDirectory = buildDirectoryManager.GetBuildVersionPath(fork, version);

            logger.LogTrace("Found manifest version: {Version}, publish time: {PublishedTime}", version,
                publishedTime);

            stmtCheckVersion.Reset();
            stmtCheckVersion.BindInt(1, forkId);
            stmtCheckVersion.BindString(2, version);

            var hasContentVersion = stmtCheckVersion.Step() == raw.SQLITE_ROW;
            stmtCheckVersion.Reset();

            if (hasContentVersion && available)
            {
                // Already have version, skip.
                logger.LogTrace("Already have version: {Version}", version);
                continue;
            }

            if (!IsSafeFileName(clientFileName))
            {
                logger.LogError("Manifest DB client file name is unsafe for version {Version}: {ClientFileName}", version, clientFileName);
                continue;
            }

            var clientZipPath = Path.Combine(versionDirectory, clientFileName);

            if (!File.Exists(clientZipPath))
            {
                logger.LogWarning("On-disk version is missing client zip: {Version}", version);
                continue;
            }

            using (var clientZip = File.OpenRead(clientZipPath))
            {
                var diskHash = SHA256.HashData(clientZip);
                if (!diskHash.AsSpan().SequenceEqual(clientSha256))
                {
                    logger.LogError("On-disk client zip hash mismatch for version {Version}; skipping ingest", version);
                    continue;
                }
            }

            if (!ServerBuildsValid(fork, version))
                continue;

            if (hasContentVersion)
                logger.LogWarning("Re-ingesting unavailable content version: {Version}", version);

            versionsToIngest.Add((version, publishedTime));
            if (!available)
                versionsToMakeAvailable.Add((version, publishedTime));
            logger.LogTrace("Found new version: {Version}", version);
        }

        return (
            versionsToIngest.OrderByDescending(x => x.Item2).Select(x => x.Item1).ToList(),
            versionsToMakeAvailable.OrderByDescending(x => x.Item2).Select(x => x.Item1).ToList());
    }

    private string GetClientFileName(string fork, string version)
    {
        return manifestDatabase.Connection.QuerySingle<string>("""
            SELECT ForkVersion.ClientFileName
            FROM ForkVersion
            INNER JOIN Fork ON Fork.Id = ForkVersion.ForkId
            WHERE Fork.Name = @Fork
              AND ForkVersion.Name = @Version
            """,
            new { Fork = fork, Version = version });
    }

    private bool ServerBuildsValid(string fork, string version)
    {
        var serverBuilds = manifestDatabase.Connection.Query<(string fileName, byte[] sha256)>("""
            SELECT ForkVersionServerBuild.FileName, ForkVersionServerBuild.Sha256
            FROM ForkVersionServerBuild
            INNER JOIN ForkVersion ON ForkVersion.Id = ForkVersionServerBuild.ForkVersionId
            INNER JOIN Fork ON Fork.Id = ForkVersion.ForkId
            WHERE Fork.Name = @Fork
              AND ForkVersion.Name = @Version
            """,
            new { Fork = fork, Version = version });

        foreach (var (fileName, sha256) in serverBuilds)
        {
            if (!IsSafeFileName(fileName))
            {
                logger.LogError("Manifest DB server file name is unsafe for version {Version}: {FileName}", version, fileName);
                return false;
            }

            var path = buildDirectoryManager.GetBuildVersionFilePath(fork, version, fileName);
            if (!File.Exists(path))
            {
                logger.LogWarning("On-disk version is missing server zip: {Version} {FileName}", version, fileName);
                return false;
            }

            using var file = File.OpenRead(path);
            var diskHash = SHA256.HashData(file);
            if (!diskHash.AsSpan().SequenceEqual(sha256))
            {
                logger.LogError("On-disk server zip hash mismatch for version {Version} file {FileName}; skipping ingest", version, fileName);
                return false;
            }
        }

        return true;
    }

    private static bool IsSafeFileName(string fileName)
    {
        return !string.IsNullOrEmpty(fileName)
               && fileName == Path.GetFileName(fileName)
               && fileName.IndexOfAny(new[] { '/', '\\' }) == -1;
    }

    private static int EnsureForkCreated(string fork, SqliteConnection connection)
    {
        var id = connection.QuerySingleOrDefault<int?>(
            "SELECT Id FROM Fork WHERE Name = @Name",
            new { Name = fork });

        id ??= connection.QuerySingle<int>(
            "INSERT INTO Fork (Name) VALUES (@Name) RETURNING Id",
            new { Name = fork });

        return id.Value;
    }
}
