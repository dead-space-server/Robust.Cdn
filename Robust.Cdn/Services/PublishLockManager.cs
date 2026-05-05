using System.Collections.Concurrent;

namespace Robust.Cdn.Services;

public static class PublishLockManager
{
    private static readonly ConcurrentDictionary<string, SemaphoreSlim> Locks = new(StringComparer.Ordinal);

    public static async Task<IDisposable> AcquireAsync(string fork, string version, CancellationToken cancel)
    {
        var semaphore = GetSemaphore(fork, version);
        await semaphore.WaitAsync(cancel);
        return new Releaser(semaphore);
    }

    public static IDisposable Acquire(string fork, string version)
    {
        var semaphore = GetSemaphore(fork, version);
        semaphore.Wait();
        return new Releaser(semaphore);
    }

    private static SemaphoreSlim GetSemaphore(string fork, string version)
    {
        return Locks.GetOrAdd($"{fork}\0{version}", _ => new SemaphoreSlim(1, 1));
    }

    private sealed class Releaser(SemaphoreSlim semaphore) : IDisposable
    {
        public void Dispose()
        {
            semaphore.Release();
        }
    }
}
