using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.File;

namespace AzureCopy
{
    public static class TransferManager
    {
        public static void Copyto(this CloudBlobDirectory srcDir, CloudBlobDirectory dstDir)
            => new CloudBlobDirectoryTransferInfo(srcDir, string.Empty).Copyto(new CloudBlobDirectoryTransferInfo(dstDir, string.Empty));

        public static void Copyto(this CloudBlobDirectory srcDir, CloudFileDirectory dstDir)
            => new CloudBlobDirectoryTransferInfo(srcDir, string.Empty).Copyto(new CloudFileDirectoryTransferInfo(dstDir, string.Empty));

        public static void Copyto(this CloudBlobDirectory srcDir, DirectoryInfo dstDir)
            => new CloudBlobDirectoryTransferInfo(srcDir, string.Empty).Copyto(new LocalDirectoryTransferInfo(dstDir, string.Empty));

        public static void Copyto(this CloudFileDirectory srcDir, CloudBlobDirectory dstDir)
            => new CloudFileDirectoryTransferInfo(srcDir, string.Empty).Copyto(new CloudBlobDirectoryTransferInfo(dstDir, string.Empty));

        public static void Copyto(this CloudFileDirectory srcDir, CloudFileDirectory dstDir)
            => new CloudFileDirectoryTransferInfo(srcDir, string.Empty).Copyto(new CloudFileDirectoryTransferInfo(dstDir, string.Empty));

        public static void Copyto(this CloudFileDirectory srcDir, DirectoryInfo dstDir)
            => new CloudFileDirectoryTransferInfo(srcDir, string.Empty).Copyto(new LocalDirectoryTransferInfo(dstDir, string.Empty));

        public static void Copyto(this DirectoryInfo srcDir, CloudBlobDirectory dstDir)
            => new LocalDirectoryTransferInfo(srcDir, string.Empty).Copyto(new CloudBlobDirectoryTransferInfo(dstDir, string.Empty));

        public static void Copyto(this DirectoryInfo srcDir, CloudFileDirectory dstDir)
            => new LocalDirectoryTransferInfo(srcDir, string.Empty).Copyto(new CloudFileDirectoryTransferInfo(dstDir, string.Empty));

        public static void Copyto(this DirectoryInfo srcDir, DirectoryInfo dstDir)
            => new LocalDirectoryTransferInfo(srcDir, string.Empty).Copyto(new LocalDirectoryTransferInfo(dstDir, string.Empty));

        public static void Copyto(this IDirectoryTransferInfo srcDir, IDirectoryTransferInfo dstDir)
        {
            // TODO: parallelism based on cores
            var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = 4 };

            // enumearte all entries in source and destination
            var srcEntries = new Dictionary<string, ITransferInfo>(StringComparer.OrdinalIgnoreCase);
            var dstEntries = new Dictionary<string, ITransferInfo>(StringComparer.OrdinalIgnoreCase);
            var dirs = new[] { srcDir, dstDir };
            var entries = new[] { srcEntries, dstEntries };
            Console.Write($"Enumerating source and destination directories ");
            Parallel.For(0, dirs.Length, parallelOptions, index =>
            {
                foreach (var entry in dirs[index].EnumerateEntries())
                {
                    if (entries[index].Count % 10 == 0)
                    {
                        Console.Write(".");
                    }
                    entries[index].Add(entry.RelativePath, entry);
                }
            });
            Console.WriteLine($" src: {srcEntries.Count} entries, dst: {dstEntries.Count} entries");

            // create directories on destination
            var completed = 0;
            var srcDirectoryEntries = srcEntries.Values.OfType<IDirectoryTransferInfo>().ToArray();
            foreach (var srcEntry in srcDirectoryEntries)
            {
                if (dstEntries.TryGetValue(srcEntry.RelativePath, out var dstEntry))
                {
                    if (!(dstEntry is IDirectoryTransferInfo))
                    {
                        throw new InvalidOperationException($"The destination '{dstEntry.FullName}' is not directory!");
                    }

                    WriteLine($"({Interlocked.Increment(ref completed)} of {srcDirectoryEntries.Length}) {srcEntry.RelativePath}: Directory already exists  '{dstEntry.FullName}'");
                }
                else
                {
                    var newEntry = dstDir.CreateIfNotExists(srcEntry.RelativePath);
                    WriteLine($"({Interlocked.Increment(ref completed)} of {srcDirectoryEntries.Length}) {srcEntry.RelativePath}: Directory created '{newEntry.FullName}'");
                }
            }

            // second pass: copy files
            var srcFileEntries = srcEntries.Values.OfType<IFileTransferInfo>().ToArray();
            completed = 0;
            Parallel.For(0, srcFileEntries.Length, parallelOptions, index =>
            {
                var srcFileEntry = srcFileEntries[index];
                var exists = dstEntries.TryGetValue(srcFileEntry.RelativePath, out var dstEntry);
                var dstFileEntry = dstEntry as IFileTransferInfo;
                if (exists)
                {
                    if (dstFileEntry == null)
                    {
                        throw new InvalidOperationException($"The destination '{dstEntry.FullName}' is not of file type!");
                    }

                    // length and MD5 match, skip
                    if (srcFileEntry.Length == dstFileEntry.Length
                        && (string.IsNullOrEmpty(srcFileEntry.MD5Hash) || string.Equals(srcFileEntry.MD5Hash, dstFileEntry.MD5Hash)))
                    {
                        WriteLine($"({Interlocked.Increment(ref completed)} of {srcFileEntries.Length}) {dstFileEntry.RelativePath} ({dstFileEntry.Length} bytes): Skipping already matching MD5 '{srcFileEntry.MD5Hash}' vs '{dstFileEntry.MD5Hash}'");
                        return;
                    }
                }

                dstFileEntry = dstFileEntry ?? dstDir.GetChildEntry(srcFileEntry.RelativePath);

                using (var stream = srcFileEntry.ReadStream())
                {
                    dstFileEntry.WriteStream(stream);
                }

                if (!string.IsNullOrEmpty(srcFileEntry.MD5Hash) && !string.Equals(srcFileEntry.MD5Hash, dstFileEntry.MD5Hash))
                {
                    throw new InvalidOperationException($"Mismatch MD5 after write '{dstEntry.FullName}', '{srcFileEntry.MD5Hash}' != '{dstFileEntry.MD5Hash}'");
                }

                WriteLine($"({Interlocked.Increment(ref completed)} of {srcFileEntries.Length}) {dstFileEntry.RelativePath} ({dstFileEntry.Length} bytes): Successfully {(exists ? "overwriting" : "creating")} '{dstFileEntry.FullName}', MD5: '{dstFileEntry.MD5Hash}'");
            });
        }

        static void WriteLine(object obj)
        {
            WriteLine($"{0}", obj);
        }

        static void WriteLine(string format, params object[] args)
        {
            lock (typeof(Console))
            {
                Console.WriteLine($"{DateTime.UtcNow:s}Z [{Process.GetCurrentProcess().Id}:{Thread.CurrentThread.ManagedThreadId}] {string.Format(format, args)}");
            }
        }
    }
}
