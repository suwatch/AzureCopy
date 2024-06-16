using System.Collections.Generic;
using System.Threading;
using Microsoft.WindowsAzure.Storage.File;

namespace AzureCopy
{
    public class CloudFileDirectoryTransferInfo : TransferInfo, IDirectoryTransferInfo
    {
        private readonly CloudFileDirectory _cloudDirectory;

        public CloudFileDirectoryTransferInfo(CloudFileDirectory cloudDirectory, string relativePath) : base(cloudDirectory.Uri.AbsoluteUri, relativePath)
        {
            _cloudDirectory = cloudDirectory;
        }

        public override bool Exists => _cloudDirectory.Exists();

        public IDirectoryTransferInfo CreateIfNotExists(string relativePath = null)
        {
            var dir = string.IsNullOrEmpty(relativePath) ? _cloudDirectory : _cloudDirectory.GetDirectoryReference(relativePath);
            dir.CreateIfNotExists();
            return string.IsNullOrEmpty(relativePath) ? this : new CloudFileDirectoryTransferInfo(dir, relativePath);
        }

        public IFileTransferInfo GetChildEntry(string relativePath) 
            => new CloudFileTransferInfo(_cloudDirectory.GetFileReference(relativePath), relativePath);

        public IEnumerable<ITransferInfo> EnumerateEntries()
        {
            var directories = new Stack<CloudFileDirectory>();
            directories.Push(_cloudDirectory);
            while (0 != directories.Count)
            {
                var directory = directories.Pop();
                FileContinuationToken continuationToken = null;
                do
                {
                    var resultSegment = directory.ListFilesAndDirectoriesSegmentedAsync(
                        MaxBatchResults,
                        continuationToken,
                        DefaultFileRequestOptions,
                        null,
                        CancellationToken.None).Result;

                    continuationToken = resultSegment.ContinuationToken;
                    foreach (var result in resultSegment.Results)
                    {
                        var relativePath = result.Uri.AbsoluteUri.Substring(_cloudDirectory.Uri.AbsoluteUri.Length).Replace('\\', '/').Trim('/');
                        var dir = result as CloudFileDirectory;
                        if (dir != null)
                        {
                            directories.Push(dir);
                            yield return new CloudFileDirectoryTransferInfo(dir, relativePath);
                        }
                        else
                        {
                            yield return new CloudFileTransferInfo((CloudFile)result, relativePath);
                        }
                    }
                }
                while (continuationToken != null);
            }
        }
    }
}
