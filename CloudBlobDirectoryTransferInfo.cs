using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzureCopy
{
    public class CloudBlobDirectoryTransferInfo : TransferInfo, IDirectoryTransferInfo
    {
        private readonly CloudBlobDirectory _blobDirectory;

        public CloudBlobDirectoryTransferInfo(CloudBlobDirectory blobDirectory, string relativePath) : base(blobDirectory.Uri.AbsoluteUri, relativePath)
        {
            _blobDirectory = blobDirectory;
        }

        public override bool Exists => true;

        public IDirectoryTransferInfo CreateIfNotExists(string relativePath = null)
            => string.IsNullOrEmpty(relativePath) ? this : new CloudBlobDirectoryTransferInfo(_blobDirectory.GetDirectoryReference(relativePath), relativePath);

        public IFileTransferInfo GetChildEntry(string relativePath) 
            => new CloudBlockBlobTransferInfo(_blobDirectory.GetBlockBlobReference(relativePath), relativePath);

        public IEnumerable<ITransferInfo> EnumerateEntries()
        {
            var directories = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var container = _blobDirectory.Container;
            var patternPrefix = _blobDirectory.Prefix;

            BlobContinuationToken continuationToken = null;
            do
            {
                var resultSegment = container.ListBlobsSegmentedAsync(
                    patternPrefix,
                    true,
                    BlobListingDetails.Snapshots,
                    MaxBatchResults,
                    continuationToken,
                    DefaultBlobRequestOptions,
                    null,
                    CancellationToken.None).Result;

                continuationToken = resultSegment.ContinuationToken;
                foreach (var result in resultSegment.Results)
                {
                    var relativePath = result.Uri.AbsoluteUri.Substring(_blobDirectory.Uri.AbsoluteUri.Length).Replace('\\', '/').Trim('/');

                    // create cloud blob directory for path
                    var parts = relativePath.Split('/');
                    var currentRelativePath = string.Empty;
                    for (int i = 0; i < parts.Length - 1; i++)
                    {
                        currentRelativePath = string.IsNullOrEmpty(currentRelativePath) ? parts[i] : $"{currentRelativePath}/{parts[i]}";
                        if (!directories.Contains(currentRelativePath))
                        {
                            directories.Add(currentRelativePath);
                            yield return new CloudBlobDirectoryTransferInfo(_blobDirectory.GetDirectoryReference(currentRelativePath), currentRelativePath);
                        }
                    }

                    if (result is CloudBlockBlob blob)
                    {
                        yield return new CloudBlockBlobTransferInfo((CloudBlockBlob)result, relativePath);
                    }
                    else
                    {
                        throw new InvalidOperationException($"Blob type '{result.GetType().Name}' is not supported!");
                    }
                }
            }
            while (continuationToken != null);
        }
    }
}
