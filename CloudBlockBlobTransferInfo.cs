using System.IO;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzureCopy
{
    public class CloudBlockBlobTransferInfo : TransferInfo, IFileTransferInfo
    {
        private readonly CloudBlockBlob _blob;
        private bool _attributesFetched;

        public CloudBlockBlobTransferInfo(CloudBlockBlob blob, string relativePath) : base(blob.Uri.AbsoluteUri, relativePath)
        {
            _blob = blob;
        }

        public override bool Exists => _blob.Exists();
        
        public long Length => FetchProperties().Length;
        
        public string MD5Hash => FetchProperties().ContentMD5;
        
        public Stream ReadStream() => _blob.OpenRead();

        public void WriteStream(Stream stream)
        {
            _attributesFetched = false;
            _blob.UploadFromStream(stream, options: DefaultBlobRequestOptions);
        }

        private BlobProperties FetchProperties()
        {
            if (!_attributesFetched)
            {
                if (Exists)
                {
                    _blob.FetchAttributes();
                }

                _attributesFetched = true;
            }

            return _blob.Properties;
        }
    }
}
