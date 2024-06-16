using System.IO;

namespace AzureCopy
{
    public interface IFileTransferInfo : ITransferInfo
    {
        string MD5Hash { get; }
        long Length { get; }
        Stream ReadStream();
        void WriteStream(Stream stream);
    }
}
