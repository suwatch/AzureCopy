namespace AzureCopy
{
    public interface ITransferInfo
    {
        string RelativePath { get; }
        string FullName { get; }
        bool Exists { get; }
    }
}
