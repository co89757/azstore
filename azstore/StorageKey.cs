namespace azstore
{
    public abstract class StorageKey
    {
        public abstract string ContainerName { get; }
        public abstract string BlobName { get; }
    }
}