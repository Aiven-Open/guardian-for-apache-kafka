# Design

Storage mechanisms are implemented via the @apidoc[BackupClientInterface] and @apidoc[RestoreClientInterface]. To add
custom storage mechanisms you need to implement these methods. These interfaces are designed to be as simple as possible
while being completely abstract to allow for any theoretical storage mechanism.

## BackupClientInterface

The @apidoc[BackupClientInterface] implements the entire backup flow including the resuming from a previously terminated
backup. Of note is the @apidoc[BackupClientInterface.State](BackupClientInterface) which is the data structure that is
returned when any previously existing backup for that key exists. This is provided to
@apidoc[BackupClientInterface.backupToStorageSink](BackupClientInterface) indicating whether the backup being performed
is a new backup or resuming from a previous one with the retrieval of the current state being defined by
@apidoc[BackupClientInterface.getCurrentUploadState](BackupClientInterface).

Note that when implementing @apidoc[BackupClientInterface] you do not need to handle the corner cases regarding the
contents of the byte string when resuming/suspending/terminating, this is automatically handled for you. Essentially you
just need to handle how to store/push `ByteString` into the storage of your choice.

## RestoreClientInterface

The @apidoc[RestoreClientInterface] implements restoration from an existing backup. Implementing this is quite simple,
you need to define @apidoc[RestoreClientInterface.retrieveBackupKeys](RestoreClientInterface) which returns all valid
keys to restore (i.e. don't include currently in progress backup keys) and
@apidoc[RestoreClientInterface.downloadFlow](RestoreClientInterface) which is an akka-stream `Flow` that takes
a `String` which is the key and outputs the content of that key.
