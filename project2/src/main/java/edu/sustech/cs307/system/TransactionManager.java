package edu.sustech.cs307.system;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;


public class TransactionManager {

    private final DBManager dbManager;
    private Path transactionSnapshot;
    private final List<SavepointEntry> savepoints = new ArrayList<>();

    private record SavepointEntry(String name, Path snapshot) {
    }


    public TransactionManager(DBManager dbManager) {
        this.dbManager = dbManager;
    }


    public void begin() throws DBException {
        if (transactionSnapshot != null) {
            throw new DBException(ExceptionTypes.TransactionAlreadyActive());
        }
        transactionSnapshot = createSnapshot();
        savepoints.clear();
    }


    public void commit() throws DBException {
        if (transactionSnapshot == null) {
            return;
        }
        dbManager.persistRuntimeState();
        cleanupTransactionState();
    }


    public void rollback() throws DBException {
        if (transactionSnapshot == null) {
            return;
        }
        restoreSnapshot(transactionSnapshot);
        cleanupTransactionState();
    }


    public void savepoint(String savepointName) throws DBException {
        ensureTransactionActive();
        savepoints.add(new SavepointEntry(savepointName, createSnapshot()));
    }


    public void rollbackToSavepoint(String savepointName) throws DBException {
        ensureTransactionActive();
        int index = findLatestSavepoint(savepointName);
        if (index < 0) {
            throw new DBException(ExceptionTypes.SavepointDoesNotExist(savepointName));
        }
        SavepointEntry target = savepoints.get(index);
        restoreSnapshot(target.snapshot());
        deleteSavepointsAfter(index);
    }


    public void releaseSavepoint(String savepointName) throws DBException {
        ensureTransactionActive();
        int index = findLatestSavepoint(savepointName);
        if (index < 0) {
            throw new DBException(ExceptionTypes.SavepointDoesNotExist(savepointName));
        }
        deleteDirectory(savepoints.remove(index).snapshot());
    }

    private Path createSnapshot() throws DBException {
        dbManager.persistRuntimeState();
        Path snapshotDir;
        try {
            snapshotDir = Files.createTempDirectory("cs307-txn-");
            copyDirectoryContents(getDbRoot(), snapshotDir);
        } catch (IOException e) {
            throw new DBException(ExceptionTypes.BadIOError(e.getMessage()));
        }
        return snapshotDir;
    }

    private Path getDbRoot() {
        return Path.of(dbManager.getDiskManager().getCurrentDir());
    }

    private void restoreSnapshot(Path snapshot) throws DBException {
        try {
            Path dbRoot = getDbRoot();
            if (Files.exists(dbRoot)) {
                deleteDirectoryContents(dbRoot);
            }
            Files.createDirectories(dbRoot);
            copyDirectoryContents(snapshot, dbRoot);
            dbManager.reloadRuntimeState();
        } catch (IOException e) {
            throw new DBException(ExceptionTypes.BadIOError(e.getMessage()));
        }
    }

    private void ensureTransactionActive() throws DBException {
        if (transactionSnapshot == null) {
            throw new DBException(ExceptionTypes.TransactionRequired());
        }
    }

    private int findLatestSavepoint(String savepointName) {
        for (int i = savepoints.size() - 1; i >= 0; i--) {
            if (savepoints.get(i).name().equals(savepointName)) {
                return i;
            }
        }
        return -1;
    }

    private void deleteSavepointsAfter(int index) throws DBException {
        for (int i = savepoints.size() - 1; i > index; i--) {
            deleteDirectory(savepoints.remove(i).snapshot());
        }
    }

    private void cleanupTransactionState() throws DBException {
        deleteDirectory(transactionSnapshot);
        transactionSnapshot = null;
        for (SavepointEntry savepoint : savepoints) {
            deleteDirectory(savepoint.snapshot());
        }
        savepoints.clear();
    }

    private void copyDirectoryContents(Path sourceRoot, Path targetRoot) throws IOException {
        if (!Files.exists(sourceRoot)) {
            Files.createDirectories(targetRoot);
            return;
        }
        Files.createDirectories(targetRoot);
        try (var paths = Files.walk(sourceRoot)) {
            for (Path source : paths.toList()) {
                Path relative = sourceRoot.relativize(source);
                Path target = targetRoot.resolve(relative);
                if (Files.isDirectory(source)) {
                    Files.createDirectories(target);
                } else {
                    Files.createDirectories(target.getParent());
                    Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
                }
            }
        }
    }

    private void deleteDirectoryContents(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted((left, right) -> right.getNameCount() - left.getNameCount()).toList()) {
                if (!path.equals(root)) {
                    Files.deleteIfExists(path);
                }
            }
        }
    }

    private void deleteDirectory(Path root) throws DBException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try {
            deleteDirectoryContents(root);
            Files.deleteIfExists(root);
        } catch (IOException e) {
            throw new DBException(ExceptionTypes.BadIOError(e.getMessage()));
        }
    }
}
