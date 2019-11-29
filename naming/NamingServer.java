package naming;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import rmi.*;
import common.*;
import storage.*;

/** Naming server. 

    <p>
    Each instance of the filesystem is centered on a single naming server. The
    naming server maintains the filesystem directory tree. It does not store any
    file data - this is done by separate storage servers. The primary purpose of
    the naming server is to map each file name (path) to the storage server
    which hosts the file's contents.

    <p>
    The naming server provides two interfaces, <code>Service</code> and
    <code>Registration</code>, which are accessible through RMI. Storage servers
    use the <code>Registration</code> interface to inform the naming server of
    their existence. Clients use the <code>Service</code> interface to perform
    most filesystem operations. The documentation accompanying these interfaces
    provides details on the methods supported.

    <p>
    Stubs for accessing the naming server must typically be created by directly
    specifying the remote network address. To make this possible, the client and
    registration interfaces are available at well-known ports defined in
    <code>NamingStubs</code>.
 */
public class NamingServer implements Service, Registration
{

    Set<Path> serverfiles;
    Map<String, List<Storage>> clientStubsForFile;
    Map<String, List<Command>> commandStubsForFile;
    Set<Map<String, Object>> storageServerStubs;
    Directory directoryTree;

    String rootDirName = "data";

    Registration regStub;
    Service servStub;
    Skeleton<Registration> regSkeleton;
    Skeleton<Service> servSkeleton;

    /** Creates the naming server object.

        <p>
        The naming server is not started.
     */
    public NamingServer()
    {
        this.serverfiles = new HashSet<Path>();
        this.clientStubsForFile = new HashMap<String, List<Storage>>();
        this.commandStubsForFile = new HashMap<String, List<Command>>();
        this.directoryTree = new Directory(rootDirName, new Hashtable<String, Directory>(), new HashSet<String>());
        this.storageServerStubs = new HashSet<Map<String, Object>>();
    }

    /** Starts the naming server.

        <p>
        After this method is called, it is possible to access the client and
        registration interfaces of the naming server remotely.

        @throws RMIException If either of the two skeletons, for the client or
                             registration server interfaces, could not be
                             started. The user should not attempt to start the
                             server again if an exception occurs.
     */
    public synchronized void start() throws RMIException
    {
        //this.namingListeners = new NamingListener(this);
        String hostname = "127.0.0.1";

        // Create registration stub
        InetSocketAddress regAddr = new InetSocketAddress(hostname,NamingStubs.REGISTRATION_PORT);
        regSkeleton = new Skeleton<Registration>(Registration.class,this, regAddr);
        try {

            regSkeleton.start();
        } catch (RMIException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        regStub = NamingStubs.registration(hostname);

        // Create service stub
        InetSocketAddress servAddr = new InetSocketAddress(hostname,NamingStubs.SERVICE_PORT);
        servSkeleton = new Skeleton<Service>(Service.class, this,servAddr);
        try {
            servSkeleton.start();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        servStub = NamingStubs.service(hostname);

    }

    /** Stops the naming server.

        <p>
        This method waits for both the client and registration interface
        skeletons to stop. It attempts to interrupt as many of the threads that
        are executing naming server code as possible. After this method is
        called, the naming server is no longer accessible remotely. The naming
        server should not be restarted.
     */
    public void stop()
    {
        if(regSkeleton!=null)
            regSkeleton.stop();
        if(servSkeleton!=null)
            servSkeleton.stop();
        stopped(null);
    }

    /** Indicates that the server has completely shut down.

        <p>
        This method should be overridden for error reporting and application
        exit purposes. The default implementation does nothing.

        @param cause The cause for the shutdown, or <code>null</code> if the
                     shutdown was by explicit user request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following methods are documented in Service.java.
    @Override
    public boolean isDirectory(Path path) throws FileNotFoundException
    {
        if(path==null)
            throw new NullPointerException("path is null");
        if(path.toString().equals("/"))
            return true;

        java.nio.file.Path dir = Paths.get(path.toString());

        int length = dir.getNameCount();

        Hashtable<String, Directory> currentDirs = directoryTree.getSubDirs();
        for (int i = 0; i < length; i++) {
            if (!currentDirs.keySet().contains(dir.getName(i).toString())) {
                if (i == length ) {
                    return false;
                } else {
                    Directory d=currentDirs.get(dir.getName(i).toString());
                    if(d.getFiles().contains(dir.getName(i).toString())){
                        return false;
                    }
                    throw new FileNotFoundException("Invalid directory path");
                }
            }
            currentDirs = currentDirs.get(dir.getName(i).toString())
                    .getSubDirs();
        }
        return true;
    }

    @Override
    public String[] list(Path directory) throws FileNotFoundException
    {
        if(directory==null)
            throw new NullPointerException("directory is null");
        java.nio.file.Path dir = Paths.get(directory.toString());
        HashSet<String> files = null;

        String dirName=dir.toString().equals("/")?rootDirName:dir.getName(0).toString();
        // Validate root directory
        if (!dirName.equals(directoryTree.getName())) {
            throw new FileNotFoundException("Invalid directory path");
        }

        int length = dir.getNameCount();
        if (dir.toString().equals("/")) {
            // Since length is 1, the directory is root directory
            // Root directory is already validated above
            files = directoryTree.getFiles();
        }

        Hashtable<String, Directory> currentDirs = directoryTree.getSubDirs();
        for (int i = 0; i < length; i++) {

            if (!currentDirs.keySet().contains(dir.getName(i).toString())) {
                throw new FileNotFoundException("Invalid directory path");
            }
            if (i == length - 1) {
                files = currentDirs
                        .get(dir.getName(dir.getNameCount() - 1).toString())
                        .getFiles();
            }
            currentDirs = currentDirs.get(dir.getName(i).toString())
                    .getSubDirs();
        }

        files.addAll(currentDirs.keySet());
        return files.toArray(new String[files.size()]);
    }

    @Override
    public boolean createFile(Path file) throws RMIException, FileNotFoundException
    {
        Command cmd_stub = null;
        Storage clnt_stub = null;
        synchronized (storageServerStubs) {
            java.nio.file.Path path = Paths.get(file.toString());
            if (!serverfiles.contains(path.subpath(0, path.getNameCount() - 1))) {
                throw new FileNotFoundException(
                        "Parent directory does not exist");
            }

            Long serverSize = 0L;
            Iterator<Map<String, Object>> iter = storageServerStubs.iterator();
            while (iter.hasNext()) {
                Map<String, Object> currObj = (Map<String, Object>) iter.next();
                Long currServerSize = (Long) currObj.get("size");
                if (currServerSize > serverSize) {
                    serverSize = (Long) currObj.get("size");
                    cmd_stub = (Command) currObj.get("command_stub");
                    clnt_stub = (Storage) currObj.get("client_stub");
                }
            }
        }

        boolean isFileCreated = cmd_stub.create(file);

        if (isFileCreated) {
            HashSet<Path> fileSet = new HashSet<Path>();
            fileSet.add(file);
            serverfiles.add(file);
            addFilesToDirectoryTree(fileSet, clnt_stub, cmd_stub);
        }
        return isFileCreated;
    }



    @Override
    public boolean createDirectory(Path directory) throws FileNotFoundException, RMIException {
        Command cmd_stub = null;
        Storage clnt_stub = null;
        synchronized (storageServerStubs) {
            java.nio.file.Path path = Paths.get(directory.toString());
            if (!serverfiles
                    .contains(path.subpath(0, path.getNameCount() - 1))) {
                throw new FileNotFoundException(
                        "Parent directory does not exist");
            }
            Long serverSize = 0L;
            Iterator<Map<String, Object>> iter = storageServerStubs.iterator();
            while (iter.hasNext()) {
                Map<String, Object> currObj = (Map<String, Object>) iter.next();
                Long currServerSize = (Long) currObj.get("size");
                if (currServerSize > serverSize) {
                    serverSize = (Long) currObj.get("size");
                    cmd_stub = (Command) currObj.get("command_stub");
                    clnt_stub = (Storage) currObj.get("client_stub");
                }
            }
        }
        boolean isDirCreated = cmd_stub.create(directory);

        if (isDirCreated) {
            HashSet<Path> dirSet = new HashSet<Path>();
            dirSet.add(directory);
            serverfiles.add(directory);
            addDirectoriesToDirectoryTree(dirSet, clnt_stub, cmd_stub);
        }
        return isDirCreated;
    }



    @Override
    public boolean delete(Path path) throws FileNotFoundException
    {
        if (!serverfiles.contains(path.toString())) {
            throw new FileNotFoundException();
        }
        boolean isDeleted = false;
        try {

            // Delete from all the nodes
            for (int i = 0; i < commandStubsForFile.get(path.toString())
                    .size(); i++) {
                isDeleted = commandStubsForFile.get(path.toString()).get(i)
                        .delete(path);
                if (!isDeleted) {
                    return isDeleted;
                }
            }

            // To remove from naming server

            // Remove from directory tree
            boolean deletedFromTree = false;
            Directory parentDir = getParentDir(path);
            String fileName = Paths.get(path.toString()).getFileName()
                    .toString();
            for (String file : parentDir.getFiles()) {
                if (file.equals(fileName)) {
                    parentDir.getFiles().remove(file);
                    deletedFromTree = true;
                    break;
                }
            }
            if (!deletedFromTree) {
                for (String dir : parentDir.getSubDirs().keySet()) {
                    if (dir.equals(fileName)) {
                        parentDir.getSubDirs().remove(fileName);
                    }
                }
            }
            // Remove from file list
            serverfiles.remove(path.toString());
            // Delete from stub command stub list
            commandStubsForFile.remove(path.toString());
            // Delete from stub storage stub list
            clientStubsForFile.remove(path.toString());
            isDeleted = true;
        } catch (RMIException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            isDeleted = false;
        }
        return isDeleted;
    }

    @Override
    public Storage getStorage(Path file) throws FileNotFoundException
    {
        boolean found=false;
        Iterator<Path> it=serverfiles.iterator();
        while(it.hasNext()){
            Path i=it.next();
            if(i.equals(file)){
                found=true;
                break;
            }
        }
        if (!found) {
            throw new FileNotFoundException();
        }
        return clientStubsForFile.get(file.toString()).get(0);
    }

    // The method register is documented in Registration.java.
    @Override
    public Path[] register(Storage client_stub, Command command_stub, Path[] files)
    {
        if(client_stub==null)
            throw new NullPointerException("client stub null");
        if(command_stub==null)
            throw new NullPointerException("command stub null");
        if(files==null)
            throw new NullPointerException("files null");

        Map<String, Object> stubs = new HashMap<String, Object>();
        stubs.put("command_stub", command_stub);
        stubs.put("client_stub", client_stub);

        for (Map<String, Object> storageServerStub : storageServerStubs) {
            if(storageServerStub.get("client_stub").equals(client_stub)
                && storageServerStub.get("command_stub").equals(command_stub))
                throw new IllegalStateException("server already registered");
        }
        //if(this.storageServerStubs.contains(stubs))
        //  throw new IllegalStateException("server already registered");
        storageServerStubs.add(stubs);
        /*
        // Store the stubs for server
        try {
            stubs.put("size", client_stub.size(new Path("/data")));

        } catch (FileNotFoundException | RMIException e) {
            System.out.println("Registration failed : " + e.getMessage());
            e.printStackTrace();
        }
        */

        Set<Path> del=new HashSet<>();
        Set<Path> add=new HashSet<>();
        // Add files to directory tree
        synchronized (serverfiles) {
            synchronized (directoryTree){
                for(Path file2:files){
                    if(file2.equals("/"))
                        continue;
                    if(serverfiles.contains(file2)){
                        del.add(file2);
                    }else {
                        serverfiles.forEach(x -> {
                            if (x.toString().startsWith(file2.toString())) {
                                del.add(file2);
                            }
                        });
                    }
                }
            }
            serverfiles.addAll(Arrays.asList(files).stream().filter(x->!x.equals("/")).collect(Collectors.toList()));

            addFilesToDirectoryTree(serverfiles, client_stub, command_stub);
        }
        Path[] result=new Path[del.size()];
        for (int i = 0; i < result.length; i++) {
            result[i]=del.stream().collect(Collectors.toList()).get(i);
        }

        return result;
    }

    public boolean addFilesToDirectoryTree(Set<Path> files, Storage client_stub,
                                           Command command_stub) {
        try {
            Directory currentDir;
            synchronized (directoryTree) {
                for (Path file : files) {
                    currentDir = directoryTree;
                    java.nio.file.Path path = Paths.get(file.toString());

                    if (!currentDir.getFiles()
                            .contains(path.getFileName().toString())) {
                        // If not present in root directory,
                        // iterate over sub-directories for current file
                        for (int i = 0; i < path.getNameCount() - 1; i++) {

                            if (!currentDir.getSubDirs().keySet()
                                    .contains(path.getName(i).toString())) {
                                Directory newDir = new Directory(
                                        path.getName(i).toString(),
                                        new Hashtable<String, Directory>(),
                                        new HashSet<String>());
                                currentDir.getSubDirs().put(
                                        path.getName(i).toString(), newDir);

                                String currSubPath = null;
                                if (i == 0) {
                                    currSubPath = path.toString();
                                } else {
                                    currSubPath = path.subpath(0, i).toString();
                                }

                                // Add storage stubs for the directory
                                if (!commandStubsForFile
                                        .containsKey(currSubPath)) {
                                    commandStubsForFile.put(currSubPath,
                                            new ArrayList<Command>());
                                }
                                commandStubsForFile.get(currSubPath)
                                        .add(command_stub);

                                if (!clientStubsForFile
                                        .containsKey(currSubPath)) {
                                    clientStubsForFile.put(currSubPath,
                                            new ArrayList<Storage>());
                                }
                                clientStubsForFile.get(currSubPath)
                                        .add(client_stub);

                            }
                            currentDir = currentDir.getSubDirs()
                                    .get(path.getName(i).toString());
                        }
                        currentDir.getFiles().add(path
                                .getName(path.getNameCount() - 1).toString());
                    }

                    // Add storage stubs for the file
                    if (!commandStubsForFile.containsKey(file.toString())) {
                        commandStubsForFile.put(file.toString(),
                                new ArrayList<Command>());
                    }
                    commandStubsForFile.get(file.toString()).add(command_stub);

                    if (!clientStubsForFile.containsKey(file.toString())) {
                        clientStubsForFile.put(file.toString(),
                                new ArrayList<Storage>());
                    }
                    clientStubsForFile.get(file.toString()).add(client_stub);
                }
                return true;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    private boolean addDirectoriesToDirectoryTree(HashSet<Path> dirSet,
                                                  Storage client_stub, Command command_stub) {
        try {
            Directory currentDir;
            synchronized (directoryTree) {
                for (Path dir : dirSet) {
                    currentDir = directoryTree;
                    java.nio.file.Path path = Paths.get(dir.toString());

                    if (!currentDir.getSubDirs().keySet()
                            .contains(path.getFileName().toString())) {
                        // If not present in root directory,
                        // iterate over sub-directories for current dir
                        for (int i = 0; i < path.getNameCount() - 1; i++) {
                            currentDir = currentDir.getSubDirs()
                                    .get(path.getName(i).toString());
                        }
                        currentDir.getFiles().add(path
                                .getName(path.getNameCount() - 1).toString());
                        currentDir.getSubDirs().put(
                                path.getName(path.getNameCount() - 1)
                                        .toString(),
                                new Directory(
                                        path.getName(path.getNameCount() - 1)
                                                .toString(),
                                        new Hashtable<String, Directory>(),
                                        new HashSet<String>()));
                    }

                    // Add storage stubs for the dir
                    if (!commandStubsForFile.containsKey(dir.toString())) {
                        commandStubsForFile.put(dir.toString(),
                                new ArrayList<Command>());
                    }
                    commandStubsForFile.get(dir.toString()).add(command_stub);

                    if (!clientStubsForFile.containsKey(dir.toString())) {
                        clientStubsForFile.put(dir.toString(),
                                new ArrayList<Storage>());
                    }
                    clientStubsForFile.get(dir.toString()).add(client_stub);
                }
                return true;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    private Directory getParentDir(Path path) {
        java.nio.file.Path dir = Paths.get(path.toString());
        int pathLength = dir.getNameCount();
        Hashtable<String, Directory> currentDirs = directoryTree.getSubDirs();
        for (int i = 0; i < pathLength; i++) {
            if (i == pathLength - 2) {
                return currentDirs.get(dir.getName(i).toString());
            }

            currentDirs = currentDirs.get(dir.getName(i).toString())
                    .getSubDirs();
        }
        return null;
    }

}
