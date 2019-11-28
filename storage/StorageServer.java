package storage;

import java.io.*;
import java.net.*;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import common.*;
import rmi.*;
import naming.*;

/** Storage server.

    <p>
    Storage servers respond to client file access requests. The files accessible
    through a storage server are those accessible under a given directory of the
    local filesystem.
 */ 
public class StorageServer implements Storage, Command
{
    private File root;
    private Storage client_stub;
    private Command command_stub;
    Skeleton<Command> cmdSkeleton;
    Skeleton<Storage> strgSkeleton;




    /** Creates a storage server, given a directory on the local filesystem.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
    */
    public StorageServer(File root)
    {
        if (null == root) {
            throw new NullPointerException("root parameter is null");
        }
        this.root = root.getAbsoluteFile();
    }

    /** Starts the storage server and registers it with the given naming
        server.

        @param hostname The externally-routable hostname of the local host on
                        which the storage server is running. This is used to
                        ensure that the stub which is provided to the naming
                        server by the <code>start</code> method carries the
                        externally visible hostname or address of this storage
                        server.
        @param naming_server Remote interface for the naming server with which
                             the storage server is to register.
        @throws UnknownHostException If a stub cannot be created for the storage
                                     server because a valid address has not been
                                     assigned.
        @throws FileNotFoundException If the directory with which the server was
                                      created does not exist or is in fact a
                                      file.
        @throws RMIException If the storage server cannot be started, or if it
                             cannot be registered.
     */
    public synchronized void start(String hostname, Registration naming_server)
        throws RMIException, UnknownHostException, FileNotFoundException
    {
        List<java.nio.file.Path> paths = new ArrayList<java.nio.file.Path>();
        Path[] files = null;
        try {

            paths.addAll(Files.find(Paths.get(root.getAbsolutePath()),Integer.MAX_VALUE,(filePath,fileAttr)->fileAttr.isRegularFile()).collect(Collectors.toList()));
            //paths.addAll(Files.find(Paths.get(root.getAbsolutePath()),Integer.MAX_VALUE,(filePath,fileAttr)->fileAttr.isDirectory()).collect(Collectors.toList()));

            files = new Path[paths.size()];
            for (int i = 0; i < paths.size(); i++) {
                files[i] = new Path(paths.get(i).toString().replace(root.getAbsolutePath(),"/"));
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        // Initialize client and command stubs
        InetSocketAddress cmdAddr = new InetSocketAddress(hostname,StorageStubs.COMMAND_PORT);
        cmdSkeleton = new Skeleton<Command>(Command.class, this, cmdAddr);
        try {
            cmdSkeleton.start();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        command_stub = StorageStubs.command(hostname);

        // Storage stub
        InetSocketAddress strgAddr = new InetSocketAddress(hostname,
                StorageStubs.STORAGE_PORT);
        strgSkeleton = new Skeleton<Storage>(Storage.class, this, strgAddr);
        try {
            strgSkeleton.start();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        client_stub = StorageStubs.storage(hostname);

        // register using naming_server, client_stub, command_stub
        try {
            Path[] filesToDelete = naming_server.register(client_stub,command_stub, files);
            if (filesToDelete != null) {
                for (Path file : filesToDelete) {
                    delete(file);
                }
            }
        } catch (RMIException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        // Prune this storage server directories

        pruneLocalStorage(Paths.get(root.getAbsolutePath()));

    }

    /** Stops the storage server.

        <p>
        The server should not be restarted.
     */
    public void stop()
    {
        cmdSkeleton.stop();
        strgSkeleton.stop();
    }

    /** Called when the storage server has shut down.

        @param cause The cause for the shutdown, if any, or <code>null</code> if
                     the server was shut down by the user's request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following methods are documented in Storage.java.
    @Override
    public synchronized long size(Path file) throws FileNotFoundException
    {
        if(!file.toString().contains(root.getAbsolutePath()))
            file=new Path(root.getAbsolutePath()+file.toString());
        File f = new File(file.toString());
        if (!f.exists()) {
            throw new FileNotFoundException("Not Found");
        }
        if(Files.isDirectory(Paths.get(file.toString())))
        {
            throw new FileNotFoundException("Dir no size");
        }
        return f.length();
    }

    @Override
    public synchronized byte[] read(Path file, long offset, int length)
        throws FileNotFoundException, IOException
    {
        if(!file.toString().contains(root.getAbsolutePath()))
            file=new Path(root.getAbsolutePath()+file.toString());
        File f = new File(file.toString());
        if (!f.exists()) {
            throw new FileNotFoundException("not implemented");
        }
        DataInputStream in = new DataInputStream(new FileInputStream(f));
        byte fileContent[] = new byte[(int) f.length()];
        // REVISIT TYPE CAST
        in.read(fileContent, (int) offset, length);
        in.close();
        return fileContent;
    }

    @Override
    public synchronized void write(Path file, long offset, byte[] data)
        throws FileNotFoundException, IOException
    {
        if(data==null)
            throw new NullPointerException("nothing to write");
        if(offset<0)
            throw new IndexOutOfBoundsException("offset out of bound");


        if(!file.toString().contains(root.getAbsolutePath()))
            file=new Path(root.getAbsolutePath()+file.toString());

        File f = new File(file.toString());
        if (!f.exists()) {
            throw new FileNotFoundException("not implemented");
        }

        DataOutputStream out = new DataOutputStream(new FileOutputStream(f,true));

        if(offset>out.size()) {
            long count = offset - out.size();
            out.write(" ".getBytes());
        }
        out.write(data);
        out.flush();
        out.close();
    }

    // The following methods are documented in Command.java.
    @Override
    public synchronized boolean create(Path file)
    {
        if(!file.toString().contains(root.getAbsolutePath()))
            file=new Path(root.getAbsolutePath()+file.toString());
        java.nio.file.Path p = Paths.get(file.toString());

        if(Files.exists(Paths.get(file.toString())))
            return false;

        try {
            Files.createDirectories(Paths.get(file.toString()).getParent());
            Files.createFile(Paths.get(file.toString()));
            OutputStream out = Files.newOutputStream(p);
            out.close();
            return true;
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public synchronized boolean delete(Path path)
    {
        try {
            if(path.toString().equals("/"))
                return false;

            if(!path.toString().contains(root.getAbsolutePath()))
                path=new Path(root.getAbsolutePath()+path.toString());
            if(!Files.exists(Paths.get(path.toString())))
                return false;
            if(Files.isDirectory(Paths.get(path.toString()))) {
                Iterator<java.nio.file.Path> it=Files.newDirectoryStream(Paths.get(path.toString())).iterator();
                while(it.hasNext())
                    delete(new Path(it.next().toString()));
            }
                Files.delete(Paths.get(path.toString()));
            return true;
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    private void pruneLocalStorage(java.nio.file.Path dir) {
        if(!Files.exists(dir))
            return;
        try (DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(dir)) {
            Iterator<java.nio.file.Path> it = stream.iterator();
            if (!it.hasNext()) {
                // Directory is empty, delete it
                File currentDir = new File(dir.toString());
                currentDir.delete();
                return;
            }

            while (it.hasNext()) {
                java.nio.file.Path file = it.next();
                if (Files.isDirectory(file)) {
                    pruneLocalStorage(file);
                    pruneLocalStorage(file);
                }
            }

        } catch (IOException | DirectoryIteratorException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }
}
