package common;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.*;

/** Distributed filesystem paths.

    <p> 
    Objects of type <code>Path</code> are used by all filesystem interfaces.
    Path objects are immutable.

    <p>
    The string representation of paths is a forward-slash-delimeted sequence of
    path components. The root directory is represented as a single forward
    slash.

    <p>
    The colon (<code>:</code>) and forward slash (<code>/</code>) characters are
    not permitted within path components. The forward slash is the delimeter,
    and the colon is reserved as a delimeter for application use.
 */
public class Path implements Iterable<String>, Serializable
{
    private static final long serialVersionUID = 8909040966566983449L;
    String path;
    String root = "/";

    /** Creates a new path which represents the root directory. */
    public Path()
    {
        path = Paths.get(root).normalize().toString();
    }

    /** Creates a new path by appending the given component to an existing path.

        @param path The existing path.
        @param component The new component.
        @throws IllegalArgumentException If <code>component</code> includes the
                                         separator, a colon, or
                                         <code>component</code> is the empty
                                         string.
    */
    public Path(Path path, String component)
    {
        if (component == null | component.length() == 0) {
            throw new IllegalArgumentException("component is null or empty");
        }
        if (component.contains(":") | component.contains("/")) {
            throw new IllegalArgumentException("component contains : or /");
        }
        this.path = Paths.get(path.toString(), component).normalize()
                .toString();
    }

    /** Creates a new path from a path string.

        <p>
        The string is a sequence of components delimited with forward slashes.
        Empty components are dropped. The string must begin with a forward
        slash.

        @param path The path string.
        @throws IllegalArgumentException If the path string does not begin with
                                         a forward slash, or if the path
                                         contains a colon character.
     */
    public Path(String path)
    {

        if (path == null | path.length() == 0) {
            throw new IllegalArgumentException("path is null or empty");
        }
        if (path.contains(":") | path.charAt(0) != '/') {
            throw new IllegalArgumentException(
                    "component contains : or does not start with /");
        }
        this.path = Paths.get(path).normalize().toString();
    }

    /** Returns an iterator over the components of the path.

        <p>
        The iterator cannot be used to modify the path object - the
        <code>remove</code> method is not supported.

        @return The iterator.
     */
    @Override
    public Iterator<String> iterator()
    {

        java.nio.file.Path path = Paths.get(this.path).normalize();
        Iterator<java.nio.file.Path> pathIterator = path.iterator();
        List<String> myPathIterator = new ArrayList<String>();

        while (pathIterator.hasNext()) {
            myPathIterator.add(pathIterator.next().toString());
        }
        return new Iterator<String>() {
            private Iterator<String> i=myPathIterator.iterator();
            @Override
            public boolean hasNext() {
                return i.hasNext();
            }

            @Override
            public String next() {
                return i.next();
            }
            @Override
            public void remove(){
                throw new UnsupportedOperationException("iterator remove not supported");
            }
        };
    }

    /** Lists the paths of all files in a directory tree on the local
        filesystem.

        @param directory The root directory of the directory tree.
        @return An array of relative paths, one for each file in the directory
                tree.
        @throws FileNotFoundException If the root directory does not exist.
        @throws IllegalArgumentException If <code>directory</code> exists but
                                         does not refer to a directory.
     */
    public static Path[] list(File directory) throws FileNotFoundException
    {
        if (!Files.exists(Paths.get(directory.getAbsolutePath()))) {
            throw new FileNotFoundException();
        }
        if (!new File(directory.getAbsolutePath()).isDirectory()) {
            throw new IllegalArgumentException("parameter directory is not a directory");
        }

        
        ArrayList<common.Path> ls=new ArrayList<>();
        try {
            Files.walk(Paths.get(directory.getPath()))
                    .filter(Files::isRegularFile)
                    .forEach(x->{
                        ls.add(new Path(x.toString().replace(directory.getAbsolutePath(),"/")));
                    });
        }catch (IOException e){
            throw  new FileNotFoundException(e.getMessage());
        }
        Path[] list=new Path[ls.size()];
        for(int i=0;i<ls.size();i++){
            list[i]=ls.get(i);
        }
        return list;

    }

    /** Determines whether the path represents the root directory.

        @return <code>true</code> if the path does represent the root directory,
                and <code>false</code> if it does not.
     */
    public boolean isRoot()
    {
        return (Paths.get(this.path).normalize()
                .compareTo(Paths.get(root)) == 0);
    }

    /** Returns the path to the parent of this path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no parent.
     */
    public Path parent()
    {
        if (isRoot()) {
            throw new IllegalArgumentException(
                    "The path represents the root directory, and therefore has no parent");
        }
        return new Path(
                new File(this.path).getParentFile().toPath().toString());
    }

    /** Returns the last component in the path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no last
                                         component.
     */
    public String last()
    {
        if (Files.isDirectory(Paths.get(this.path).normalize())) {
            throw new IllegalArgumentException(
                    "The path represents the root directory, and therefore has no last component");
        }
        return Paths.get(this.path).getFileName().toString();
    }

    /** Determines if the given path is a subpath of this path.

        <p>
        The other path is a subpath of this path if is a prefix of this path.
        Note that by this definition, each path is a subpath of itself.

        @param other The path to be tested.
        @return <code>true</code> If and only if the other path is a subpath of
                this path.
     */
    public boolean isSubpath(Path other)
    {
        return Paths.get(this.path).normalize().toString()
                .contains(other.path);
    }

    /** Converts the path to <code>File</code> object.

        @param root The resulting <code>File</code> object is created relative
                    to this directory.
        @return The <code>File</code> object.
     */
    public File toFile(File root)
    {
        return new File(this.path.substring(this.path.indexOf(root.getPath())));
    }

    /** Compares two paths for equality.

        <p>
        Two paths are equal if they share all the same components.

        @param other The other path.
        @return <code>true</code> if and only if the two paths are equal.
     */
    @Override
    public boolean equals(Object other)
    {
        return this.path.equals( other.toString());
    }

    /** Returns the hash code of the path. */
    @Override
    public int hashCode()
    {
        return this.path.hashCode();
    }

    /** Converts the path to a string.

        <p>
        The string may later be used as an argument to the
        <code>Path(String)</code> constructor.

        @return The string representation of the path.
     */
    @Override
    public String toString()
    {
        return this.path;
    }
}
