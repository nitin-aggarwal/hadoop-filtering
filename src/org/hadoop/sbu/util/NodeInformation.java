package org.hadoop.sbu.util;

/* 
 * Store node related information
 */
 final public class NodeInformation<T> {
    public final T parent;
    public final T treeRoot;
    public final boolean isOuter; // True for outer node, false for inner.

    /**
     * Constructs a new NodeInformation wrapping the indicated data.
     *
     * @param parent The parent of the given node.
     * @param treeRoot The root of the given node.
     * @param isOuter Whether the given node is an outer node.
     */
    public NodeInformation(T parent, T treeRoot, boolean isOuter) {
        this.parent = parent;
        this.treeRoot = treeRoot;
        this.isOuter = isOuter;
    }
}