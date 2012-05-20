package org.hadoop.sbu.util;

import java.util.List;
import java.util.Set;

/* 
 * Navigate odd alternating cycles in the alternating forest.  
 */
final public class Blossom<T> {
    public final T root; // The root of the blossom; also the representative
    public final List<T> cycle; // The nodes, listed in order around the cycle
    public final Set<T> nodes; // The nodes, stored in a set for efficient lookup

    /**
     * Construct a blossom
     *
     * @param root The root node of the blossom.
     * @param cycle The nodes of the cycle listed in the order in which
     *              they appear in the blossom.
     * @param nodes The nodes in the cycle, stored as a set.
     */
    public Blossom(T root, List<T> cycle, Set<T> nodes) {
        this.root = root;
        this.cycle = cycle;
        this.nodes = nodes;
    }
}
