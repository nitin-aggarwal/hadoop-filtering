package org.hadoop.sbu.util;

/* A utility struct representing an edge in the graph. */
public final class Edge<T> {
    public final T start;
    public final T end;

    /**
     * Constructs a new edge between the two indicated endpoints.
     *
     * @param start The edge's starting point.
     * @param end The edge's endpoint.
     */
    public Edge(T start, T end) {
        this.start = start;
        this.end = end;
    }
}
