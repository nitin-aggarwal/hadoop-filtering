package org.hadoop.sbu.graph.algo;
/******************************************************************************
 *
 * The algorithm can make at most O(n) iterations of finding alternating paths,
 * since a maximum matching has cardinality at most n/2.  During each iteration
 * of the tree-growing step, we need to consider each edge at most once, since
 * it either goes in the tree, finds an augmenting path, or creates a cycle.
 * Whenever a cycle is found, we can create the graph formed by contracting
 * that cycle in O(m) by scanning over the edges.  Finally, we can have at most
 * O(n) contractions, because each contraction removes at least one node.  This
 * gives the algorithm a runtime of O(n^2 m).
 */
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.hadoop.sbu.util.Blossom;
import org.hadoop.sbu.util.Edge;
import org.hadoop.sbu.util.NodeInformation;
import org.hadoop.sbu.util.UndirectedGraph;

public final class Edmonds {
    /**
     * Given an undirected graph, returns a graph containing the edges of a
     * maximum matching in that graph.
     *
     * @param g The graph in which a maximum matching should be found.
     * @return A graph containing a maximum matching in that graph.
     */
    public static <T> UndirectedGraph<T> maximumMatching(UndirectedGraph<T> g) {
        /* If the graph is empty, return an empty graph as
         * the matching.
         */
        if (g.isEmpty())
            return new UndirectedGraph<T>();

        /* Construct a new graph to hold the matching.  Fill it with the nodes
         * from the original graph, but not the edges.
         */
        UndirectedGraph<T> result = new UndirectedGraph<T>();
        for (T node: g)
            result.addNode(node);

        //find alternating paths
        while (true) {
            // if no path exists, we get final matching by Berge's theorem
             List<T> path = getAlternatingPath(g, result);
            if (path == null) return result;

            // If not, update the matching graph using this alternating path. 
            updateMatching(path, result);
        }
    }

    /**
     * Increasing the cardinality by flipping the edges in Maximum matching
     *
     * @param path The alternating path linking the exposed endpoints.
     * @param m The matching to update, which is an in/out parameter.
     */
    private static <T> void updateMatching(List<T> path, UndirectedGraph<T> m) {
          for (int i = 0; i < path.size() - 1; ++i) {
            //flipping the edges
            if (m.edgeExists(path.get(i), path.get(i + 1)))
                m.removeEdge(path.get(i), path.get(i + 1));
            else
                m.addEdge(path.get(i), path.get(i + 1));
        }
    }

    
    /**
     * Given a graph and a matching in that graph, returns an augmenting path
     * in the graph if one exists.
     *
     * @param g The graph in which to search for the path.
     * @param m A matching in that graph.
     * @return An alternating path in g, or null if none exists.
     */
    private static <T> List<T> getAlternatingPath(UndirectedGraph<T> g,
                                                   UndirectedGraph<T> m) {
    	//create forests consisting all information about nodes being considered
        Map<T, NodeInformation<T>> forest = new HashMap<T, NodeInformation<T>>();

        //explore the node in BFS manner and add all its outgoing edges to worklist queue
        Queue<Edge<T>> worklist = new LinkedList<Edge<T>>();

        
        for (T node: g) {
            //adding nodes having no outgoing edges in matching
        	if (!m.edgesFrom(node).isEmpty())
                continue;

            /* This node is an outer node that has no parent and belongs in
             * its own tree.
             */
            forest.put(node, new NodeInformation<T>(null, node, true));

            /* Add to the worklist all edges leaving this node. */
            for (T endpoint: g.edgesFrom(node))
                worklist.add(new Edge<T>(node, endpoint));
        }

        //start creating tree from worklist nodes
        while (!worklist.isEmpty()) {
            //considering edges not in matching already
            Edge<T> curr = worklist.remove();
            if (m.edgeExists(curr.start, curr.end))
                continue;

            /* Look up the information associated with the endpoints. */
            NodeInformation<T> startInfo = forest.get(curr.start);
            NodeInformation<T> endInfo = forest.get(curr.end);

            /* First, if the endpoint of
             * this edge is in some tree, there are two options:
             *
             * Case 1. If both endpoints are outer nodes in the same tree, we have
             *    found an odd-length cycle (blossom).  We then contract the
             *    edges in the cycle, repeat the search in the contracted
             *    graph, then expand the result.
             * Case 2. If both endpoints are outer nodes in different trees, then
             *    we've found an augmenting path from the root of one tree
             *    down through the other.
             * Case 3. If one endpoint is an outer node and one is an inner node,
             *    we don't need to do anything.  The path that we would end
             *    up taking from the root of the first tree through this edge
             *    would not end up at the root of the other tree, since the
             *    only way we could do this while alternating would direct us
             *    away from the root.  We can just skip this edge.
             */
            if (endInfo != null) {
                // Case 1: Do the contraction. 
                if (endInfo.isOuter && startInfo.treeRoot == endInfo.treeRoot) {
                    
                    Blossom<T> blossom = findBlossom(forest, curr);

                    /* Next, rebuild the graph using the indicated pseudonode,
                     * and recursively search it for an augmenting path.
                     */
                    List<T> path = getAlternatingPath(contractGraph(g, blossom),
                                                       contractGraph(m, blossom));

                    if (path == null) return path;

                    /* Otherwise, expand the path out into a path in this
                     * graph, then return it.
                     */
                    return expandPath(path, g, forest, blossom);
                }
                /* Case 2: Return the augmenting path from root to root. */
                else if (endInfo.isOuter && startInfo.treeRoot != endInfo.treeRoot) {
                    
                    List<T> result = new ArrayList<T>();

                    for (T node = curr.start; node != null; node = forest.get(node).parent)
                        result.add(node);

                    /* Turn the path around. */
                    result = reversePath(result);

                    /* Path from edge end to its root. */
                    for (T node = curr.end; node != null; node = forest.get(node).parent)
                        result.add(node);

                    return result;    
                }
                /* Case 3 requires no processing. */
            }
            /* Otherwise, add that node to the tree containing the
             * start of the endpoint as an inner node, then add the node for
             * its endpoint to the tree as an outer node.
             */
            else {
                 forest.put(curr.end, new NodeInformation<T>(curr.start,
                                                            startInfo.treeRoot,
                                                            false));

                T endpoint = m.edgesFrom(curr.end).iterator().next();
                forest.put(endpoint, new NodeInformation<T>(curr.end,
                                                            startInfo.treeRoot,
                                                            true));

                //Add all outgoing edges from this endpoint to the worklist
                for (T fringeNode: g.edgesFrom(endpoint))
                    worklist.add(new Edge<T>(endpoint, fringeNode));
            }
        }
        
        return null;
    }

    /**
     * Given a forest of alternating trees and an edge forming a blossom in
     * one of those trees, returns information about the blossom.
     *
     * @param forest The alternating forest.
     * @param edge The edge that created a cycle.
     * @return A Blossom struct holding information about then blossom.
     */
    private static <T> Blossom<T> findBlossom(Map<T, NodeInformation<T>> forest,
                                              Edge<T> edge) {
        // store the root of blossom by walking from each node to root and 
    	// looking for last node common to both
        LinkedList<T> onePath = new LinkedList<T>(), twoPath = new LinkedList<T>();
        for (T node = edge.start; node != null; node = forest.get(node).parent)
            onePath.addFirst(node);
        for (T node = edge.end; node != null; node = forest.get(node).parent)
            twoPath.addFirst(node);

        int mismatch = 0;
        for (; mismatch < onePath.size() && mismatch < twoPath.size(); ++mismatch)
            if (onePath.get(mismatch) != twoPath.get(mismatch))
                break;

        //cycle node is at mismatch-1
        List<T> cycle = new ArrayList<T>();
        for (int i = mismatch - 1; i < onePath.size(); ++i)
            cycle.add(onePath.get(i));
        for (int i = twoPath.size() - 1; i >= mismatch - 1; --i)
            cycle.add(twoPath.get(i));

        return new Blossom<T>(onePath.get(mismatch - 1), cycle, new HashSet<T>(cycle));
    }

    /**
     * Given a graph and a blossom, returns the contraction of the graph 
     * around that blossom.
     *
     * @param g The graph to contract.
     * @param blossom The set of nodes in the blossom.
     * @return The contraction g / blossom.
     */
    private static <T> UndirectedGraph<T> contractGraph(UndirectedGraph<T> g,
                                                        Blossom<T> blossom) {
        
        UndirectedGraph<T> result = new UndirectedGraph<T>();

        // first add all nodes not in the blossom.
        for (T node: g) {
            if (!blossom.nodes.contains(node))
                result.addNode(node);
        }

        // Add the pseudonode 
        result.addNode(blossom.root);

        /* Add each edge in, adjusting the endpoint as appropriate. */
        for (T node: g) {
            /* Skip nodes in the blossom; they're not included in the
             * contracted graph.
             */
            if (blossom.nodes.contains(node)) continue;

            /* Explore all nodes connected to this one. */
            for (T endpoint: g.edgesFrom(node)) {
                //for endpoint in blossom, add its edge through pseudonode
                if (blossom.nodes.contains(endpoint))
                    endpoint = blossom.root;

                result.addEdge(node, endpoint);
            }
        }

        return result;
    }

    
    /**
     * get a new alternating path by expanding the path if it goes through a pseudonode.
     *
     * @param path The path in the contracted graph.
     * @param g The uncontracted graph.
     * @param forest The alternating forest of the original graph.
     * @param blossom The blossom that was contracted.
     * @param pseudonode The pseudonode representing the blossom in the
     *                   contracted graph.
     * @return An alternating path in the original graph.
     */
    private static <T> List<T> expandPath(List<T> path,
                                          UndirectedGraph<T> g,
                                          Map<T, NodeInformation<T>> forest,
                                          Blossom<T> blossom) {
       
        int index = path.indexOf(blossom.root);

        /* If the node doesn't exist at all, our path is valid. */
        if (index == -1) return path;

        /* If the node is at an odd index, reverse the list and recompute the
         * index.
         */
        if (index % 2 == 1)
            path = reversePath(path);

        /* Now that we have the pseudonode at an even index (the start of a
         * dotted edge), we can start expanding out the path.
         */
        List<T> result = new ArrayList<T>();
        for (int i = 0; i < path.size(); ++i) {
            /* Look at the current node.  If it's not the pseudonode, then add
             * it into the resulting path with no modifications.
             */
            if (path.get(i) != blossom.root) {
                result.add(path.get(i));
            } 
            //else look at pseudonode, find the node in cycle connecting to next node in path
            
            else {
               
                result.add(blossom.root);

                /* Find some node in the cycle with an edge to the next node
                 * in the path.
                 */
                T outNode = findNodeLeavingCycle(g, blossom, path.get(i + 1));
                
                int outIndex = blossom.cycle.indexOf(outNode);
               
                /*  If the index of the outgoing node is even, then the path through
                 * the cycle in the forward direction will end by following a
                 * solid edge.  If it's odd, then the path through the cycle
                 * in the reverse direction ends with an outgoing edge.  We'll
                 * choose which way to go accordingly.
                 *
                 * The cycle we've stored has the root of the blossom at
                 * either endpoint, and so we'll skip over it in this
                 * iteration.
                 */
                int start = (outIndex % 2 == 0)? 1 : blossom.cycle.size() - 2;
                int step  = (outIndex % 2 == 0)? +1 : -1;

                /* Walk along the cycle accordingly. */
                for (int k = start; k != step; k += step)
                    result.add(blossom.cycle.get(k));
            }
        }
        return result;
    }

    /**
     * Given a path, returns the path formed by reversing the order of the
     * nodes in that path.
     *
     * @param path The path to reverse.
     * @return The reverse of that path.
     */
    private static <T> List<T> reversePath(List<T> path) {
        List<T> result = new ArrayList<T>();

        /* Visit the path in the reverse order. */
        for (int i = path.size() - 1; i >= 0; --i)
            result.add(path.get(i));

        return result;
    }

    /**
     * Given a graph, a blossom in that graph, and a node in that graph,
     * returns a node in the blossom that has an outgoing edge to that node.
     * If multiple nodes in the blossom have such an edge, one of them is
     * chosen arbitrarily.
     *
     * @param g The graph in which the cycle occurs.
     * @param blossom The blossom in that graph.
     * @param node The node outside of the blossom.
     * @return Some node in the blossom with an edge in g to the indicated
     *         node.
     */
    private static <T> T findNodeLeavingCycle(UndirectedGraph<T> g,
                                              Blossom<T> blossom,
                                              T node) {
        /* Check each node in the blossom for a matching edge. */
    	T cycleNode1 = null;
    	int i=0;
    	for (T cycleNode: blossom.nodes){
    		i++;
    		if(i == 2)
    		cycleNode1 = cycleNode;
			
            if (g.edgeExists(cycleNode, node)){
            	return cycleNode;
            }
    	}
        
        return cycleNode1;
    }
}