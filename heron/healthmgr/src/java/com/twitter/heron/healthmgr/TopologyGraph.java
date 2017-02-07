// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.twitter.heron.healthmgr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;


public class TopologyGraph {
  private MultiMap outEdges = new MultiMap();
  private MultiMap inEdges = new MultiMap();

  public TopologyGraph() {
  }

  public TopologyGraph(TopologyGraph graph) {
    this.inEdges.putAll(graph.inEdges);
    this.outEdges.putAll(graph.outEdges);
  }

  /**
   * Adds a directed edge from origin to target. The vertices are not
   * required to exist prior to this call.
   *
   * @param source the source vertex
   * @param dest the dest vertex
   * @return true if the edge was added, false otherwise
   */
  public boolean addEdge(String source, String dest) throws RuntimeException {

    if (source != null && dest != null) {
      if (hasPath(dest, source)) {
        return false;
      }

      outEdges.put(source, dest);
      outEdges.put(dest, null);
      inEdges.put(dest, source);
      inEdges.put(source, null);
      return true;
    } else {
      throw new RuntimeException("Cannot add null vertex");
    }
  }

  /**
   * Adds a vertex to the graph. If the vertex does not exist prior to this call, it is added with
   * no incoming or outgoing edges.
   *
   * @param vertex the new vertex
   */
  public void addVertex(String vertex) throws RuntimeException {
    if (vertex != null) {
      outEdges.put(vertex, null);
      inEdges.put(vertex, null);
    } else {
      throw new RuntimeException("Cannot add null vertex");
    }
  }

  /**
   * Removes a vertex and all its edges from the graph.
   *
   * @param vertex the vertex to remove
   */
  public void removeVertex(String vertex) {
    Set<String> targets = outEdges.removeAll(vertex);
    for (Iterator<String> it = targets.iterator(); it.hasNext();) {
      inEdges.remove(it.next(), vertex);
    }
    Set<String> origins = inEdges.removeAll(vertex);
    for (Iterator<String> it = origins.iterator(); it.hasNext();) {
      outEdges.remove(it.next(), vertex);
    }
  }

  /**
   * Returns the vertices with no incoming edges. The
   * returned Set<String>'s iterator traverses the nodes in the order they were added to the graph.
   *
   * @return the sources of the receiver
   */
  public Set<String> getSources() {
    return computeZeroEdgeVertices(inEdges);
  }

  /**
   * Returns the vertices with no outgoing edges. The returned
   * Set<String>'s iterator traverses the nodes in the order they were added to the graph.
   *
   * @return the sinks of the receiver
   */
  public Set<String> getSinks() {
    return computeZeroEdgeVertices(outEdges);
  }

  private Set<String> computeZeroEdgeVertices(MultiMap map) {
    Set<String> candidates = map.keySet();
    Set<String> roots = new LinkedHashSet<String>(candidates.size());
    for (Iterator<String> it = candidates.iterator(); it.hasNext();) {
      String candidate = it.next();
      if (map.get(candidate).isEmpty()) {
        roots.add(candidate);
      }
    }
    return roots;
  }

  /**
   * Returns the children of a vertex.
   *
   * @param vertex the parent vertex
   * @return the direct children of the vertex
   */
  public Set<String> getChildren(String vertex) {
    return outEdges.get(vertex);
  }


  /**
   * Returns the parents of a vertex.
   *
   * @param vertex the child vertex
   * @return the parents of the vertex
   */
  public Set<String> getParents(String vertex) {
    return inEdges.get(vertex);
  }

  public void removeEdge(String source, String dest) {
    outEdges.remove(source, dest);
    inEdges.remove(dest, source);
  }

  private boolean hasPath(String start, String end) {
    if (start == end) {
      return true;
    }

    Set<String> children = outEdges.get(start);
    for (Iterator<String> it = children.iterator(); it.hasNext();) {
      if (hasPath(it.next(), end)) {
        return true;
      }
    }
    return false;
  }

  public String toString() {
    return "Out: " + outEdges.toString() + " In: " + inEdges.toString();
  }

  /**
   * Performs topological sorting of the graph and returns the inverse sorted order
   * using Kahn's algorithm (see https://en.wikipedia.org/wiki/Topological_sorting)
   */
  public ArrayList<String> topologicalSort() {
    ArrayList<String> sortedElements = new ArrayList<>();
    Set<String> sources = this.getSources();

    Iterator<String> iterator = sources.iterator();
    while (iterator.hasNext()) {
      String element = iterator.next();
      iterator.remove();
      sortedElements.add(element);
      Set<String> children = getChildren(element);
      Iterator<String> childIterator = children.iterator();
      while (childIterator.hasNext()) {
        String dest = childIterator.next();
        removeEdge(element, dest);
        if (inEdges.get(dest).isEmpty()) {
          sources.add(dest);
          iterator = sources.iterator();
        }
      }
    }
    Collections.reverse(sortedElements);
    return sortedElements;
  }

  private static class MultiMap {
    private Map<String, Set<String>> nMap = new LinkedHashMap<String, Set<String>>();

    /**
     * Adds val to the values mapped to by key. If
     * val is null, key is added to the key Set<String> of
     * the multimap.
     *
     * @param key the key
     * @param val the value
     */
    public void put(String key, String val) {
      Set<String> values = nMap.get(key);
      if (values == null) {
        values = new LinkedHashSet<String>();
        nMap.put(key, values);
      }
      if (val != null) {
        values.add(val);
      }
    }

    public void putAll(MultiMap map) {
      for (String key : map.keySet()) {
        Set<String> newValue = new LinkedHashSet<String>(map.get(key));
        nMap.put(key, newValue);
      }
    }

    /**
     * Returns all mappings for the given key
     *
     * @param key the key
     * @return the mappings for key
     */
    public Set<String> get(String key) {
      Set<String> values = nMap.get(key);
      return values == null ? new LinkedHashSet<String>() : values;
    }

    public Set<String> keySet() {
      return nMap.keySet();
    }

    /**
     * Removes all mappings for key and removes key from the key
     * Set<String>.
     *
     * @param key the key to remove
     * @return the removed mappings
     */
    public Set<String> removeAll(String key) {
      Set<String> values = nMap.remove(key);
      return values == null ? new LinkedHashSet<String>() : values;
    }

    /**
     * Removes a mapping from the multimap, but does not remove the key from the
     * key Set<String>.
     *
     * @param key the key
     * @param val the value
     */
    public void remove(String key, String val) {
      Set<String> values = nMap.get(key);
      if (values != null) {
        values.remove(val);
      }
    }

    public String toString() {
      return nMap.toString();
    }
  }
}

