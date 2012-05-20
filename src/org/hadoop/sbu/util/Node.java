package org.hadoop.sbu.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

public class Node {

  public static enum Color {
    WHITE, GRAY, BLACK
  };

  private final int id;
  private int distance;
  private List<Integer> edges = new ArrayList<Integer>();
  private Color color = Color.WHITE;

  public Node(String str) {

    String[] map = str.split("\t");
    String key = map[0];
    String value = map[1];

    String[] tokens = value.split("\\|");

    this.id = Integer.parseInt(key);

    if(tokens[0].length() > 0)
    for (String s : tokens[0].split(",")) {
      if (s.length() > 0) {
        edges.add(Integer.parseInt(s));
      }
    }
    
    if (tokens[1].equals("Integer.MAX")) {
      this.distance = Integer.MAX_VALUE;
    } else {
      this.distance = Integer.parseInt(tokens[1]);
    }
    
    this.color = Color.valueOf(tokens[2]);

  }

  public Node(int id) {
    this.id = id;
  }

  public int getId() {
    return this.id;
  }

  public int getDistance() {
    return this.distance;
  }

  public void setDistance(int distance) {
    this.distance = distance;
  }

  public Color getColor() {
    return this.color;
  }

  public void setColor(Color color) {
    this.color = color;
  }

  public List<Integer> getEdges() {
    return this.edges;
  }

  public void setEdges(List<Integer> edges) {
	  if(edges != null)
		  this.edges = edges;
  }

  public Text getLine() {
    StringBuffer s = new StringBuffer();
    
    for (int v : edges) {
      s.append(v).append(",");
    }
    if(s.length() > 0)
    	s.setLength(s.length()-1);
    s.append("|");

    if (this.distance < Integer.MAX_VALUE) {
      s.append(this.distance).append("|");
    } else {
      s.append("Integer.MAX").append("|");
    }

    s.append(color.toString());

    //System.out.println(s);
    return new Text(s.toString());
  }

}