/*
  M2 MBDS - Big Data/Hadoop
	Année 2017/2018
  --
  TP2: parcours de graphe & programmation Hadoop avancée.
  --
  GraphNodeWritable.java: Type spécifique décrivant le noeud d'un graphe; utilisé directement dans les classes map et reduce.
*/
package org.mbds.hadoop.graph;
import java.util.ArrayList;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.Arrays;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Iterator;

// Comme le noeud d'un graphe sera uniquement utilisé comme valeur, on pourrait ici à la place
// implémenter Writable plutôt que WritableComparable (qui permet d'utiliser un type spécifique
// à la fois comme une clef ou comme une valeur dans nos classes map et reduce).
public class GraphNodeWritable implements WritableComparable<GraphNodeWritable> {
	public String neighbours;
	public String[] neighbours_list;
	public String color;
	public String depth;

	// Ecriture disque (serialise).
	public void write(DataOutput out) throws IOException {
		Text line=new Text(get_serialized());
		line.write(out);
	}

	// Constructeur textuel (deserialise).
	public GraphNodeWritable(String datatxt)
	{
		unserialize(datatxt);
	}

	// Constructeur.
	public GraphNodeWritable(String neighbours_v, String color_v, int depth_v)
	{
		neighbours=neighbours_v;
    if(neighbours.equals(""))
      neighbours_list=new String[0];
    else
      neighbours_list=neighbours.split(",");
		color=color_v;
		depth=Integer.toString(depth_v);
	}

	// Constructeur 2.
	public GraphNodeWritable(String neighbours_v, String color_v, String depth_v)
	{
		neighbours=neighbours_v;
    if(neighbours.equals(""))
      neighbours_list=new String[0];
    else
      neighbours_list=neighbours.split(",");
		color=color_v;
		depth=depth_v;
	}

	// Deserialisation.
	public void unserialize(String datatxt)
	{
		String[] data=datatxt.split("\\|");
		neighbours=data[0];
    if(neighbours.equals(""))
      neighbours_list=new String[0];
    else
      neighbours_list=neighbours.split(",");
		color=data[1];
		depth=data[2];
	}

	public GraphNodeWritable() { }

	// Serialisation.
	public String get_serialized()
	{
		String line="";
		line+=neighbours+"|"+color+"|"+depth;
		return(line);
	}
	
	// Lecture disque (deserialise).
	public void readFields(DataInput in) throws IOException {
		Text line=new Text();
		line.readFields(in);
		String linestr=line.toString();
		unserialize(linestr);
	}

  // Renvoie la profondeur en format numérique.
  public int get_depth()
  {
    return(Integer.parseInt(depth));
  }

	// Comparaison. Utilise le nombre de voisins.
	// La fonction est ici arbitraire, mais peu importe: le type qu'on défini
	// ne sera jamais utilisé comme clef, uniquement comme valeur, et cela n'aura donc
	// pas d'impact (voir commentaire en début de classe).
	public int compareTo(GraphNodeWritable o) {
		int mysize=Integer.parseInt(depth);
		int theirsize=Integer.parseInt(o.depth);
		return (mysize < theirsize ? -1 : (mysize==theirsize ? 0 : 1));
	}

	// Hashcode. Imparfaite (le hashcode peut changer), mais même remarque que pour la fonction
	// de comparaison (+ voir commentaire en début de classe).
	public int hashCode() {
		return neighbours.hashCode();
	}
}
