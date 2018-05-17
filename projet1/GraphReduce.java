/*
  M2 MBDS - Big Data/Hadoop
	Année 2017/2018
  --
  TP2: parcours de graphe & programmation Hadoop avancée.
  --
  GraphReduce.java: classe Reduce.
*/
package org.mbds.hadoop.graph;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;
import java.io.IOException;


// Notre classe REDUCE.
public class GraphReduce extends Reducer<Text, GraphNodeWritable, Text, GraphNodeWritable>
{
	// La fonction REDUCE elle-même. Les arguments: la clef key, un Iterable de toutes les valeurs
	// qui sont associées à la clef en question, et le contexte Hadoop (un handle qui nous permet de renvoyer le résultat à Hadoop).
  public void reduce(Text key, Iterable<GraphNodeWritable> values, Context context) throws IOException, InterruptedException
	{
		int new_depth=-1;
		String new_neighbours="";
		String new_color="";

		// Pour parcourir toutes les valeurs associées à la clef fournie.
		Iterator<GraphNodeWritable> i=values.iterator();
		int nb=0;
		while(i.hasNext())   // Pour chaque valeur...
		{
			GraphNodeWritable node=i.next();
			nb=nb+1;

      // On va récupérer la liste de voisins la plus longue, la couleur la plus "forte", et enfin
      // la profondeur la plus grande parmis toutes les valeurs associées à la clef.
      // Pour les noeuds initialement gris (IE, en cours de parcours) lors de cette exécution, cela nous permet de
      // regrouper le couple clef;valeur contenant la liste des voisins mais l'ancienne couleur/profondeur et le
      // couple clef;valeur contenant la nouvelle couleur/profondeur mais aucune liste de voisins.

      // Profondeur.
      if(node.get_depth()>new_depth)
        new_depth=node.get_depth();
      // Liste de voisins.
      if(node.neighbours.length()>new_neighbours.length())
        new_neighbours=node.neighbours;
      // Couleur.
      if((new_color.equals("")) || ((new_color.equals("BLANC") && (node.color.equals("GRIS") || node.color.equals("NOIR"))) ||
                                    (new_color.equals("GRIS") && (node.color.equals("NOIR")))))
      {
        new_color=node.color;
      }
		}

		// Enfin, on écrit un seul couple (clef;valeur) pour la clef fournie; avec une valeur contenant les voisins corrects du noeud, et la couleur et la profondeur mises
		// à jour.
		context.write(key, new GraphNodeWritable(new_neighbours, new_color, new_depth));
  }
}
