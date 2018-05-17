/*
  M2 MBDS - Big Data/Hadoop
	Année 2017/2018
  --
  TP2: parcours de graphe & programmation Hadoop avancée.
  --
  GraphMap.java: classe MAP.
*/
package org.mbds.hadoop.graph;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


// Notre classe MAP.
public class GraphMap extends Mapper<Text, GraphNodeWritable, Text, GraphNodeWritable>
{
	private static final String GREY="GRIS";

	// La fonction MAP elle-même.
	protected void map(Text key, GraphNodeWritable node, Context context) throws IOException, InterruptedException
	{
		// Si la couleur du noeud courant est grise.
		if(node.color.equals(GREY))
		{
      // On récupère la profondeur actuelle.
			int depth=node.get_depth();
      // Pour chaque voisin du noeud courant.
			for(int i=0; i<node.neighbours_list.length; ++i)
			{
				// Pour chaque voisin, on écrit un couple (clef;valeur) dont la clef est l'ID du noeud voisin, et la valeur une
				// description de ce noeud avec la couleur modifiée à "GRIS", sans liste de voisins, et avec un niveau de profondeur
				// égal à celui du noeud courant (entrée map), +1.
				context.write(new Text(node.neighbours_list[i]), new GraphNodeWritable("", "GRIS", depth+1));
			}
			// On réécris également le noeud courant, mais en passant sa couleur à NOIR.
			GraphNodeWritable rv=new GraphNodeWritable(node.neighbours, "NOIR", node.depth);
			context.write(key, rv);
		}
		else // Si la couleur du noeud courant est blanche ou noire.
		{
			// On ré-émet simplement le même couple (clef;valeur), sans le modifier.
			context.write(key, node);
		}
	}
}
