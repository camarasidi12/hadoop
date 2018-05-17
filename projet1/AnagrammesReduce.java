/*
  M2 MBDS - Big Data/Hadoop
	Année 2017/2018
  --
  TP1: détection d'anagrammes - correction.
  --
  AnagrammesReduce.java: classe REDUCE.
*/
package org.mbds.hadoop.anagrammes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;
import java.io.IOException;


// Notre classe REDUCE.
public class AnagrammesReduce extends Reducer<Text, Text, Text, Text>
{
	// La fonction REDUCE elle-même. Les arguments: la clef key, un Iterable de toutes les valeurs
	// qui sont associées à la clef en question, et le contexte Hadoop.
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		// On va recevoir ici toutes les valeurs (les mots) associés à une clef commune (les lettres triées par
		// ordre alphabétique - voir fonction MAP).
		// Le seul travail qu'on va effectuer ici va donc être de concaténer les mots ensemble pour les lister
		// comme des anagrammes dans une chaîne de caractères unique.
		//
		// On va par exemple recevoir la clef "elmno" et la liste (l'iterable): (melon, lemon).
		// Et on renverra un couple (clef;valeur) avec comme clef: "elmno" et comme valeur: "melon | lemon".

		Iterator<Text> i=values.iterator();  // Pour parcourir toutes les valeurs associées à la clef fournie.
		String result="";
		Boolean first=true;
		while(i.hasNext())   // Pour chaque valeur...
		{
			if(first)   // Premier mot, donc on n'inclut pas le symbole "|".
			{
				result=i.next().toString();   // Notre chaîne de résultat contient initiallement le premier mot.
				first=false;
			}
			else
				result=result+" | "+i.next().toString();   // On concatene le mot à la chaîne de resultat.
		}

		// On renvoie le couple (clef;valeur) constitué de notre clef key et de la chaîne concaténée.
		context.write(key, new Text(result));
  }
}
