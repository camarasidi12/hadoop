/*
  M2 MBDS - Big Data/Hadoop
	Année 2017/2018
  --
  TP1: détection d'anagrammes - correction.
  --
  WCountMap.java: classe MAP.
*/
package org.mbds.hadoop.anagrammes;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;


// Notre classe MAP.
public class AnagrammesMap extends Mapper<Object, Text, Text, Text>
{
	// La fonction MAP elle-même.
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		// Nos données d'entrée: la liste des mots courants du langage Anglais.
		// On a un mot sur chaque ligne, et Hadoop va envoyer une ligne aprés l'autre aux fonctions MAP.
		// En conséquence, la fonction map recevra uniquement un mot unique de la liste.
		// Le seul travail effectué par notre fonction map() va être de renvoyer ce mot en valeur dans notre
		// couple clef;valeur.
		//
		// Pour ce qui est de la clef, on choisit les lettres du mot lui-même, triées par ordre alphabétique.
		// C'est cette clef qui va nous permettre de détecter les anagrammes: on choisit une clef qui sera
		// commune à tous les mots qui sont des anagrammes. Lors de l'étape "shuffle" (tri), Hadoop va
		// regrouper toutes les valeurs (les mots) qui ont la même clef (les lettres triées par ordre
		// alphabétique), et les envoyer à notre fonction REDUCE. On se retrouvera donc à l'étape REDUCE
		// avec tous les mots qui sont des anagrammes regroupés ensemble.
		//
		// Par exemple, les mots "melon" et "lemon" donneront les couples (clef;valeur):
		//  (elmno;melon) et (elmno;lemon)
		// Et on se retrouvera donc à l'étape reduce avec la clef: elmno
		// Et la liste de mots: melon, lemon

		// On récupère la valeur (le mot) et on trie ses lettres par ordre alphabétique.
		// Le resultat (dans letters) est notre clef.
		// On en profite aussi pour passer toutes les lettres en minuscules.
		char[] letters=value.toString().toLowerCase().toCharArray();
		Arrays.sort(letters);

		// On renvoie le couple (clef;valeur), en convertissant la clef au type Hadoop (Text).
		context.write(new Text(new String(letters)), value);
	}
}
