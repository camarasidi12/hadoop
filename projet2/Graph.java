/*
  M2 MBDS - Big Data/Hadoop
	Année 2017/2018
  --
  TP2: parcours de graphe & programmation Hadoop avancée.
  --
  Graph.java: classe driver (contient le main du programme).
*/
package org.mbds.hadoop.graph;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.FileSystem;
import java.io.BufferedReader;
import java.io.InputStreamReader;



// Note classe Driver (contient le main du programme Hadoop).
public class Graph
{
	// Le main du programme.
	public static void main(String[] args) throws Exception
	{
		// Créé un object de configuration Hadoop.
		Configuration conf=new Configuration();
		// Permet à Hadoop de lire ses arguments génériques, récupère les arguments restants dans ourArgs.
		String[] ourArgs=new GenericOptionsParser(conf, args).getRemainingArgs();

    // On récupère le premier argument de la ligne de commande non spécifique à Hadoop: il s'agit de notre
    // fichier d'entrée initial (par exemple /graph_input.txt).
		String input_path=ourArgs[0];
    // On récupère également le second argument de la ligne de commande: il s'agira d'un prefixe pour nos
    // répertoires de résultat lors des exécutions (par exemple si "/graphout", on aura "/graphout-step-1", "/graphout-step-2", etc.).
		String output_path_prefix=ourArgs[1];
		String output_path="";
		int nb_step=0;
    
		// Boucle infinie (on en sortira si on confirme qu'on a parcouru tout le graphe).
		while(true)
		{
      // Aprés la première exécution et avant chaque nouvelle exécution, on va 1. modifier le fichier d'entrée pour qu'il
      // corresponde à la sortie de l'exécution précédente et 2. vérifier qu'on ait pas parcouru tout le graphe (si c'est le
      // cas, on s'arrète).
			if(nb_step>0)
			{
				// Les données de sortie de l'exécution précédente deviennent les nouvelles données d'entrée.
        // On utilise ici un wildcard ("*"): en effet, si on a beaucoup de données de sortie, les données seront stockées
        // dans plusieurs fichiers RESULTATS-r-0000.txt, RESULTATS-r-0001.txt, etc. (voir GraphOutputFormat pour plus de
        // détails). Cette expression nous permettra d'ouvrir tous les fichiers commençant par "RESULTATS" au sein du
        // répertoire de sortie de l'exécution précédente; c'est automatique lorsqu'on passe ce type de syntaxe en
        // entrée du programme map/reduce (en revanche, on devra lister les différents fichiers nous-même lors de leur ouverture
        // pour confirmer qu'on ait parcouru tout le graphe; voir la fonction output_all_black()).
				input_path=output_path+"/RESULTATS*";

				// On vérifie qu'on ait pas parcouru tout le graphe. Si c'est le cas, on affiche un message et on interrompt les exécutions.
        if(output_all_black(conf, input_path))
        {
					System.out.println("Tous les noeuds sont parcourus; parcours de graphe TERMINE.");
          System.out.println("Répertoire de sortie final: '"+output_path+"'");
					break;
				}
			}
      
      // On génère notre prochain répertoire de sortie pour les résultats: sur le modèle PREFIX-step-X (où PREFIX est le préfixe
      // passé sur la ligne de commande).
			nb_step=nb_step+1;
			output_path=output_path_prefix+"-step-"+nb_step;

      System.out.println("Exécution numéro #"+nb_step+": entrée '"+input_path+"', sortie '"+output_path+"'");

			// Obtient un nouvel objet Job: une tâche Hadoop. On fourni la configuration Hadoop ainsi qu'une description
			// textuelle de la tâche.
			Job job=Job.getInstance(conf, "Parcours de graphes v1.0");

      // On indique que les InputFormat et OutputFormat à utiliser sont ceux qu'on a créé nous-même; c'est à dire
      // respectivement GraphInputFormat et GraphOutputFormat, qui savent écrire des objets GraphNodeWritable.
			job.setInputFormatClass(GraphInputFormat.class);
			job.setOutputFormatClass(GraphOutputFormat.class);
			
			// Défini les classes driver, map et reduce.
			job.setJarByClass(Graph.class);
			job.setMapperClass(GraphMap.class);
			job.setReducerClass(GraphReduce.class);
			
			// Défini types clefs/valeurs de notre programme Hadoop.
      // On utilise ici comme valeur notre type spécifique GraphNodeWritable.
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(GraphNodeWritable.class);

			// Défini les fichiers d'entrée du programme et le répertoire des résultats.
      // Lors de la première exécution, le fichier d'entrée sera celui passé en premier argument de la ligne de commande; mais
      // lors des exécutions ultérieures, il s'agira du répertoire de sortie de l'exécution précédente (par exemple /graphout-step-1).
			FileInputFormat.addInputPath(job, new Path(input_path));
			FileOutputFormat.setOutputPath(job, new Path(output_path));

			// On lance la tâche Hadoop.
			if(!job.waitForCompletion(true))
			{
				System.out.println("ERREUR: l'exécution numéro #"+nb_step+" a échoué.");
				System.exit(-1);
			}
		}

		System.exit(0);
	}


  // Fonction qui prends en paramètre le path des fichiers de sortie d'une exécution du programme; la fonction
  // va lire tous les fichiers résultat de l'exécution et vérifier si tous les noeuds ont désormais pour couleur
  // "NOIR" (ie, tous parcourus); si c'est le cas, elle renvoie true. Sinon, elle renvoie false.
  // C'est cette fonction qui permet d'avoir une condition d'arrét à l'exécution répétée du programme.
  private static Boolean output_all_black(Configuration conf, String output_path) throws Exception
  {
    // Recupère une instance désignant le système de fichier HDFS à partir de la configuration de la tâche.
    FileSystem fs=FileSystem.get(conf);
    // On créé un objet Path désignant nos différents fichiers de sortie.
    Path output_files=new Path(output_path);
    // On doit désormais lister tous les fichiers de sortie correspondant à l'output path passé en paramètre;
    // cela nous permet de lister pour les fichiers (0000, 0001, 0002, etc.) à partir d'un pattern. Ici, on
    // passera par exemple "/graphout-step-1/RESULTATS*" à la fonction (voir le main), et donc on ouvrira
    // tous les fichiers correspondants les uns aprés les autres: RESULTATS-r-0000.txt, etc.; ceci afin
    // de les lire pour confirmer si oui ou non tous les noeuds sont parcourus.
    FileStatus[] list=fs.globStatus(output_files);
    // On parcours la liste des fichiers correspondant.
    for(int i=0; i<list.length; ++i)
    {
      System.out.println("Ouverture du fichier '"+list[i].getPath()+"' pour vérification...");
      // On ouvre le fichier.
      BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(list[i].getPath())));
      // On le lit, ligne par ligne.
      String line=br.readLine();
      Boolean is_over=true;
      while(line!=null)
      {
        if(!line.contains("NOIR"))
        {
          // La ligne courante ne contient PAS la couleur "NOIR"; on a donc au moins un noeud qui n'est pas encore
          // parcouru; on renvoit false.
          br.close();
          return(false);
        }
        line=br.readLine();
      }
      br.close();
    }

    // On a parcouru tous les fichiers et tous les noeuds étaient noirs; on renvoie true.
    return(true);
  }
}
