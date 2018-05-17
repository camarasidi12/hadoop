/*
  M2 MBDS - Big Data/Hadoop
	Année 2017/2018
  --
  TP2: parcours de graphe & programmation Hadoop avancée.
  --
  GraphOutputFormat.java: OutputFormat hadoop custo (utilise GraphRecordWriter pour écrire les GraphNodeWritable).
*/
package org.mbds.hadoop.graph;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.DataOutputStream;
import java.util.List;
import java.io.IOException;

public class GraphOutputFormat extends FileOutputFormat<Text, GraphNodeWritable> {
  // Le GraphOutputFormat a ici deux rôles: ouvrir le fichier de sortie sur HDFS, et créer et renvoyer une instance de
  // RecordWriter qui sera en charge d'écrire dans ce fichier et capable d'écrire notre type custo GraphNodeWritable - c'est à
  // dire une instance de notre classe GraphRecordWriter.
	public RecordWriter<Text, GraphNodeWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    // On récupère le répertoire de sortie sur HDFS.
    Path path = FileOutputFormat.getOutputPath(context);
    
    // On ouvre le fichier. Pour déterminer son nom, on utilise la fonction getUniqueFile de FileOutputFormat (notre classe mère);
    // celle-ci prends en paramètre le contexte, un prefixe, et une extension (ici .txt) et renvoie un nom de fichier unique
    // pour stocker les résultats qui seront écrits par notre OutputFormat. Elle renverra ainsi des noms de fichier sur le modèle
    // RESULTATS-r-XXXX.txt (par exemple RESULTATS-r-0000.txt); c'est elle qui génère d'habitude les noms de fichier au modèle
    // part-r-0000, etc. sur les OutputFormat Hadoop standards.
    // Si on utilisait à la place ici un nom de fichier statique, les données d'un bloc (0000, par exemple) seraient susceptibles
    // d'être écrasées par l'écriture d'un autre bloc ultérieur (0001, par exemple); c'est donc une approche à déconseiller, et viable uniquement
    // si les données de sorties ont à coup sûr un faible volume en terme de taille.
    Path fullPath = new Path(path, FileOutputFormat.getUniqueFile(context, "RESULTATS", ".txt"));
    
    FileSystem fs = path.getFileSystem(context.getConfiguration());
    FSDataOutputStream fileOut = fs.create(fullPath, context);
    // Renvoi du recordwriter custo, auquel on passe le flux vers notre fichier nouvellement ouvert.
    return new GraphRecordWriter(fileOut);
	}
}
