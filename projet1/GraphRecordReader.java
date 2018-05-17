/*
  M2 MBDS - Big Data/Hadoop
	Année 2017/2018
  --
  TP2: parcours de graphe & programmation Hadoop avancée.
  --
  GraphRecordReader.java: RecordReader hadoop custo (lit un GraphNodeWritable à partir du format textuel).
*/
package org.mbds.hadoop.graph;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import java.io.IOException;

public class GraphRecordReader extends RecordReader<Text, GraphNodeWritable> {
	private LineRecordReader lineRecordReader=null;
	private Text key=null;
	private GraphNodeWritable value=null;

  // Initialisation. On utilise au sein de la classe le helper Hadoop LineRecordReader, qui permet de facilement
  // lire un fichier ligne par ligne; l'initialisation consiste donc à créer le LineRecordReader en question en lui
  // passant les informations relatives au split et au contexte Hadoop.
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		close();
		lineRecordReader=new LineRecordReader();
		lineRecordReader.initialize(split, context);
	}

  // Permet d'obtenir la clef courante (celle qui a été lue lors de la dernière opération de lecture effectuée dans nextKeyValue()).
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  // Permet d'obtenir la valeur courante (celle qui a été lue lors de la dernière opération de lecture effectuée dans nextKeyValue()).
	public GraphNodeWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

  // Permet d'obtenir un indicateur de progression; on fait ici appel au helper LineRecordReader (qui indique une progression basée sur
  // le nombre total de lignes au sein du fichier et le nombre de lignes lues jusqu'ici).
	public float getProgress() throws IOException, InterruptedException {
		return lineRecordReader.getProgress();
	}

  // Fermeture du fichier (toujours via le helper LineRecordReader).
	public void close() throws IOException {
		if(lineRecordReader!=null)
		{
			lineRecordReader.close();
			lineRecordReader=null;
		}
		key=null;
		value=null;
	}

	// Lit la ligne suivante. On ne renvoie pas la clef et la valeur lues; à la place, on les stocke et celles-ci seront renvoyées
  // dans les méthodes getKeyValue() et getCurrentValue(). On renvoie en revanche true pour indiquer que la ligne a été lue avec succés;
  // ou false pour indiquer que non (fin du fichier, par exemple).
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!lineRecordReader.nextKeyValue()) {
			key = null;
			value = null;
			return false;
		}

		// Logique de lecture / interprétation (split clef / valeur sur ';').
		Text line=lineRecordReader.getCurrentValue();
		String str=line.toString();
		String[] arr=str.split(";");
		key=new Text(arr[0]);
		value=new GraphNodeWritable(arr[1]);
		return true;
	}
}
