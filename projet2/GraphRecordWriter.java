/*
  M2 MBDS - Big Data/Hadoop
	Année 2017/2018
  --
  TP2: parcours de graphe & programmation Hadoop avancée.
  --
  GraphRecordWriter.java: recordwriter spécifique (écrit un GraphNodeWritable).
*/
package org.mbds.hadoop.graph;

import org.apache.hadoop.io.Text;
import java.util.List;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

// Recordwriter spécifique à un noeud du graphe. Utilisé par GraphOutputFormat.
public class GraphRecordWriter extends RecordWriter<Text, GraphNodeWritable> {
	private DataOutputStream out;

	// Appelée initialement lors de l'écriture des tuples finaux (post reduce).
  // Permet d'obtenir le flux de sortie qui servira à écrire sur HDFS.
	public GraphRecordWriter(DataOutputStream stream) {
		out = stream;
	}

	// Appelée pour chaque tuple. Écrit le tuple sur le flux de sortie (out).
	public void write(Text k, GraphNodeWritable val) throws IOException, InterruptedException {
		out.writeBytes(k.toString()+";"+val.get_serialized()+"\n");
	}

	// Appelée à la fin; permet de fermer le flux de sortie HDFS.
	public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
		out.close();
	}
}
