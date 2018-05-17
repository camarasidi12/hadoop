#!/usr/bin/python
# -*- coding: utf-8 -*-
#  M2 MBDS - Big Data/Hadoop
#  Année 2017/2018
#  --
#  TP1: détection d'anagrammes - correction, version Python.
#  --
#  anagrammes_map.py: opération MAP.
import sys

# Pour chaque mot d'entrée.
for input_word in sys.stdin:
  # Nos données d'entrée: la liste des mots courants du langage Anglais.
  # On a un mot sur chaque ligne, et Hadoop va envoyer une ligne aprés l'autre aux fonctions MAP.
  # En conséquence, la fonction map recevra uniquement un mot unique de la liste.
  # Le seul travail effectué par notre fonction map va être de renvoyer ce mot en valeur dans notre
  # couple clef;valeur.
  #
  # Pour ce qui est de la clef, on choisit les lettres du mot lui-même, triées par ordre alphabétique.
  # C'est cette clef qui va nous permettre de détecter les anagrammes: on choisit une clef qui sera
  # commune à tous les mots qui sont des anagrammes. Lors de l'étape "shuffle" (tri), Hadoop va
  # regrouper toutes les valeurs (les mots) qui ont la même clef (les lettres triées par ordre
  # alphabétique), et les envoyer à notre fonction REDUCE. On se retrouvera donc à l'étape REDUCE
  # avec tous les mots qui sont des anagrammes regroupés ensemble.
  #
  # Par exemple, les mots "melon" et "lemon" donneront les couples (clef;valeur):
  #  (elmno;melon) et (elmno;lemon)
  # Et on se retrouvera donc à l'étape reduce avec la clef: elmno
  # Et la liste de mots: melon, lemon

  # Supprimer les espaces autour du mot envoyé en entrée.
  word=input_word.strip()

  # On trie les lettres du mot par ordre alphabétique.
  letters=''.join(sorted(word))

  # On renvoie notre couple (clef;valeur) sur une ligne, avec une tabulation entre la clef et la valeur.
  print "%s\t%s" % (letters, word)
