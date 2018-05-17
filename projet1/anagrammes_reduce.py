#!/usr/bin/python
# -*- coding: utf-8 -*-
#  M2 MBDS - Big Data/Hadoop
#  Année 2017/2018
#  --
#  TP1: détection d'anagrammes - correction, version Python.
#  --
#  anagrammes_reduce.py: opération REDUCE.
import sys

# On va recevoir ici toutes les valeurs (les mots) associés à une clef commune (les lettres triées par
# ordre alphabétique - voir fonction MAP).
# Le seul travail qu'on va effectuer ici va donc être de concaténer les mots ensemble pour les lister
# comme des anagrammes dans une chaîne de caractères unique.
#
# On va par exemple recevoir la clef "elmno" et la liste (l'iterable): (melon, lemon).
# Et on renverra un couple (clef;valeur) avec comme clef: "elmno" et comme valeur: "melon | lemon".

lastletters=None
current_result=""

# Pour chaque couple clef;valeur d'entrée.
for input_tuple in sys.stdin:
  # Supprimer les espaces autour de la string.
  intuple=input_tuple.strip()
  # Récupérer la clef (les lettres triées par ordre alphabétique) et la valeur (le mot).
  letters, word=intuple.split('\t', 1)

  # On utilise ce test parce que contrairement aux implémentations Java, dans le cas de Streaming Hadoop
  # est susceptible de nous renvoyer plusieurs clefs distinctes, avec la liste des valeurs associées à chaque
  # fois. On pourrait ainsi recevoir sur l'entrée standard les lignes:
  #  elmno[tab]lemon
  #  elmno[tab]melon
  #  aekl[tab]lake
  #  aekl[tab]leak
  if letters!=lastletters and lastletters!=None:
    print "%s\t%s" % (lastletters, current_result)
    current_result=""
  lastletters=letters

  if current_result=="":    # Premier mot
    current_result=word
  else:
    current_result="%s | %s" % (current_result, word)   # On concatene le mot courant à la suite du resultat courant.

print "%s\t%s" % (lastletters, current_result)
