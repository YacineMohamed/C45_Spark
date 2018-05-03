package Donnees.Creation

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Discretisation {

val sc = Initialisation.Spark.sc


  def creeInterval(array: Array[Double]): String={
    var resultat=""
    for(i<-0 to array.size-2){
      if(resultat.equals("")){
        resultat = (array(i) + array(i+1))/2+""
      }else{
        resultat= resultat+","+(array(i) + array(i+1))/2
      }
    }

    return resultat
  }


  def discretisation(pos:String,rdd:RDD[(Int,String)]):Double={
    val categorieDeClasse = Traitement.Calcul.categorieClasse(rdd)

    val total = categorieDeClasse
      .map{case(x,y) => y}.sum()

    val entropieEnsemble = Traitement.Calcul.entropieEnsemble(total,categorieDeClasse)

    val ensemble = rdd
        .map{case(x,y) => y}
      .flatMap(x=> x.split(","))


    val valeurs = ensemble
      .map(x => x.split("_")(0))
        .distinct()
      .map(x=> (1,x))
      .reduceByKey(_+","+_)
        .map{case(x,y) => y}
      .map(x => creeInterval(x.split(",").map(_.toDouble).sorted))
      .flatMap(x=>x.split(","))
      .map(x=>x.toDouble)


    var listRapport:RDD[(Double,Double)] = sc.emptyRDD

    val listInterval = sc.broadcast(valeurs.collect())
    listInterval.value.foreach(x=> {
      val ensembleDiscret = ensemble
        .map(k => (k.split("_")(0).toDouble, k.split("_")(1)))
        .map{case(k,v) =>
          if(k>x) (">"+x+"_"+v)
        else
            ("<"+x+"_"+v)
        }
        .map(x => (pos,x)).reduceByKey(_+","+_)


      val rapportGain = Traitement.Calcul.rapportGain(total,entropieEnsemble,pos,ensembleDiscret)


      listRapport = listRapport ++ sc.parallelize(Map(x-> rapportGain).toSeq)

    }
    )

    val posMax = listRapport.reduce((x,y) => if(x._2>y._2) x else y)._1




    return posMax
  }








}
