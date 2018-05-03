package Traitement

import org.apache.spark.rdd.RDD

object Calcul {


  def log2(num: Double): Double = {
    return (Math.log(num) / Math.log(2))
  }


  def categorieClasse(rdd: RDD[(Int, String)]):RDD[(String, Int)] ={

    val categorieDeClasse = rdd.map{case(x,y) => y}.zipWithIndex()
      .filter{case(x,y) => (y.toInt==0)}
      .map{case(x,y) => x}
      .flatMap(x=> x.split(","))
      .map(x=> x.split("_")(1))
      .map(x=> (x,1))
      .reduceByKey(_+_)


    return categorieDeClasse
  }

 def entropieEnsemble(total:Double,categorieDeClasse:RDD[(String,Int)]): Double ={
  val entropieEnsemble = categorieDeClasse
    .map{case(x,y) =>
      ((y.toDouble / total))
    }
    .map(x=> ((-x) *  log2((x))))
    .sum()
  return entropieEnsemble
}

  def rapportGain(total:Double,entropieEnsemble:Double,pos:String,rdd:RDD[(String,String)]): Double ={


    val valeurAttribut = rdd.filter { case (k, v) =>
      (k==pos)
    }.map{case(k,v) => v}
      .flatMap(k=> k.split(","))
      .map(k => (k.split("_")(0) , k.split("_")(1)))
      .reduceByKey(_+","+_)

    val entropieAttribut = valeurAttribut
      .map{case(k,v) => (v.split(",").size,calculEntropie(v.split(",")))}



    val gain = entropieEnsemble-entropieAttribut
      .map{case(k,v) => (v*k)/total}
      .sum()


    val infoSplit = entropieAttribut
      .map{ case(k,v) => k}
      .map(k=> k.toDouble / total)
      .map(k=> ((-k) *  log2((k))))
      .sum()
    val rapportGain = gain/infoSplit
    return rapportGain
  }

  def testExiste(cle: String, rdd: RDD[String]): Boolean = {
    val test = rdd.filter(a => a == cle).count() > 0
    return test
  }
  def testExiste2(cle: String, array: Array[String]): Boolean = {
if (array.contains(cle)) true else false
  }

  def calculEntropie(array: Array[String]): Double ={
    var resultat =""
    var nbOccurence = array.toSeq.groupBy(identity).mapValues(_.size)
    val total = nbOccurence.values.sum
    var entropie=0.0

    for ((k,v) <- nbOccurence)
    {
      entropie += -( v.toDouble / total ) * log2( v.toDouble / total )
    }

    return entropie
  }


  def isParsableAsDouble(s: String) = try {
    s.toDouble
    true
  } catch {
    case numberFormatException: NumberFormatException =>
      false
  }

}
