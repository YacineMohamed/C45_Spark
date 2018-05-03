import java.util.Calendar

import PreTraitement.Preparation
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.Path

object C45 {
  val sc = Initialisation.Spark.sc
  var res: RDD[String] = sc.emptyRDD
  def main(args: Array[String]): Unit = {

    val fichierEntee =  args(0)
    val dossierSortie = args(1)
    val separateurAttribut = args(2)
    val entete =args(3)
    val ID = args(4)
    val attributNumerique=""

    val params:Array[String] =
      Array(fichierEntee,dossierSortie,separateurAttribut,entete,ID,attributNumerique)

    val ensembleApprentissage = Preparation.prepar(sc,params)
    val ensembleStructure = ensembleApprentissage
      .map(a => Donnees.Creation.Structure.structurerEnsemble(a))
   // println(".....DEBUT.....")

    val debut = ""+Calendar.getInstance().getTime.getMinutes+":"+Calendar.getInstance().getTime.getSeconds

    val ensembleRegroupe = ensembleStructure
      .flatMap(a => a.split(" "))
      .map(a => (a.split(":")(0).toInt, (a.split(":")(1))))
      .reduceByKey(_ + "," + _)


    construireArbre("", ensembleRegroupe)

    enregistreArbre(debut,dossierSortie)

  }

  def construireArbre(noeud: String, rdd: RDD[(Int, String)]): Unit = {
    if (rdd.count() > 0) {

      val categorieDeClasse = Traitement.Calcul.categorieClasse(rdd)


      if (categorieDeClasse.count() > 1) {

        val total = categorieDeClasse
          .map{case(x,y) => y}.sum()


        val entropieEnsemble = Traitement
          .Calcul.entropieEnsemble(total,categorieDeClasse)


        val posAttribut = sc.broadcast(rdd.map{case(x,y) => x}.collect())


        var listRapportGain:RDD[(Int,Double)]=sc.emptyRDD
        posAttribut.value.foreach(x => {
          val rapportGain = Traitement.Calcul.rapportGain(total,entropieEnsemble,x+"",rdd.map{case(x,y)=>(x+"",y)})

          listRapportGain = listRapportGain ++ sc.parallelize(Map(x->rapportGain).toSeq)

        }
        )



        val posMax = listRapportGain
          .reduce((x, y) => if (x._2 > y._2) x else y)._1

        diviserEnsemble(sc, noeud, posMax, rdd)


      }

      /** *Cas ou l ensemble de données est homogene */
      else {

        val valeurClasse = categorieDeClasse.first()._1

        res = res ++ sc.parallelize(Seq(noeud + ":" + valeurClasse))

        /** *Ajouter un element a notre rdd resultat,
          * en ajoutant cette nouvelle feuille !
          * */
      }
    }
  }





  def diviserEnsemble(sc: SparkContext, noeud: String, pos: Int, rdd: RDD[(Int, String)]): Unit = {
    val nouvelEns = rdd
      .filter { case (x, y) => x != pos } //1
      .map { case (x, y) => Donnees.Creation.Structure.changerStructure(x, y) } //2
      .flatMap(x => x.split(","))
      .map(x => (x.split("#")(0), x.split("#")(1))) //3



    val valAttDiv = rdd
      .filter { case (x, y) => x == pos }


    val valeurLigne = sc.parallelize(valAttDiv.first()._2.split(","))
      .map(x => x.split("_")(0)).zipWithIndex()
      .map { case (x, y) => (x, y.toString) }
      .reduceByKey(_ + "," + _)


    val broadcastValLigne = sc.broadcast(valeurLigne.collectAsMap())

    broadcastValLigne.value.foreach { case (x, y) =>
      val lesLigne = sc.parallelize(y).flatMap(x => x.toString.split(","))


      val sousEnsemble = nouvelEns.filter{case(c,v) => Traitement.Calcul.testExiste2(c,y.split(","))}
        .map { case (xx, yy) => (yy.split(":")(0).toInt, yy.split(":")(1)) }
        .reduceByKey(_ + "," + _)


      var nouvNoeud = ""
      if (noeud.equals("")) {
        nouvNoeud = pos + "=" + x
      } else {
        nouvNoeud = noeud + ":" + pos + "=" + x
      }
      construireArbre(nouvNoeud, sousEnsemble)
    }
  }

  def enregistreArbre(debut:String,output: String): Unit = {
    if (scala.reflect.io.File(scala.reflect.io.Path(output)).exists) {
      val jj: Path = Path(output)
      jj.deleteRecursively()
      res.saveAsTextFile(output)
    } else {
      res.saveAsTextFile(output)
    }
  //  res.foreach(x=> println(x))
   // println("CREATION ... [DEBUT] " +debut)
   // println("CREATION ... [FIN] " +Calendar.getInstance().getTime.getMinutes+":"+Calendar.getInstance().getTime.getSeconds)
  }


}
