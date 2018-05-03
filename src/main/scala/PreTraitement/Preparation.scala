package PreTraitement

import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.io.Path

object Preparation{

  def prepar(sc:SparkContext,args:Array[String]): RDD[String]={
    println("................................................... ")
    println("PREPARATION[DEBUT] " +Calendar.getInstance().getTime.getMinutes+":"+Calendar.getInstance().getTime.getSeconds)

    /***Recuperation des parametre du programme*/
    val input=args(0) // fichier d entre
    var output=args(1) // dossier de sortie
    var separateur=args(2) // separateur d attribut utilise
    var entete=args(3) // l existance d une entete (1 si oui, 0 sinon)
    var ID=args(4) // l existance d un ID d instance (1 si oui, 0 sinon)
    var attributNumerique =args(5)
    /***Fin*/

    val texte: RDD[String] = sc.textFile(input,5)

    val ensemblePrepare: RDD[String] = preparerEnsemble (texte, separateur, entete.toInt, ID.toInt)

    /** * RDD contenant l'ensemble de données d entrée préparé,
      * La transformation implique les points suivants :
      * 1) Separateur : SI c'est un espace, OK, sinon, le transformer en espace
      * 2) ID : SI un ID d'instance existe, le supprimer
      * 3) Entete : SI une entete existe, la supprimer
      */


    val ensembleStructure = ensemblePrepare
      .map (a => Donnees.Creation.Structure.structurerEnsemble(a) )


    /** * Changement de la structure de l'ensemble de données,
      * nos données seront structuré de la façon suivante :
      * ---------------------------------------------------
      * --positionAttribut:valeurAttribut_categorieClasse -
      * ---------------------------------------------------
      */

    val ensembleRegroupe = ensembleStructure
      .flatMap (a => a.split (" ") )
      .map (a => (a.split (":") (0).toInt, (a.split (":") (1) ) ) ) //1
      .reduceByKey (_+ "," + _) //2


    /** *Utiliser la positionAttribut afin de créer, en premier lieu(1) un RDD (clé,valeur),
      * chaque couple aura donc comme
      * Cle : positionAttribut
      * Valeur : valeurAttribut_categorieClasse
      * Ensuite(2), regrouper tous les couples par rapport a la clé,
      * autrement dit, les toutes les valeurAttribut_categorieClasse apartenant
      * au même attribur seront regroupées.
      */

var ensembleDiscret = ensembleRegroupe
  if(!attributNumerique.equals("")){
      val attributNum = attributNumerique.split(",")
    for(i<-0 to attributNum.size-1){


      val attribut = ensembleDiscret
        .filter { case (x, y) => (x==attributNum(i).toInt) }



      val interval = Donnees.Creation.Discretisation
        .discretisation(attributNum(i),attribut)



      val nouvelAtt = attribut.map{case(x,y)=>y}
        .flatMap{x => x.split(",")}
        .map(x => (x.split("_")(0).toDouble, x.split("_")(1)))
        .map{case(x,y) =>
        if(x > interval) (">"+interval+"_"+y)
        else
          ("<"+interval+"_"+y)
        }
     .map(x=> (attributNum(i).toInt,x)).reduceByKey(_+","+_)



      ensembleDiscret = ensembleDiscret.subtract(attribut)


      ensembleDiscret = ensembleDiscret ++ nouvelAtt

      println("PREPARATION[FIN] " +Calendar.getInstance().getTime.getMinutes+":"+Calendar.getInstance().getTime.getSeconds)

    }

  }


    val ensembleFinale=ensembleDiscret.sortByKey()
      .map{case(x,y) =>
      (separation(x,y.split(",")))
    }.flatMap(x=> x.split(","))
      .map(x=> (x.split(":")(0), x.split(":")(1)))
      .reduceByKey(_+","+_).sortByKey()
      .map{case(x,y) => (separation2(y.split(",")))}
      .zipWithIndex()
      .map{case(x,y) => (x,y.toInt)}


    val taille = (ensembleFinale.count()-1)
    val apprentissage = taille - (taille/3)



    val ensembleApprentissage = ensembleFinale
      .filter{case(x,y) =>  (y<apprentissage)}


    val ensembleTest = ensembleFinale
      .subtract(ensembleApprentissage)



  //  enregistrer(sc,input+"_test",ensembleTest.map{case(x,y)=>x})


    return ensembleFinale.map{case(x,y) => x}
  }
/*
  def enregistrer(sc:SparkContext,output:String,rdd:RDD[String]): Unit = {
    if (scala.reflect.io.File(scala.reflect.io.Path(output)).exists) {
      val jj: Path = Path(output)
      jj.deleteRecursively()
      sc.parallelize(rdd.collect()).saveAsTextFile(output)
    } else {
      sc.parallelize(rdd.collect()).saveAsTextFile(output)
    }
    println("PREPARATION[FIN] " +Calendar.getInstance().getTime.getMinutes+":"+Calendar.getInstance().getTime.getSeconds)
  }
*/

  def separation(id:Int,array: Array[String]): String ={
    var resultat=""
    for(i<-0 to array.size-1){
      if(resultat.equals("")){
        resultat = i+":"+array(i)
      }else{
        resultat =resultat+","+i+":"+array(i)
      }
    }
    return resultat
  }



  def separation2(array: Array[String]): String ={
    var resultat=""
    val valClasse = array(array.size-1).split("_")(1)
    for(i<-0 to array.size-1){
      if(resultat.equals("")){
        resultat =array(i).split("_")(0)
      }else{
        resultat =resultat+" "+array(i).split("_")(0)
      }
    }

    return resultat+" "+valClasse
  }





  def preparerEnsemble(rdd:RDD[String],separateur:String,entete:Int,ID:Int): RDD[String]= {
    var rdd1:RDD[String]=rdd

    /***Traitement du separateur s'il differe de l'espace*/
    if(!separateur.equals(" ")){
      rdd1=rdd1.map(x => x.replaceAll(separateur," "))
    }

    /*** Supression des succession d espace si elle existe !! */
    rdd1=rdd1.map(x => x.replaceAll("\\s"," "))
    /***Traitement de l'entete si elle existe*/
    if(entete==1) {
      rdd1 = traitementEntete(rdd1)
    }
    /***Traitement des identifiants d'instances s'ils éxistent*/
    if(ID==1) {
      rdd1 = rdd1.map( x=> traitementId(x) )
    }
    return rdd1
  }

  def traitementEntete(rdd:RDD[String]): RDD[String] ={
    val result = rdd.mapPartitionsWithIndex(
      (i,iterator)=>
        if(i==0 && iterator.hasNext){
          iterator.next
          iterator}
        else iterator)
    return result
  }

  def traitementId(instance:String): String ={
    var resultat=""
    val inst:List[String] = instance.split(" ").toList
    for(i<-1 to inst.size-1){
      if(resultat.equals("")){
        resultat=inst(i)
      }else{
        resultat=resultat+" "+inst(i)
      }
    }
    return resultat
  }

}
