  package Donnees.Creation

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

  object Structure {


    def changerStructure(pos:Int,texte:String): String ={
      var resultat=""
          val list:List[String] = texte.split(",").toList
      for(i<-0 to list.size-1){
        if(resultat.equals("")){
          resultat = i+"#"+pos+":"+list(i)
        }else{
          resultat = resultat+","+i+"#"+pos+":"+list(i)
        }
      }

      return resultat.toString
    }


    def structurerEnsemble(teste: String): String = {
      val a: Array[String] = teste.split(" ").toArray
      val b: ArrayBuffer[String] = ArrayBuffer[String]()
      for (i <- 0 to a.size - 2) {
        b += i + ":" + a(i) + "_" + a(a.size - 1)
      }
      var result = ""
      for (i <- 0 to b.size - 1) {
        if (result.equals("")) {
          result = b(i)
        } else {
          result = result + " " + b(i)
        }
      }
      return result
    }


    def separerValeurs(cle:String,texte:String):String={
      var resultat:String=""
      val array=texte.split(",")
      for(i<-0 to array.size-1){
        if(resultat.equals("")){
          resultat = cle+":"+array(i)
        }else{
          resultat = resultat+" "+cle+":"+array(i)
        }
      }
      return resultat
    }




  }
