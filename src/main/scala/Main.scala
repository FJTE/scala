
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

final case class Person(firstName: String, lastName: String)

object Main extends InitSpark {

  def main(args: Array[String]) = {

    import spark.implicits._

    val fileName = "Citywide_Payroll_Data__Fiscal_Year_.csv"

    val version = spark.version
    println("SPARK VERSION = " + version)

    println("Reading from file:" + fileName )
    val persons = getPayRollFile(fileName)
    persons.show(3)

    val sumPayroll= this.getTotalPayroll(persons)
    println(f"Base Salary Sumatory: $sumPayroll%.2f")

    val flagFile=this.saveIntoFile(persons)
    close
  }

 

   def getPayRollFile(file: String): Dataset[Person] = {

    import spark.implicits._

    return reader.csv(file).as[Person]

  }

 

  def getTotalPayroll(persons: Dataset[Person]): Double ={
     val sumSalary = persons.agg(sum("BaseSalary")).first.get(0).asInstanceOf[Double]
     return sumSalary;
  }

  def saveIntoFile(persons: Dataset[Person]): Boolean ={
    val fileOuput ="NYCOutput.csv"

    println("******Starting to SAVE Datasets INTO a new external file *******************")

    //persons.write.csv("NYCOutput.csv")
    persons.write.parquet(fileOuput)
    println("**** END OF SAVING IN CSV Format...");

    return false;

  }

 

  /* def listFromColumn(data: Person, column: String): Array[String] = {

    data.select(column).rdd.map(r => r(0)).collect().map(a => a.toString()).toArray

  }*/

 

}