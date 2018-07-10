import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
 

class DatasetFunSpec extends NYCBoardFuncSpecBasic{

  val fileName = "Citywide_Payroll_Data__Fiscal_Year_.csv"
  val persons = Main.getPayRollFile(fileName)
  //val dataset =  reader.csv(fileName)
  val column = "firstName"

  describe("Test case for NYC Board-Payroll") {
    it(" Should test create the Spark context ") (pending)
  }

  

  describe("Open file for create a Dataset") {
    describe("when read the data from file") {
      it("should have data") {
        assert(persons.collect().nonEmpty)
      }
    }
  }

  describe("Calculate for Payroll sumatorie"){
     describe("When the dataset Persons are ready"){
       it("take all the Persons and apply a sumatorie for PayRoll"){
         assume(persons.collect().nonEmpty)
         val sumPaiments = (Main.getTotalPayroll(persons))
         assert(sumPaiments > 0)
       }
     }
  }

  describe("Save Results in a csv file"){
     describe("Take Persons Dataset and write to disk"){
       assume(persons.collect().nonEmpty)
       it("Write to disk action"){
          val flagFile= Main.saveIntoFile(persons)
           flagFile shouldBe false
       }
     }
  }

}