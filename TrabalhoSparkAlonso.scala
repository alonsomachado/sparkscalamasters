// Databricks notebook source
// DBTITLE 1,Ler Core_Dataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._
import java.sql.Date

case class Funcionario(
                    employeeName:String,
                    employeeNumber:Long,
                    state:String,
                    zip:Integer,
                    dob:Date,
                    age:Integer,
                    sex:String,
                    maritalDesc:String,
                    citizenDesc:String,
                    hispanicLatino:String,
                    raceDesc:String,
                    dateOfHire:Date,
                    dateOfTermination:Option[Date],
                    reasonForTerm:String,
                    employementStatus:String,
                    department:String,
                    position:String,
                    payRate:Double,
                    managerName:String,
                    employeeSource:String,
                    performanceScore:String)

val funcionarios = spark.read.option("header","true").option("delimiter", ";")
     //.option("timestampFormat", "MM/dd/yyyy HH:mm:ss.SSSXXX") 
    //.option("dateFormat", "MM/dd/yyyy") //Não funciona 
    //Resolvi fazer o Casting direto na leitura Data por Data usando to_date no lugar de .cast(DateType) com o format acima pois dava erros
    .csv("/FileStore/tables/core_dataset.csv")
    //.withColumn("employeeName", $"employeeName".cast(String))
    .withColumn("employeeNumber", $"employeeNumber".cast(LongType))
    //.withColumn("state", $"state".cast(String))
    .withColumn("zip", $"zip".cast(IntegerType))
    .withColumn("dob", to_date($"dob","MM/dd/yyyy")) //.withColumn("dob", $"dob".cast(DateType))
    .withColumn("age", $"age".cast(IntegerType))
    //.withColumn("sex", $"sex".cast(String))
    //.withColumn("maritalDesc", $"maritalDesc".cast(String))
    //.withColumn("citizenDesc", $"citizenDesc".cast(String))
    //.withColumn("hispanicLatino", $"hispanicLatino".cast(String))
    //.withColumn("raceDesc", $"raceDesc".cast(String))
    .withColumn("dateOfHire", to_date($"dateOfHire","MM/dd/yyyy")) 
    .withColumn("dateOfTermination", to_date($"dateOfTermination","MM/dd/yyyy")) 
    //.withColumn("reasonForTerm", $"reasonForTerm".cast(String))
    //.withColumn("employementStatus", $"employementStatus".cast(String))
    //.withColumn("department", $"department".cast(String))
    //.withColumn("position", $"position".cast(String))
    .withColumn("payRate", $"payRate".cast(DoubleType))
    //.withColumn("managerName", $"managerName".cast(String))
    //.withColumn("employeeSource", $"employeeSource".cast(String))
    //.withColumn("performanceScore", $"performanceScore".cast(String))
    .as[Funcionario] //.cache
funcionarios.show(25, false)
funcionarios.printSchema()

// COMMAND ----------

// DBTITLE 1,Ler Recruiting_Cost Dataset
//employmentSource,January,February,March,April,May,June,July,August,September,October,November,December,Total
case class Recruitcost(
                    employmentSource:String,
                    January:Integer,
                    February:Integer,
                    March:Integer,
                    April:Integer,
                    May:Integer,
                    June:Integer,
                    July:Integer,
                    August:Integer,
                    September:Integer,
                    October:Integer,
                    November:Integer,
                    December:Integer,
                    Total:Integer)
val recruitcosts = spark.read.option("header","true").option("delimiter", ",") 
    .csv("/FileStore/tables/recruiting_costs.csv")
    .withColumn("January", $"January".cast(IntegerType))
    .withColumn("February", $"February".cast(IntegerType))
    .withColumn("March", $"March".cast(IntegerType))
    .withColumn("April", $"April".cast(IntegerType))
    .withColumn("May", $"May".cast(IntegerType))
    .withColumn("June", $"June".cast(IntegerType))
    .withColumn("July", $"July".cast(IntegerType))
    .withColumn("August", $"August".cast(IntegerType))
    .withColumn("September", $"September".cast(IntegerType))
    .withColumn("October", $"October".cast(IntegerType))
    .withColumn("November", $"November".cast(IntegerType))
    .withColumn("December", $"December".cast(IntegerType))
    .withColumn("Total", $"Total".cast(IntegerType))
    .as[Recruitcost]
recruitcosts.show

// COMMAND ----------

// DBTITLE 1,Ler Dataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._
import java.sql.Date

case class Salario(position:String,
                    min:Long,
                    mid:Long,
                    max:Long,
                    min_h:Double,
                    mid_h:Double,
                    max_H:Double)

   val salarios =  spark.read.option("header","true").option("delimiter",",")//.option("timestampFormat", "MM/dd/yyyy") 
    .csv("/FileStore/tables/salary_grid.csv")
    .withColumn("min", $"min".cast(LongType))
    .withColumn("mid", $"mid".cast(LongType))
    .withColumn("max", $"max".cast(LongType))
    .withColumn("min_h", $"min_h".cast(DoubleType))
    .withColumn("mid_h", $"mid_h".cast(DoubleType))
    .withColumn("max_H", $"max_H".cast(DoubleType))
    .as[Salario]

salarios.show

// COMMAND ----------

// DBTITLE 1,Ex1
//funcionarios.where($"sex" === "Female").select(min($"payRate").as("Minimo"), avg($"payRate").as("Media"), max($"payRate").as("Maximo")).show
//funcionarios.where($"sex" === "Male").select(min($"payRate").as("Minimo"), avg($"payRate").as("Media"), max($"payRate").as("Maximo")).show
val ex1 = funcionarios.groupBy(lower($"sex").as("gender"))
.agg(min($"payRate").as("Salario Minimo p/dia"),  bround(avg($"payRate"),2).as("Salario Medio p/dia"), max($"payRate").as("Salario Maximo p/dia"))
ex1.show

// COMMAND ----------

// DBTITLE 1,Ex2
val ex2where =  funcionarios.where($"dateOfTermination".isNotNull)
    .withColumn("tempodetrabalho", datediff($"dateOfTermination",$"dateOfHire")/365)
    .sort($"tempodetrabalho".asc)
//exer2.groupBy($"department","Diferenca").show
val exer2 = ex2where.groupBy("department")
           .agg(avg($"tempodetrabalho"))
           .orderBy($"avg(tempodetrabalho)".desc)
           .withColumn("Tempo de Trabalho em Anos",bround($"avg(tempodetrabalho)" , 2)) //Arredondar o valor que na realidade parece cortar e não arredondar 
           .drop($"avg(tempodetrabalho)")
exer2.show

// COMMAND ----------

// DBTITLE 1,Ex3
/*val contratacoesanomes = funcionarios.groupBy(year($"dateOfHire").as("year"), month($"dateOfHire").as("monthnumber")).count
.sort($"year".desc, $"monthnumber".desc)
.withColumn("month", 
            when($"monthnumber" === "1", "Janeiro")
            .when($"monthnumber" === "2", "Fevereiro")
            .when($"monthnumber" === "3", "Março")
            .when($"monthnumber" === "4", "Abril")
            .when($"monthnumber" === "5", "Maio")
            .when($"monthnumber" === "6", "Junho")
           .when($"monthnumber" === "7", "Julho")
            .when($"monthnumber" === "8", "Agosto")
            .when($"monthnumber" === "9", "Setembro")
            .when($"monthnumber" === "10", "Outubro")
            .when($"monthnumber" === "11", "Novembro")
            .when($"monthnumber" === "12", "Dezembro"))
            .drop($"monthnumber") //Retiro a coluna com Strings
.withColumnRenamed("count","contratados")
contratacoesanomes.show*/

val countryValue = udf{(checknull: String, value: Long) =>
  if(checknull == null) value else 0
}

//val countryFuncs = countries.map{country => (dataFrame: DataFrame) => dataFrame.withColumn(country, countryValue(lit(country), df("value"))) }

val ex3pivot = funcionarios.groupBy(year($"dateOfHire").as("Ano")).pivot(month($"dateOfHire").as("monthnumber")).count
          .na.fill(0)
          //.map { x => when(x.getAs[Long](0) === null, 0)  }
          //.map { x => when(x.get(0) === null, 0)  }
          .sort($"ano".desc)

val afterpivot = ex3pivot.withColumnRenamed("1", "Janeiro")
.withColumnRenamed("2", "Fevereiro")
.withColumnRenamed("3", "Março")
.withColumnRenamed("4", "Abril")
.withColumnRenamed("5", "Maio")
.withColumnRenamed("6", "Junho")
.withColumnRenamed("7", "Julho")
.withColumnRenamed("8", "Agosto")
.withColumnRenamed("9", "Setembro")
.withColumnRenamed("10", "Outubro")
.withColumnRenamed("11", "Novembro")
.withColumnRenamed("12", "Dezembro")
            
afterpivot.show

// COMMAND ----------

// DBTITLE 1,EX4
val ex4 = funcionarios
        .where($"employementStatus" === "Active")
        .select($"employeeName",$"dateOfHire")
        .sort($"dateOfHire".asc)
        .withColumnRenamed("dateOfHire","Funcionário desde")
        .withColumnRenamed("employeeName","Funcionário")
ex4.show

// COMMAND ----------

// DBTITLE 1,EX5
//3 Maiores tipos de Contratacao
val tipoContratacao = funcionarios.groupBy($"employeeSource").count.where($"employeeSource" =!= "Employee Referral") //.limit(3) //Retirei a indicacao

//3 Maiores CUSTOS
val custo = recruitcosts.select($"employmentSource",$"Total").where($"Total" =!= 0).sort($"Total".desc).limit(3) 

//val innerjoin = recruitcosts.join(tipoContratacao,tipoContratacao("employeeSource") === recruitcosts("employmentSource"))

val innerjoin = custo.join(tipoContratacao,tipoContratacao("employeeSource") === custo("employmentSource"))

val ex5avg = innerjoin.withColumn("Custo Médio de Contratação", $"Total"/$"count") 

val ex5 = ex5avg.drop($"January").drop($"January").drop($"January")
        .drop($"January").drop($"February").drop($"March")
        .drop($"April").drop($"May").drop($"June")
        .drop($"July").drop($"August").drop($"September")
        .drop($"October").drop($"November").drop($"December")
        .drop($"employeeSource")//.drop($"Total").drop($"count")

ex5.show

// COMMAND ----------

// DBTITLE 1,EX6
//funcionarios.show
val linhacomtempocont = funcionarios.withColumn("tempoContrato",datediff($"dateOfTermination",$"dateOfHire"))//.show
val ex6 = linhacomtempocont.groupBy(lower($"sex").as("Genero")).avg("tempoContrato").as("Duração Média de Contrato")
//val ex6debug = linhacomtempocont.groupBy($"sex").avg("tempoContrato").show
ex6.show

// COMMAND ----------

// DBTITLE 1,EX7
case class PorcentagemPorMotivo(reasonForTerm:String,
                    count:Long,
                    Porcentagem:Double)

val ex7where = funcionarios.where($"reasonForTerm" =!= "N/A - still employed").where($"reasonForTerm" =!= "N/A - Has not started yet") 
//Retirar Funcionarios Ativos e Contratacoes Novas         
val ex7groupby = ex7where.groupBy($"reasonForTerm").count

val ex7total = ex7groupby.agg(sum($"count")).first.get(0) //Unico jeito que consegui pegar uma variável count e deixar ela utilizável para posteriormente fazer médias

val exer7 = ex7groupby.withColumn("Porcentagem", bround(($"count"/ex7total)*100 , 2) )
            .withColumn("Pct", concat($"Porcentagem", lit("%")))
            //.withColumnRenamed("reasonForTerm","Motivos da Saída")
            //.withColumnRenamed("count","Qnt").show
            //format_string("%s Pct"  , ($"count"/total)*100) 
            //df.withColumn("nome", format_string("%s Costa", $"nome"))
.as[PorcentagemPorMotivo]
exer7.withColumnRenamed("reasonForTerm","Motivos da Saída").withColumnRenamed("count","Qnt").show
//exer7.printSchema()

// COMMAND ----------

// DBTITLE 1,Ex8
val ex8enum = funcionarios.withColumn("performanceScore2", when($"performanceScore" === "Exceptional", 6)
            .when($"performanceScore" === "Exceeds", 5)
            .when($"performanceScore" === "Fully Meets", 4)
            .when($"performanceScore" === "90-day meets", 3)
            .when($"performanceScore" === "Needs Improvement", 2)
            .when($"performanceScore" === "N/A- too early to review", 1)
            .when($"performanceScore" === "PIP", 0))
            .drop($"performanceScore") //Retiro a coluna com Strings


val exer8 = ex8enum.groupBy($"department").agg(avg($"performanceScore2").as("performanceScore")).sort($"performanceScore".desc)
exer8.show

// COMMAND ----------

// DBTITLE 1,EX9
//Considere o dataset recruiting_costs. Crie um resumo deste dataset que contenha os custos por trimestre
case class CustosTrimestre(employmentSource:String,
                           Q1:Integer,
                           Q2:Integer,
                           Q3:Integer,
                           Q4:Integer)

val exer9 = recruitcosts 
    .withColumn("Q1", $"January"+$"February"+$"March") 
    .withColumn("Q2", $"April"+$"May"+$"June")
    .withColumn("Q3", $"July"+$"August"+$"September") 
    .withColumn("Q4", $"October"+$"November"+$"December")
    .drop($"January").drop($"February").drop($"March")
    .drop($"April").drop($"May").drop($"June")
    .drop($"July").drop($"August").drop($"September")
    .drop($"October").drop($"November").drop($"December")
    .drop($"Total")
.as[CustosTrimestre]

exer9.printSchema()
exer9.withColumnRenamed("employmentSource","Metodo de Contratação").show



// COMMAND ----------

// DBTITLE 1,EX10
val salarios_grid = salarios.drop($"min_h")
                    .drop($"mid_h")
                    .drop($"max_H")
                    .withColumnRenamed("Position","PositionToDrop")

val exer10 = funcionarios.groupBy($"position")
 .agg( min($"payRate").as("Salario Minimo p/ Hora"), avg($"payRate").as("Salario Medio p/ Hora"), max($"payRate").as("Salario Maximo p/ Hora") )
//min_h mid_h max_H são pouco significativos então coloquei nomes melhores

val ex10juntar = exer10.join(salarios_grid,salarios_grid("PositionToDrop") === exer10("position"))
.drop($"PositionToDrop").show




//.drop("position").show //Dropa as duas colunas do Join
//exer10.show
//salarios_grid.show

// COMMAND ----------

// DBTITLE 1,Ex 11 PROVA
val salarios_grid = salarios.drop($"min_h")
                    .drop($"mid_h")
                    .drop($"max_H")
                    .withColumnRenamed("Position","PositionToDrop")

//val exer11count = funcionarios.groupBy($"position").count.withColumnRenamed("position","positionToDrop2")

//val exer11avg = funcionarios.groupBy($"position")
// .agg( min($"payRate").as("Salario Minimo p/ Hora"), avg($"payRate").as("Salario Medio p/ Hora"), max($"payRate").as("Salario Maximo p/ Hora"), count($"payRate") )
//min_h mid_h max_H são pouco significativos então coloquei nomes melhores

//val ex11juntar = exer11avg.join(exer11count,exer11count("positionToDrop2") === exer11avg("position"))

val ex11juntarfinal = funcionarios.join(salarios_grid,salarios_grid("PositionToDrop") === funcionarios("position"),"left_anti")
.groupBy($"position")
.agg( min($"payRate").as("Salario Minimo p/ Hora"), avg($"payRate").as("Salario Medio p/ Hora"), max($"payRate").as("Salario Maximo p/ Hora"), count($"payRate") )
.withColumnRenamed("count(payRate)","Numero de Funcionarios na Posicao").show


