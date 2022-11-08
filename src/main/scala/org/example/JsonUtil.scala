package org.example
import com.google.gson.Gson


object JsonUtil {
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    collection2Json()
  }
  def collection2Json():Unit={
    import com.fasterxml.jackson.module.scala.DefaultScalaModule
    import com.fasterxml.jackson.databind.ObjectMapper
    case class Student(name: String, age: Int, score: Double)
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val s1 = Student("a1", 10, 99.5)
    val s2 = Student("a2", 12, 89.3)
    //不能单独序列号case class,但是放到map里就可以
    val m = Map("school" -> List(s1, s2))
    val json = mapper.writeValueAsString(m)
    println(json)
    println(mapper.writeValueAsString(List(1,2,3)))
    println(mapper.writeValueAsString(Set(1,2,3)))

  }

  def caseClass2Json():Unit={
    // gson可以转case class但是不能转map,list,set
    val gson = new Gson
    val jsonString = gson.toJson(Person("wsy", 2))
    println(jsonString)
    val person=gson.fromJson(jsonString,classOf[Person])
    println(person)
  }

  def parseJson():Unit={
    //解析json
    val jsonStr = """{"school":[{"name":"a1","age":10,"score":99.5},{"name":"a2","age":12,"score":89.3}]}"""
    import com.google.gson.{JsonObject, JsonParser}
    import scala.collection.JavaConversions._
    val obj = new JsonParser().parse(jsonStr).asInstanceOf[JsonObject]
    case class Student(name: String, age: Int, score: Double)
    val students = obj.get("school").getAsJsonArray.iterator().toList.map{j=>
      val name=j.asInstanceOf[JsonObject].get("name").getAsString
      val age=j.asInstanceOf[JsonObject].get("age").getAsInt
      val score=j.asInstanceOf[JsonObject].get("age").getAsDouble
      Student(name, age, score)
    }
    println(students)

  }



}
