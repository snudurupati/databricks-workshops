// Databricks notebook source
// MAGIC %python
// MAGIC def dbTest(id, expected, result):
// MAGIC   assert str(expected) == str(result), "{} does not equal expected {}".format(result, expected)

// COMMAND ----------

def dbTest[T](id: String, expected: T, result: => T, message: String = ""): Unit = {
  assert(result == expected, message)
}
displayHTML("Imported Test Library...") // suppress output
