// Databricks notebook source

import scala.util.Random
import java.io._
import java.time._

// Notebook #2 has to set this to 8, we are setting
// it to 200 to "restore" the default behavior.
spark.conf.set("spark.sql.shuffle.partitions", 200)

// Make the username available to all other languages.
// "WARNING: use of the "current" username is unpredictable
// when multiple users are collaborating and should be replaced
// with the notebook ID instead.
val username = com.databricks.logging.AttributionContext.current.tags(com.databricks.logging.BaseTagDefinitions.TAG_USER);
spark.conf.set("com.databricks.training.username", username)

object DummyDataGenerator extends Runnable {
  var runner : Thread = null;
  val className = getClass().getName()
  val streamDirectory = s"dbfs:/tmp/$username/new-flights"
  val airlines = Array( ("American", 0.15), ("Delta", 0.17), ("Frontier", 0.19), ("Hawaiian", 0.21), ("JetBlue", 0.25), ("United", 0.30) )

  val rand = new Random(System.currentTimeMillis())
  var maxDuration = 3 * 60 * 1000 // default to a couple of minutes

  def clean() {
    System.out.println("Removing old files for dummy data generator.")
    dbutils.fs.rm(streamDirectory, true)
    if (dbutils.fs.mkdirs(streamDirectory) == false) {
      throw new RuntimeException("Unable to create temp directory.")
    }
  }

  def run() {
    val date = LocalDate.now()
    val start = System.currentTimeMillis()

    while (System.currentTimeMillis() - start < maxDuration) {
      try {
        val dir = s"/dbfs/tmp/$username/new-flights"
        val tempFile = File.createTempFile("flights-", "", new File(dir)).getAbsolutePath()+".csv"
        val writer = new PrintWriter(tempFile)

        for (airline <- airlines) {
          val flightNumber = rand.nextInt(1000)+1000
          val departureTime = LocalDateTime.now().plusHours(-7)
          val (name, odds) = airline
          val test = rand.nextDouble()

          val delay = if (test < odds)
            rand.nextInt(60)+(30*odds)
            else rand.nextInt(10)-5

          println(s"- Flight #$flightNumber by $name at $departureTime delayed $delay minutes")
          writer.println(s""" "$flightNumber","$departureTime","$delay","$name" """.trim)
        }
        writer.close()

        // wait a couple of seconds
        Thread.sleep(rand.nextInt(5000))

      } catch {
        case e: Exception => {
          printf("* Processing failure: %s%n", e.getMessage())
          return;
        }
      }
    }
    println("No more flights!")
  }

  def start(minutes:Int = 5) {
    maxDuration = minutes * 60 * 1000

    if (runner != null) {
      println("Stopping dummy data generator.")
      runner.interrupt();
      runner.join();
    }
    println(s"Running dummy data generator for $minutes minutes.")
    runner = new Thread(this);
    runner.start();
  }

  def stop() {
    start(0)
  }
}

DummyDataGenerator.clean()

displayHTML("Imported streaming logic...") // suppress output
