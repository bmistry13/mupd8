/**
 * Copyright 2011-2012 @WalmartLabs, a division of Wal-Mart Stores, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.walmartlabs.mupd8

import java.util.ArrayList

import scala.Option.option2Iterable
import scala.actors.Actor
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.breakOut

import org.scale7.cassandra.pelops.Mutator

import com.walmartlabs.mupd8.Misc.SlateObject
import com.walmartlabs.mupd8.Misc.argParser
import com.walmartlabs.mupd8.Misc.isLocalHost
import com.walmartlabs.mupd8.Misc.writePID
import com.walmartlabs.mupd8.Mupd8Type.Mupd8Type
import com.walmartlabs.mupd8.application.Config
import com.walmartlabs.mupd8.application.binary

import grizzled.slf4j.Logging

case class Host(ip: String, hostname: String)

case class Performer(name: String,
  pubs: Vector[String],
  subs: Vector[String],
  mtype: Mupd8Type,
  ptype: Option[String],
  jclass: Option[String],
  wrapperClass: Option[String],
  slateBuilderClass: Option[String],
  workers: Int,
  cf: Option[String],
  ttl: Int,
  copy: Boolean)

object loadConfig {

  def isTrue(value: Option[String]): Boolean = {
    (value != null) && (value != None) && (value.get.toLowerCase != "false") && (value.get.toLowerCase != "off") && (value.get != "0")
  }

  def convertPerformers(pHashMap: java.util.HashMap[String, org.json.simple.JSONObject]) = {
    val performers = pHashMap.asScala.toMap
    def convertStrings(list: java.util.List[String]): Vector[String] = {
      if (list == null) Vector() else list.asScala.toArray.map(p => p)(breakOut)
    }

    performers.map(p =>
      Performer(
        name = p._1,
        pubs = convertStrings(p._2.get("publishes_to").asInstanceOf[ArrayList[String]]),
        subs = convertStrings(p._2.get("subscribes_to").asInstanceOf[ArrayList[String]]),
        mtype = Mupd8Type.withName(p._2.get("mupd8_type").asInstanceOf[String]),
        ptype = Option(p._2.get("type").asInstanceOf[String]),
        jclass = Option(p._2.get("class").asInstanceOf[String]),
        wrapperClass = { Option(p._2.get("wrapper_class").asInstanceOf[String]) },
        slateBuilderClass = Option(p._2.get("slate_builder").asInstanceOf[String]),
        workers = if (p._2.get("workers") == null) 1.toInt else p._2.get("workers").asInstanceOf[Number].intValue(),
        cf = Option(p._2.get("column_family").asInstanceOf[String]),
        ttl = if (p._2.get("slate_ttl") == null) Mutator.NO_TTL else p._2.get("slate_ttl").asInstanceOf[Number].intValue(),
        copy = isTrue(Option(p._2.get("clone").asInstanceOf[String]))))(breakOut)
  }

}

// A factory that constructs a SlateUpdater that runs an Updater.
// The SlateUpdater expects to be accompanied by a ByteArraySlateBuilder as
// its SlateBuilder so that the slate object indeed stays the raw byte[].
class UpdaterFactory[U <: binary.Updater](val updaterType : Class[U]) {
  val updaterConstructor = updaterType.getConstructor(classOf[Config], classOf[String])
  def construct(config : Config, name : String) : binary.SlateUpdater = {
    val updater = updaterConstructor.newInstance(config, name)
    val updaterWrapper = new binary.SlateUpdater() {
      override def getName() = updater.getName()
      override def update(util : binary.PerformerUtilities, stream : String, k : Array[Byte], v : Array[Byte], slate : SlateObject) = {
        updater.update(util, stream, k, v, slate.asInstanceOf[Array[Byte]])
      }
      override def getDefaultSlate() : Array[Byte] = Array[Byte]()
    }
    updaterWrapper
  }
}

object Mupd8Main extends Logging {
  
  var RUNTIME : AppRuntime = null;
  var STATICINFO : AppStaticInfo = null;

  def main(args: Array[String]) {
    info("Mupd8 is starting ...")
    Thread.setDefaultUncaughtExceptionHandler(new Misc.TerminatingExceptionHandler())

    val syntax = Map("-s" -> (1, "Sys config file name"),
      "-a" -> (1, "App config file name"),
      "-d" -> (1, "Unified-config directory name"),
      "-sc" -> (1, "Mupd8 source class name"),
      "-sp" -> (1, "Mupd8 source class parameters separated by comma"),
      "-to" -> (1, "Stream to which data from the URI is sent"),
      "-threads" -> (1, "Optional number of execution threads, default is 5"),
      "-shutdown" -> (0, "Shut down the Mupd8 App"),
      "-pidFile" -> (1, "Optional PID filename"))

    val argMap = argParser(syntax, args)
    if (argMap.get("-s").size != argMap.get("-a").size || argMap.get("-s").size == argMap.get("-d").size) {
      error("Command parameter \"-s, -a, -d\" error, exiting...")
      System.exit(-1)
    }
    val threads = argMap.get("-threads").map(_.head.toInt).getOrElse(5)
    val shutdown = !argMap.get("-shutdown").isDefined
    val appStatic = new AppStaticInfo(argMap.get("-d").map(_.head), argMap.get("-a").map(_.head), argMap.get("-s").map(_.head))
    argMap.get("-pidFile") match {
      case None => writePID("mupd8.log")
      case Some(x) => writePID(x.head)
    }

    val runtime = new AppRuntime(0, threads, appStatic)
    RUNTIME = runtime;
    STATICINFO = appStatic;
    
    if (runtime.ring != null) {
      if (appStatic.sources.size > 0) {
        startSources(appStatic, runtime)
      } else if (argMap.contains("-to") && argMap.contains("-sc")) {
        info("start source from cmdLine")
        runtime.startSource("cmdLineSource", argMap("-to").head, argMap("-sc").head, seqAsJavaList(argMap("-sp").head.split(',')))
      }
    } else {
      error("Mupd8Main: no hash ring found, exiting...")
    }
    info("Initialization is done")    
  }

  def startSources(app: AppStaticInfo, runtime: AppRuntime) {
    class AskPermit(sourceName: String) extends Actor {
      def act() {
        val client: MessageServerClient = new MessageServerClient(runtime, 1000)
        client.sendMessage(AskPermitToStartSourceMessage(sourceName, runtime.self))
      }
    }
    var sourcesStarted : Set[String] = Set()
    val ssources = app.sources.asScala
    info("start source from sys cfg")
    ssources.foreach { source =>
      if (isLocalHost(source.get("host").asInstanceOf[String])) {
        info("Start source - " + source + " on this node")
        val sourceName = source.get("name").asInstanceOf[String]
        val askPermit = new AskPermit(sourceName)
        askPermit.start
        sourcesStarted += sourceName
      }
    }

    class DynamicSource(threadName: String) extends Actor {
      
     var attemptedSourceName:String = null; 
     
      def act() {
        
        info("Dynamic soruces ["+threadName+"] background Thread started..now");
        var count: Int = 0;
        while (true) {
          try {
            try {
              Thread.sleep(5000);
            } catch {
              case exp: InterruptedException =>
                error("Interuppted Exception while starting dynamic sources ignore");
            }
            info("Dynamic soruces ["+threadName+"] started so far on this node is " + count);
	         val newSources = app.sources.asScala           
	         newSources.foreach { source =>
	              if (source != null
	                && isLocalHost(source.get("host").asInstanceOf[String]) && !runtime.startedSourcesOnThisNode.contains(source.get("name").asInstanceOf[String]) ) {
	            	  info("Start Dynamic source ["+threadName+"]  after main soruce is started.- " + source + " on this node.")
	            	  val sourceName = source.get("name").asInstanceOf[String]
	            	  attemptedSourceName = sourceName;
	            	  // DO NOT ASK FOR PERMISIOSN JUST START FOR NOW NO MSG SERVER PERMISSION...
	            	  val started = runtime.startSource(sourceName)
	            	  // count the source started on this node
	            	  if(started &&  runtime.startedSources.get(sourceName) != null){
	            		  if(!sourcesStarted.contains(sourceName) && runtime.startedSourcesOnThisNode.contains(sourceName)){
	            			  sourcesStarted += sourceName;
	            			  count = count +1;
	            		  }
	            	  }
	            	  
	              }
	              attemptedSourceName = null;
	         }

          } catch {
            case w: Throwable => error("WARN Occured while starting Dynamic Soruces..it is ok to ignore next run will try again Source_Name ="+ attemptedSourceName, w);
          }
        }
      }
    }
    info("Trying to start DynamicSource_Thread background Thread");
    val dynamicSourceStarted = new DynamicSource("DynamicSource_Thread"); 
    dynamicSourceStarted.start;
    info("Trying to start DynamicSource_Thread background Thread start called");
  }
 
}
