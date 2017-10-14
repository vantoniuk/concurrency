package mokey_typist

import javax.inject.Singleton

import akka.actor.ActorSystem
import com.google.inject.{AbstractModule, Provides}
import mokey_typist.util.{DefaultRandomCharGeneratorFactory, RandomCharGeneratorFactory}

import scala.concurrent.ExecutionContext


class DefaultModule extends AbstractModule {
  def configure() = {}

  @Provides @Singleton
  def randomCharGeneratorFactory: RandomCharGeneratorFactory = {
    new DefaultRandomCharGeneratorFactory
  }

  @Provides @Singleton
  def executionContext(system: ActorSystem): ExecutionContext = system.dispatcher

  @Provides @Singleton
  def actorSystem: ActorSystem = ActorSystem("string-lookup-system")

  @Provides @Singleton
  def stringLookup(randomCharGeneratorFactory: RandomCharGeneratorFactory,
                   actorSystem: ActorSystem): StringLookup = {
    new ActorStringLookup(randomCharGeneratorFactory, actorSystem)
  }
}
