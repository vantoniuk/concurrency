package orchestrator.di

import javax.inject.Singleton

import akka.actor.ActorSystem
import com.google.inject.{AbstractModule, Provides}
import orchestrator.pipeline._

import scala.concurrent.ExecutionContext

class DefaultModule extends AbstractModule {
  def configure() = {}

  @Provides @Singleton
  def executionContext(system: ActorSystem): ExecutionContext = system.dispatcher

  @Provides @Singleton
  def actorSystem: ActorSystem = ActorSystem("orchestrator-system")

  @Provides @Singleton
  def failureDetector: FailureDetector = DefaultFailureDetector

  @Provides @Singleton
  def loadBalancer: LoadBalancer = RoundRobinLoadBalancer

  @Provides @Singleton
  def jobStateKeeper: JobStateKeeper = InMemoryJobStateKeeper

  @Provides @Singleton
  def orchestratorMaster(failureDetector: FailureDetector,
                         stateKeeper: JobStateKeeper,
                         loadBalancer: LoadBalancer,
                         ctx: ExecutionContext
                        ): Master = new Master(failureDetector, stateKeeper, loadBalancer)(ctx)
}
