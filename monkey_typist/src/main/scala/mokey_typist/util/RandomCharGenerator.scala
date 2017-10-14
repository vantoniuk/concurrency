package mokey_typist.util

import scala.util.{Random, Try}

/**
  * Interface for random char generator
  */
trait RandomCharGenerator {
  def getNext: Try[Char]
}

class RandomCharGeneratorImpl(random: Random) extends RandomCharGenerator {
  def getNext: Try[Char] = Try(random.nextPrintableChar())
}

trait RandomCharGeneratorFactory {
  def get: RandomCharGenerator
}

class DefaultRandomCharGeneratorFactory extends RandomCharGeneratorFactory {
  def get: RandomCharGenerator = new RandomCharGeneratorImpl(new Random())
}
