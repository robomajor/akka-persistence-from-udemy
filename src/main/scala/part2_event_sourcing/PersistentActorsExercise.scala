package part2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

import scala.collection.mutable

object PersistentActorsExercise extends App {

  case class Vote(citizenPID: String, candidate: String)

  class VotingStation extends PersistentActor with ActorLogging {

    val citizens: mutable.Set[String] = new mutable.HashSet[String]()
    val poll: mutable.Map[String, Int] = new mutable.HashMap[String, Int]()

    override def persistenceId: String = "simple-voting-station"

    override def receiveCommand: Receive = {
      case vote @ Vote(citizenPID, candidate) =>
        if (!citizens.contains(vote.citizenPID))
        persist(vote) { _ =>
          log.info(s"Persisted: $vote")
          handleInternalStateChange(citizenPID, candidate)
        }
      case "print" => log.info(s"Current state: \nCitizens: $citizens\nPolls: $poll")
    }

    def handleInternalStateChange(citizenPID: String, candidate: String): Unit = {
        citizens.add(citizenPID)
        val votes = poll.getOrElse(candidate, 0)
        poll.put(candidate, votes + 1)
    }

    override def receiveRecover: Receive = {
      case vote @ Vote(citizenPID, candidate) =>
        log.info(s"Recovered: $vote")
        handleInternalStateChange(citizenPID, candidate)
    }
  }

  val system = ActorSystem("PersistentActorsExercise")
  val votingStation = system.actorOf(Props[VotingStation], "votingStation")

  val votesMap = Map[String, String](
    "Alice" -> "Martin",
    "Bob" -> "Roland",
    "Charlie" -> "Martin",
    "David" -> "Jonas",
    "Daniel" -> "Martin"
  )

  votesMap.keys.foreach { citizen =>
    votingStation ! Vote(citizen, votesMap(citizen))
  }
  votingStation ! "print"
}
