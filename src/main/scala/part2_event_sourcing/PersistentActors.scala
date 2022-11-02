package part2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

import java.util.Date

object PersistentActors extends App {

  val system = ActorSystem("PersistentActors")

  // COMMANDS
  case class Invoice(recipient: String, date: Date, amount: Int)

  // EVENTS
  case class InvoiceRecorded(id: Int, recipient: String, date: Date, amount: Int)

  class Accountant extends PersistentActor with ActorLogging {
    var latestInvoiceId = 0
    var totalAmount = 0

    override def persistenceId: String = "simple-accountant"

    override def receiveCommand: Receive = {
      case Invoice(recipient, date, amount) =>
      /*
        When you receive a command
        1) you create an EVENT to persist into the store
        2) you persist the event, the pass in a callback that will get triggered once the event is written
        3) we update the actor's state when the event has persisted
       */
        log.info(s"Receive invoice for amount: $amount")
        persist(InvoiceRecorded(latestInvoiceId, recipient, date, amount)) { e =>
          latestInvoiceId += 1
          totalAmount += amount
          log.info(s"Persisted $e as invoice #${e.id}, for total amount $totalAmount")
        }
      case "print" => log.info(s"Latest invoice id: $latestInvoiceId, total amount: $totalAmount")
    }

    override def receiveRecover: Receive = {
      // follow the logic in the persist steps of receiveCommand
      case InvoiceRecorded(id, _, _, amount) =>
        latestInvoiceId = id
        totalAmount += amount
    }
  }

  val simpleAccountant = system.actorOf(Props[Accountant], "simpleAccountant")

  for (i <- 1 to 10) {
    simpleAccountant ! Invoice("The Sofa Company", new Date, i * 1000)
  }
}
