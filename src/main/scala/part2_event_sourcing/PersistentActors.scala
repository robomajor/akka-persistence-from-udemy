package part2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

import java.util.Date

object PersistentActors extends App {

  val system = ActorSystem("PersistentActors")

  // COMMANDS
  case class Invoice(recipient: String, date: Date, amount: Int)
  case class InvoiceBulk(invoices: List[Invoice])

  // EVENTS
  case class InvoiceRecorded(id: Int, recipient: String, date: Date, amount: Int)

  // SPECIAL MESSAGES
  case object Shutdown

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
      case InvoiceBulk(invoices) =>
        val invoiceIds = latestInvoiceId to (latestInvoiceId + invoices.size)
        val events = invoices.zip(invoiceIds).map { pair =>
          val id = pair._2
          val invoice = pair._1
          InvoiceRecorded(id, invoice.recipient, invoice.date, invoice.amount)
        }
        persistAll(events) { e =>
          latestInvoiceId += 1
          totalAmount += e.amount
          log.info(s"Persisted $e as invoice #${e.id}, for total amount $totalAmount")
        }
      case Shutdown => context.stop(self)
      case "print" => log.info(s"Latest invoice id: $latestInvoiceId, total amount: $totalAmount")
    }

    override def receiveRecover: Receive = {
      // follow the logic in the persist steps of receiveCommand
      case InvoiceRecorded(id, _, _, amount) =>
        latestInvoiceId = id
        totalAmount += amount
    }

    // this method is called if persisting failed - actor will be stopped
    override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Fail to persist $event because of $cause")
      super.onPersistFailure(cause, event, seqNr)
    } // best practise - start the actor again after a while (use backoff supervisor)

    // called if the journal fails to persist the event - actor is resumed
    override def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Persist rejected for $event because of $cause")
      super.onPersistRejected(cause, event, seqNr)
    }
  }

  val simpleAccountant = system.actorOf(Props[Accountant], "simpleAccountant")

  val newInvoices = for (i <- 1 to 5) yield Invoice("The Awesome Sofa Company", new Date, i * 2000)
//  simpleAccountant ! InvoiceBulk(newInvoices.toList)

  // NEVER EVER CALL persist OR persistAll FROM FUTURES
}
