package part2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.{PersistentActor, SnapshotOffer}

import scala.collection.mutable

object Snapshots extends App {

  val system = ActorSystem("Snapshots")

  // COMMANDS
  case class ReceivedMessage(contents: String) // from contact
  case class SentMessage(contents: String) // to contact

  // EVENTS
  case class ReceivedMessageRecord(id: Int, contents: String)
  case class SentMessageRecord(id: Int, contents: String)

  object Chat {
    def props(owner: String, contact: String) = Props(new Chat(owner, contact))
  }

  class Chat(owner: String, contact: String) extends PersistentActor with ActorLogging {
    val MAX_MESSAGES = 10
    var commandsWithoutCheckpoint = 0
    var currentMessageId = 0
    val lastMessages = new mutable.Queue[(String, String)]()

    override def persistenceId: String = s"$owner-$contact-chat"

    override def receiveCommand: Receive = {
      case ReceivedMessage(contents) =>
        persist(ReceivedMessageRecord(currentMessageId, contents)) { e =>
          log.info(s"Received message: $contents")
          maybeReplaceMessage(contact, contents)
          currentMessageId += 1
        }
      case SentMessage(contents) =>
        persist(SentMessageRecord(currentMessageId, contents)) { e =>
          log.info(s"Sent message: $contents")
          maybeReplaceMessage(owner, contents)
          currentMessageId += 1
        }
    }

    override def receiveRecover: Receive = {
      case ReceivedMessageRecord(id, contents) =>
        log.info(s"Recovered received messages with id $id: $contents")
        maybeReplaceMessage(contact, contents)
        currentMessageId = id
        maybeCheckpoint()
      case SentMessageRecord(id, contents) =>
        log.info(s"Recovered sent messages with id $id: $contents")
        maybeReplaceMessage(owner, contents)
        currentMessageId = id
        maybeCheckpoint()
      case SnapshotOffer(metadata, contents) =>
        log.info(s"Recovered snapshot: $metadata")
        contents.asInstanceOf[mutable.Queue[(String, String)]].foreach(lastMessages.enqueue(_))
    }

    def maybeReplaceMessage(sender: String, contents: String) = {
      if (lastMessages.size >= MAX_MESSAGES) lastMessages.dequeue()
      lastMessages.enqueue((sender, contents))
    }

    def maybeCheckpoint(): Unit = {
      commandsWithoutCheckpoint += 1
      if (commandsWithoutCheckpoint >= MAX_MESSAGES) {
        log.info("Saving checkpoint")
        saveSnapshot(lastMessages)
        commandsWithoutCheckpoint = 0
      }
    }
  }

  val chat = system.actorOf(Chat.props("Duppa", "Jappa"))

//  for (i <- 1 to 100000) {
//    chat ! ReceivedMessage(s"Akka rocks $i")
//    chat ! SentMessage(s"You fuckin' hippie $i")
//  }
}