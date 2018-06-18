use futures::sync::mpsc;
use futures::Poll;
use postgres_protocol::message::backend::Message;
use want::Giver;

use disconnected;
use error::Error;
use proto::connection::Request;

pub struct Client {
    sender: mpsc::UnboundedSender<Request>,
    giver: Giver,
}

impl Client {
    pub fn new(sender: mpsc::UnboundedSender<Request>, giver: Giver) -> Client {
        Client { sender, giver }
    }

    pub fn poll_ready(&mut self) -> Poll<(), Error> {
        self.giver.poll_want().map_err(|_| disconnected())
    }

    pub fn send(&mut self, messages: Vec<u8>) -> Result<mpsc::Receiver<Message>, Error> {
        let (sender, receiver) = mpsc::channel(0);
        self.giver.give();
        self.sender
            .unbounded_send(Request { messages, sender })
            .map(|_| receiver)
            .map_err(|_| disconnected())
    }
}
