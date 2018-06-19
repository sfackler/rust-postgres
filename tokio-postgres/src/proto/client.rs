use futures::sync::mpsc;
use futures::Poll;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use want::Giver;

use disconnected;
use error::Error;
use proto::connection::Request;
use proto::prepare::PrepareFuture;
use types::Type;

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

    pub fn prepare(&mut self, name: String, query: &str, param_types: &[Type]) -> PrepareFuture {
        let mut buf = vec![];
        let receiver = frontend::parse(&name, query, param_types.iter().map(|t| t.oid()), &mut buf)
            .and_then(|()| frontend::describe(b'S', &name, &mut buf))
            .and_then(|()| Ok(frontend::sync(&mut buf)))
            .map_err(Into::into)
            .and_then(|()| self.send(buf));

        PrepareFuture::new(self.sender.clone(), receiver, name)
    }

    fn send(&mut self, messages: Vec<u8>) -> Result<mpsc::Receiver<Message>, Error> {
        let (sender, receiver) = mpsc::channel(0);
        self.giver.give();
        self.sender
            .unbounded_send(Request { messages, sender })
            .map(|_| receiver)
            .map_err(|_| disconnected())
    }
}
