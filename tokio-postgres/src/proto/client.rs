use futures::sync::mpsc;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;

use disconnected;
use error::Error;
use proto::connection::Request;
use proto::prepare::PrepareFuture;
use types::Type;

pub struct PendingRequest {
    sender: mpsc::UnboundedSender<Request>,
    messages: Vec<u8>,
}

impl PendingRequest {
    pub fn send(self) -> Result<mpsc::Receiver<Message>, Error> {
        let (sender, receiver) = mpsc::channel(0);
        self.sender
            .unbounded_send(Request {
                messages: self.messages,
                sender,
            })
            .map(|_| receiver)
            .map_err(|_| disconnected())
    }
}

pub struct Client {
    sender: mpsc::UnboundedSender<Request>,
}

impl Client {
    pub fn new(sender: mpsc::UnboundedSender<Request>) -> Client {
        Client { sender }
    }

    pub fn prepare(&mut self, name: String, query: &str, param_types: &[Type]) -> PrepareFuture {
        let mut buf = vec![];
        let request = frontend::parse(&name, query, param_types.iter().map(|t| t.oid()), &mut buf)
            .and_then(|()| frontend::describe(b'S', &name, &mut buf))
            .and_then(|()| Ok(frontend::sync(&mut buf)))
            .map(|()| PendingRequest {
                sender: self.sender.clone(),
                messages: buf,
            })
            .map_err(Into::into);

        PrepareFuture::new(request, self.sender.clone(), name)
    }
}
