use futures::sync::mpsc;
use postgres_protocol::message::backend::Message;

use error::Error;
use proto::connection::Request;
use proto::statement::Statement;

#[derive(StateMachineFuture)]
pub enum Prepare {
    #[state_machine_future(start)]
    Start {
        sender: mpsc::UnboundedSender<Request>,
        receiver: Result<mpsc::Receiver<Message>, Error>,
        name: String,
    },
    #[state_machine_future(ready)]
    Finished(Statement),
    #[state_machine_future(error)]
    Failed(Error),
}
