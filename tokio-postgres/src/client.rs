use crate::connection::Request;
use futures::channel::mpsc;

pub struct Client {
    sender: mpsc::UnboundedSender<Request>,
    process_id: i32,
    secret_key: i32,
}

impl Client {
    pub(crate) fn new(
        sender: mpsc::UnboundedSender<Request>,
        process_id: i32,
        secret_key: i32,
    ) -> Client {
        Client {
            sender,
            process_id,
            secret_key,
        }
    }
}
