use rmp_serde as rmps;
use std::result;
use std::vec::Vec;

pub type SerializedDataMsgs = Vec<Vec<u8>>;

#[derive(Serialize, Deserialize, Debug)]
pub struct BaseMsg {}

pub const MAX_DATA_SIZE: usize = 1024;

#[derive(Serialize, Deserialize, Debug)]
pub struct DataMsg<'a> {
    generation: u64,
    start: usize,
    complete_size: usize,
    data: &'a [u8],
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Subscribe(BaseMsg),
    Unsubscribe(BaseMsg),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Message<'a> {
    #[serde(borrow)]
    Data(DataMsg<'a>),
    Request(Request),
}

impl<'a> Message<'a> {
    pub fn deserialize<'b, T>(buf: &'b T) -> Result<Message<'b>, rmps::decode::Error>
    where
        T: AsRef<[u8]> + 'b,
    {
        rmps::from_slice(buf.as_ref())
    }

    pub fn serialize(&self) -> result::Result<Vec<u8>, rmps::encode::Error> {
        rmps::to_vec(&self)
    }

    pub fn serialize_data_msgs<T>(
        data: T,
        generation: u64,
    ) -> result::Result<SerializedDataMsgs, rmps::encode::Error>
    where
        T: AsRef<[u8]>,
    {
        let buf = data.as_ref();
        let chunks = buf.chunks(MAX_DATA_SIZE);
        let mut ret: Vec<Vec<u8>> = Vec::new();
        for (i, chunk) in chunks.enumerate() {
            let message = Message::Data(DataMsg {
                generation: generation,
                start: i * MAX_DATA_SIZE,
                complete_size: buf.len(),
                data: chunk,
            });

            ret.push(rmps::to_vec(&message)?);
        }
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {}
