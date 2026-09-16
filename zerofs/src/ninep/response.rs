use bytes::{Buf, Bytes};
use bytes_utils::SegmentedBuf;
use ninep_proto::{DekuBytes, Message, P9Message, Rlerror};

pub(crate) type EncodedResponse = bytes::buf::Chain<Bytes, SegmentedBuf<Bytes>>;

/// Keeps read data separate from the message until the socket write.
pub(crate) struct Response {
    message: P9Message,
    payload: Option<SegmentedBuf<Bytes>>,
}

fn counted_data(body: &mut Message) -> Option<(&mut u32, &mut DekuBytes)> {
    match body {
        Message::Rread(r) => Some((&mut r.count, &mut r.data)),
        Message::Rlopenatread(r) => Some((&mut r.count, &mut r.data)),
        Message::Rreaddir(r) => Some((&mut r.count, &mut r.data)),
        Message::Rreaddirattr(r) => Some((&mut r.count, &mut r.data)),
        _ => None,
    }
}

impl Response {
    pub(crate) fn error(tag: u16, ecode: u32) -> Self {
        Self::new(
            P9Message::new(tag, Message::Rlerror(Rlerror { ecode })),
            None,
        )
    }

    pub(crate) fn new(message: P9Message, payload: Option<SegmentedBuf<Bytes>>) -> Self {
        // A post-dispatch failure must discard any successfully read payload.
        let payload = if matches!(message.body, Message::Rread(_) | Message::Rlopenatread(_)) {
            payload
        } else {
            None
        };
        Self { message, payload }
    }

    pub(crate) fn encode(mut self) -> Result<EncodedResponse, deku::DekuError> {
        let payload = match counted_data(&mut self.message.body) {
            Some((count, data)) => {
                let inline = std::mem::take(&mut data.0);
                let payload = self.payload.unwrap_or_else(|| vec![inline].into());
                *count = payload.remaining().try_into()?;
                payload
            }
            None => SegmentedBuf::new(),
        };
        let mut header = self.message.to_bytes()?;
        let size: u32 = (header.len() + payload.remaining()).try_into()?;
        header[..4].copy_from_slice(&size.to_le_bytes());
        Ok(Bytes::from(header).chain(payload))
    }

    #[cfg(test)]
    pub(crate) fn into_message(mut self) -> P9Message {
        if let Some(mut payload) = self.payload {
            let (count, data) = counted_data(&mut self.message.body).expect("read payload");
            *count = payload.remaining().try_into().unwrap();
            data.0 = payload.copy_to_bytes(payload.remaining());
        }
        self.message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ninep_proto::Rread;

    fn flatten(mut buf: impl Buf) -> Bytes {
        buf.copy_to_bytes(buf.remaining())
    }

    #[test]
    fn segmented_read_matches_wire_codec_and_retains_payload() {
        let data = Bytes::from(vec![0xab; 32768]);
        let message = P9Message::new(
            17,
            Message::Rread(Rread {
                count: data.len() as u32,
                data: data.clone().into(),
            }),
        );
        let expected = message.to_bytes().unwrap();
        let mut encoded = Response::new(message, None).encode().unwrap();
        encoded.advance(11);
        assert_eq!(encoded.chunk().as_ptr(), data.as_ptr());
        let payload = vec![data.slice(..100), data.slice(100..)].into();
        let header = P9Message::new(
            17,
            Message::Rread(Rread {
                count: 0,
                data: DekuBytes::default(),
            }),
        );
        assert_eq!(
            flatten(Response::new(header, Some(payload)).encode().unwrap()).as_ref(),
            expected
        );
    }

    #[test]
    fn failed_response_discards_read_data() {
        let message = P9Message::new(17, Message::Rlerror(Rlerror { ecode: 5 }));
        let expected = message.to_bytes().unwrap();
        assert_eq!(
            flatten(
                Response::new(message, Some(vec![Bytes::from_static(b"secret")].into()))
                    .encode()
                    .unwrap()
            )
            .as_ref(),
            expected
        );
    }

    #[test]
    fn all_counted_responses_match_contiguous_encoding_including_empty_payloads() {
        use ninep_proto::{Qid, Rlopenatread, Rreaddir, Rreaddirattr};
        for len in [0, 1, 32768] {
            let bytes = Bytes::from(vec![0xa5; len]);
            let messages = [
                Message::Rread(Rread {
                    count: len as u32,
                    data: bytes.clone().into(),
                }),
                Message::Rlopenatread(Rlopenatread {
                    qid: Qid {
                        path: 93,
                        ..Default::default()
                    },
                    iounit: 4096,
                    eof: 1,
                    count: len as u32,
                    data: bytes.clone().into(),
                }),
                Message::Rreaddir(Rreaddir {
                    count: len as u32,
                    data: bytes.clone().into(),
                }),
                Message::Rreaddirattr(Rreaddirattr {
                    count: len as u32,
                    data: bytes.clone().into(),
                }),
            ];
            for body in messages {
                let message = P9Message::new(91, body);
                let expected = message.to_bytes().unwrap();
                let encoded = flatten(Response::new(message, None).encode().unwrap());
                assert_eq!(encoded.as_ref(), expected);
                assert!(P9Message::from_bytes_ctx(&encoded, false).is_ok());
            }
        }
    }
}
