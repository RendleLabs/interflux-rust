use bytes::{Bytes, BytesMut};
use futures::{Async, Poll, Stream};
use hyper::{Chunk, Error};
use std::collections::VecDeque;

pub struct Reader<S> {
    len: usize,
    items: VecDeque<Chunk>,
    stream: S,
}

impl<S> Reader<S>
where
    S: Stream<Item = Chunk, Error = Error>,
{
    pub fn new(stream: S) -> Self {
        Reader {
            len: 0,
            items: VecDeque::new(),
            stream,
        }
    }

    #[inline]
    fn poll_stream(&mut self) -> Poll<bool, Error> {
        self.stream.poll().map(|res| match res {
            Async::Ready(Some(data)) => {
                self.len += data.len();
                self.items.push_back(data);
                Async::Ready(true)
            }
            Async::Ready(None) =>{
                Async::Ready(false)
            },
            Async::NotReady => Async::NotReady,
        })
    }

    fn read_until(&mut self, until: u8) -> Poll<Option<Bytes>, Error> {
        let mut length = 0;
        let mut num = 0;
        let mut offset = 0;

        for i in 0..self.items.len() {
            let chunk = &self.items[i];
            let pos = chunk.iter().position(|&c| c == until);
            let found = match pos {
                Some(p) => {
                    if p > 0 {
                        num = i;
                        offset = p + 1;
                        length += p + 1;
                        true
                    } else {
                        length += chunk.len();
                        false
                    }
                }
                None => {
                    length += chunk.len();
                    false
                }
            };
            if found {
                let mut buf = BytesMut::with_capacity(length);
                if num > 0 {
                    for _ in 0..num {
                        buf.extend_from_slice(&self.items.pop_front().unwrap());
                    }
                }
                if offset > 0 {
                    let chunk = self.items.pop_front().unwrap();
                    let (first, last) = chunk.split_at(offset);
                    buf.extend_from_slice(&first);
                    if last.len() > 0 {
                        self.items.push_front(Chunk::from(last.to_vec()));
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
                self.len -= length;
                return Ok(Async::Ready(Some(buf.freeze())));
            }
        }

        match self.poll_stream()? {
            Async::Ready(true) => self.read_until(until),
            Async::Ready(false) => Ok(Async::Ready(None)),
            Async::NotReady => Ok(Async::NotReady),
        }
    }

    pub fn read_line(&mut self) -> Poll<Option<Bytes>, Error> {
        self.read_until(b'\n')
    }
}
