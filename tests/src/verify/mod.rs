use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::stream::Stream;

use std::option::Option;
use std::pin::Pin;
use std::task::{Context, Poll};

pub enum VerificationStatus {
    ///Verification Succeeded with no issues
    Verified,

    ///Verification could not proceed on this message. A future one may succeed. An example use is
    ///successful message is needed to verify the next message and none has been received yet.
    Pending,

    ///Verification Failed. Further messages may not succeed.
    Error,
}

pub trait Verifier {
    ///Function to create messages
    fn create_message(&self) -> Bytes;

    ///Function to check messages created by create_message
    fn verify_message(&self, buffer: Bytes) -> VerificationStatus;
}

///Fills in a constant value
pub struct ConstantVerifier<T = u64>
where
    T: Sized,
{
    value: T,
}

impl<T> Default for ConstantVerifier<T>
where
    T: Sized + Default,
{
    fn default() -> Self {
        Self {
            value: T::default(),
        }
    }
}

impl<T> ConstantVerifier<T> {
    pub fn new(value: T) -> Self {
        Self { value }
    }
}

impl Verifier for ConstantVerifier<u64> {
    fn create_message(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(std::mem::size_of::<u64>());
        bytes.put_u64(self.value);
        bytes.freeze()
    }

    fn verify_message(&self, mut buffer: Bytes) -> VerificationStatus {
        if (buffer.get_u64() == self.value) && buffer.is_empty() {
            VerificationStatus::Verified
        } else {
            VerificationStatus::Error
        }
    }
}

pub struct VerifierStream<T>
where
    T: AsRef<dyn Verifier>,
{
    verifier: T,
}

impl<T> VerifierStream<T>
where
    T: AsRef<dyn Verifier>,
{
    pub fn new(verifier: T) -> Self {
        Self { verifier }
    }
}

impl<T> Stream for VerifierStream<T>
where
    T: AsRef<dyn Verifier>,
{
    type Item = Bytes;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
        Poll::Ready(Some(self.verifier.as_ref().create_message()))
    }
}

#[pin_project::pin_project]
pub struct VerifiedStream<'a, T, V, S>
where
    T: AsRef<V>,
    S: Stream<Item = Bytes>,
    V: Verifier + Sized,
{
    #[pin]
    stream: S,
    verifier: T,
    phantom: std::marker::PhantomData<&'a V>,
}

impl<'a, T, V, S> VerifiedStream<'a, T, V, S>
where
    T: AsRef<V>,
    S: Stream<Item = Bytes>,
    V: Verifier,
{
    pub fn new(stream: S, verifier: T) -> Self {
        Self {
            stream,
            verifier,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, T, V, S> Stream for VerifiedStream<'a, T, V, S>
where
    T: AsRef<V>,
    S: Stream<Item = Bytes>,
    V: Verifier + Sized,
{
    type Item = VerificationStatus;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let res = futures::ready!(this.stream.poll_next(cx));
        Poll::Ready(match res {
            Some(b) => Some(this.verifier.as_ref().verify_message(b)),
            None => None,
        })
    }
}
