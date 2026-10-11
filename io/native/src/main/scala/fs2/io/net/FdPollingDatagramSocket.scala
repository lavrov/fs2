/*
 * Copyright (c) 2013 Functional Streams for Scala
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package fs2
package io
package net

import cats.effect.{Async, FileDescriptorPollHandle, IO, LiftIO, Resource}
import cats.syntax.all._
import com.comcast.ip4s._
import fs2.io.internal.NativeUtil._
import fs2.io.internal.{ResizableBuffer, SocketHelpers}

import scala.scalanative.posix.sys.socket.{connect => sconnect, _}
import scala.scalanative.posix.sys.socketOps._
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

private[net] final class FdPollingDatagramSocket[F[_]: LiftIO] private (
    fd: Int,
    handle: FileDescriptorPollHandle,
    readBuffer: ResizableBuffer[F],
    val address: GenSocketAddress
)(implicit F: Async[F])
    extends DatagramSocket[F] {

  def localAddress = F.pure(address.asIpUnsafe)

  def read: F[Datagram] =
    readBuffer.get(FdPollingDatagramSocket.DefaultReadSize).use { buf =>
      handle
        .pollReadRec(()) { _ =>
          IO {
            SocketHelpers.allocateSockaddr(AF_INET6) { (src, srcLen) =>
              val received = guardSSize(
                recvfrom(
                  fd,
                  buf,
                  FdPollingDatagramSocket.DefaultReadSize.toUSize,
                  0,
                  src,
                  srcLen
                )
              )
              if (received >= 0)
                Right(
                  Datagram(
                    SocketHelpers
                      .toSocketAddress(src, src.sa_family.toInt)
                      .asInstanceOf[SocketAddress[IpAddress]],
                    Chunk.fromBytePtr(buf, received.toInt)
                  )
                )
              else
                Left(())
            }
          }
        }
        .to
    }

  def readGen: F[GenDatagram] = read.map(_.toGenDatagram)

  def reads: Stream[F, Datagram] = Stream.repeatEval(read)

  def connect(address: GenSocketAddress) = F.delay {
    SocketHelpers.toSockaddr(address.asIpUnsafe) { (addr, len) =>
      guard_(sconnect(fd, addr, len))
    }
  }

  def disconnect = F.delay {
    val addr = stackalloc[sockaddr]()
    addr.sa_family = AF_UNSPEC.toUShort
    guard_(sconnect(fd, addr, sizeof[sockaddr].toUInt))
  }

  def write(bytes: Chunk[Byte]) = writeGen(bytes, None)

  def write(bytes: Chunk[Byte], address: GenSocketAddress) = writeGen(bytes, Some(address))

  private def writeGen(bytes: Chunk[Byte], address: Option[GenSocketAddress]): F[Unit] = {
    val Chunk.ArraySlice(buf, offset, length) = bytes.toArraySlice
    val destination = address.map(_.asIpUnsafe)
    def go: IO[Either[Int, Unit]] =
      IO {
        destination match {
          case Some(remote) =>
            SocketHelpers.toSockaddr(remote) { (addr, len) =>
              guardSSize(sendto(fd, buf.atUnsafe(offset), length.toUSize, 0, addr, len)).toInt
            }
          case None =>
            guardSSize(sendto(fd, buf.atUnsafe(offset), length.toUSize, 0, null, 0.toUInt)).toInt
        }
      }.flatMap { wrote =>
        if (wrote >= 0) IO.pure(Either.unit)
        else IO.pure(Left(0))
      }
    handle.pollWriteRec(0)(_ => go).to
  }

  def writes: Pipe[F, Datagram, Nothing] = _.foreach(write(_))

  def join(
      join: MulticastJoin[IpAddress],
      interface: NetworkInterface
  ): F[GroupMembership] =
    F.raiseError(new UnsupportedOperationException("Multicast is not supported on Native"))

  def join(
      join: MulticastJoin[IpAddress],
      interface: DatagramSocket.NetworkInterface
  ): F[GroupMembership] =
    F.raiseError(new UnsupportedOperationException("Multicast is not supported on Native"))

  def getOption[A](key: SocketOption.Key[A]) = SocketHelpers.getOption[F, A](fd, key)

  def setOption[A](key: SocketOption.Key[A], value: A) =
    SocketHelpers.setOption(fd, key, value)

  def supportedOptions = SocketHelpers.supportedOptions

  override def toString = s"DatagramSocket($address)"
}

private[net] object FdPollingDatagramSocket {
  private[net] final val DefaultReadSize = 2048

  def apply[F[_]: LiftIO](
      fd: Int,
      handle: FileDescriptorPollHandle,
      domain: CInt
  )(implicit F: Async[F]): Resource[F, DatagramSocket[F]] =
    for {
      readBuffer <- ResizableBuffer(DefaultReadSize)
      address <- Resource.eval(F.delay(SocketHelpers.getAddress(fd, domain)))
    } yield new FdPollingDatagramSocket(fd, handle, readBuffer, address)
}
