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

import cats.effect.{Async, IO, LiftIO, Resource}
import cats.syntax.all._
import com.comcast.ip4s._
import fs2.io.internal.NativeUtil._
import fs2.io.internal.SocketHelpers

import scala.scalanative.posix.sys.socket._

private final class FdPollingIpDatagramSocketsProvider[F[_]: Dns: LiftIO](implicit F: Async[F])
    extends IpDatagramSocketsProvider[F] {

  override def bindDatagramSocket(
      address: SocketAddress[Host],
      options: List[SocketOption]
  ): Resource[F, DatagramSocket[F]] =
    for {
      poller <- Resource.eval(fileDescriptorPoller[F])
      resolved <- Resource.eval(address.resolve)
      ipv4 = resolved.host.isInstanceOf[Ipv4Address]
      domain = if (ipv4) AF_INET else AF_INET6
      fd <- SocketHelpers.openNonBlocking(domain, SOCK_DGRAM)
      _ <- Resource.eval(options.traverse_(so => SocketHelpers.setOption(fd, so.key, so.value)))
      _ <- Resource.eval(F.delay {
        val resolvedAddress = SocketAddress(resolved.host, address.port)
        SocketHelpers.toSockaddr(resolvedAddress) { (addr, len) =>
          guard_(bind(fd, addr, len))
        }
      })
      handle <- poller.registerFileDescriptor(fd, true, true).mapK(LiftIO.liftK)
      socket <- FdPollingDatagramSocket[F](fd, handle, domain)
    } yield socket
}
