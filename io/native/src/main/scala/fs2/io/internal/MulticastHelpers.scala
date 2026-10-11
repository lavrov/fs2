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

package fs2.io.internal

import com.comcast.ip4s.{Ipv4Address, Ipv6Address}
import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.posix.errno.errno
import scala.scalanative.posix.netinet.in._
import scala.scalanative.posix.netinet.inOps._
import scala.scalanative.posix.sys.socket._
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

private[io] object MulticastHelpers {

  // The IPv4 source-filter options are not bound by Scala Native's posixlib and
  // are numbered differently on Linux and BSD/macOS, so resolve them per
  // platform at link time.
  private val IpAddMembership = if (LinktimeInfo.isLinux) 35 else 12
  private val IpDropMembership = if (LinktimeInfo.isLinux) 36 else 13
  private val IpAddSourceMembership = if (LinktimeInfo.isLinux) 39 else 70
  private val IpDropSourceMembership = if (LinktimeInfo.isLinux) 40 else 71
  private val IpBlockSource = if (LinktimeInfo.isLinux) 38 else 72
  private val IpUnblockSource = if (LinktimeInfo.isLinux) 37 else 73

  // struct ip_mreq { struct in_addr imr_multiaddr, imr_interface; }
  private type ip_mreq = CStruct2[in_addr, in_addr]

  // struct ip_mreq_source {
  //   struct in_addr imr_multiaddr, imr_sourceaddr, imr_interface;
  // }
  private type ip_mreq_source = CStruct3[in_addr, in_addr, in_addr]

  private def setIpOpt(fd: CInt, option: CInt, value: Ptr[Byte], len: CSize): Unit = {
    val rtn = setsockopt(fd, IPPROTO_IP, option, value, len.toUInt)
    if (rtn < 0)
      throw new java.io.IOException(s"setsockopt failed: errno($errno)")
  }

  private def setIpv6Opt(fd: CInt, option: CInt, value: Ptr[Byte], len: CSize): Unit = {
    val rtn = setsockopt(fd, IPPROTO_IPV6, option, value, len.toUInt)
    if (rtn < 0)
      throw new java.io.IOException(s"setsockopt failed: errno($errno)")
  }

  private def inAddrOf(address: Ipv4Address): in_addr = {
    val addr = stackalloc[in_addr]()
    addr.s_addr = htonl(address.toLong.toUInt)
    !addr
  }

  private def in6AddrOf(address: Ipv6Address): in6_addr = {
    val addr = stackalloc[in6_addr]()
    val bytes = address.toBytes
    var i = 0
    while (i < 16) {
      addr.s6_addr(i) = bytes(i).toUByte
      i += 1
    }
    !addr
  }

  def joinGroup(fd: CInt, group: Ipv4Address, interface: Ipv4Address): Unit = {
    val mreq = stackalloc[ip_mreq]()
    mreq._1 = inAddrOf(group)
    mreq._2 = inAddrOf(interface)
    setIpOpt(fd, IpAddMembership, mreq.asInstanceOf[Ptr[Byte]], sizeof[ip_mreq])
  }

  def dropGroup(fd: CInt, group: Ipv4Address, interface: Ipv4Address): Unit = {
    val mreq = stackalloc[ip_mreq]()
    mreq._1 = inAddrOf(group)
    mreq._2 = inAddrOf(interface)
    setIpOpt(fd, IpDropMembership, mreq.asInstanceOf[Ptr[Byte]], sizeof[ip_mreq])
  }

  def joinSourceGroup(
      fd: CInt,
      group: Ipv4Address,
      source: Ipv4Address,
      interface: Ipv4Address
  ): Unit = {
    val mreq = stackalloc[ip_mreq_source]()
    mreq._1 = inAddrOf(group)
    mreq._2 = inAddrOf(source)
    mreq._3 = inAddrOf(interface)
    setIpOpt(fd, IpAddSourceMembership, mreq.asInstanceOf[Ptr[Byte]], sizeof[ip_mreq_source])
  }

  def dropSourceGroup(
      fd: CInt,
      group: Ipv4Address,
      source: Ipv4Address,
      interface: Ipv4Address
  ): Unit = {
    val mreq = stackalloc[ip_mreq_source]()
    mreq._1 = inAddrOf(group)
    mreq._2 = inAddrOf(source)
    mreq._3 = inAddrOf(interface)
    setIpOpt(fd, IpDropSourceMembership, mreq.asInstanceOf[Ptr[Byte]], sizeof[ip_mreq_source])
  }

  def blockSource(
      fd: CInt,
      group: Ipv4Address,
      source: Ipv4Address,
      interface: Ipv4Address
  ): Unit = {
    val mreq = stackalloc[ip_mreq_source]()
    mreq._1 = inAddrOf(group)
    mreq._2 = inAddrOf(source)
    mreq._3 = inAddrOf(interface)
    setIpOpt(fd, IpBlockSource, mreq.asInstanceOf[Ptr[Byte]], sizeof[ip_mreq_source])
  }

  def unblockSource(
      fd: CInt,
      group: Ipv4Address,
      source: Ipv4Address,
      interface: Ipv4Address
  ): Unit = {
    val mreq = stackalloc[ip_mreq_source]()
    mreq._1 = inAddrOf(group)
    mreq._2 = inAddrOf(source)
    mreq._3 = inAddrOf(interface)
    setIpOpt(fd, IpUnblockSource, mreq.asInstanceOf[Ptr[Byte]], sizeof[ip_mreq_source])
  }

  def joinGroup6(fd: CInt, group: Ipv6Address, ifIndex: CUnsignedInt): Unit = {
    val mreq = stackalloc[ipv6_mreq]()
    mreq.ipv6mr_multiaddr = in6AddrOf(group)
    mreq.ipv6mr_interface = ifIndex
    setIpv6Opt(fd, IPV6_JOIN_GROUP, mreq.asInstanceOf[Ptr[Byte]], sizeof[ipv6_mreq])
  }

  def dropGroup6(fd: CInt, group: Ipv6Address, ifIndex: CUnsignedInt): Unit = {
    val mreq = stackalloc[ipv6_mreq]()
    mreq.ipv6mr_multiaddr = in6AddrOf(group)
    mreq.ipv6mr_interface = ifIndex
    setIpv6Opt(fd, IPV6_LEAVE_GROUP, mreq.asInstanceOf[Ptr[Byte]], sizeof[ipv6_mreq])
  }

  def setMulticastTtl(fd: CInt, ttl: CInt): Unit = {
    // Linux expects an int for IP_MULTICAST_TTL, while BSD/macOS expects an u_char.
    if (LinktimeInfo.isLinux) {
      val value = stackalloc[CInt]()
      !value = ttl
      setIpOpt(fd, IP_MULTICAST_TTL, value.asInstanceOf[Ptr[Byte]], sizeof[CInt])
    } else {
      val value = stackalloc[Byte]()
      !value = ttl.toByte
      setIpOpt(fd, IP_MULTICAST_TTL, value, sizeof[Byte])
    }
  }

  def setMulticastInterface(
      fd: CInt,
      interface: java.net.NetworkInterface
  ): Unit = {
    val ifIndex = interface.getIndex
    val addr = interface.getInetAddresses.nextElement() match {
      case v4: java.net.Inet4Address => Ipv4Address.fromInet4Address(v4)
      case other =>
        throw new IllegalArgumentException(s"interface does not have an IPv4 address: $other")
    }
    if (LinktimeInfo.isLinux) {
      // Linux takes an ip_mreqn for IP_MULTICAST_IF.
      // struct ip_mreqn { struct in_addr imr_multiaddr, imr_address; int imr_ifindex; }
      type ip_mreqn = CStruct3[in_addr, in_addr, CInt]
      val mreq = stackalloc[ip_mreqn]()
      mreq.at1.s_addr = 0.toUInt
      mreq.at2.s_addr = htonl(addr.toLong.toUInt)
      mreq._3 = ifIndex
      setIpOpt(fd, IP_MULTICAST_IF, mreq.asInstanceOf[Ptr[Byte]], sizeof[ip_mreqn])
    } else {
      // BSD/macOS take a plain in_addr.
      val value = stackalloc[in_addr]()
      value.at1.s_addr = htonl(addr.toLong.toUInt)
      setIpOpt(fd, IP_MULTICAST_IF, value.asInstanceOf[Ptr[Byte]], sizeof[in_addr])
    }
  }
}
