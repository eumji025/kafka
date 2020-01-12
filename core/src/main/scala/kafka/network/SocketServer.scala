/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels._
import java.nio.channels.{Selector => NSelector}
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.metrics.KafkaMetricsGroup
import kafka.network.RequestChannel.{CloseConnectionResponse, EndThrottlingResponse, NoOpResponse, SendResponse, StartThrottlingResponse}
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.{KafkaException, Reconfigurable}
import org.apache.kafka.common.memory.{MemoryPool, SimpleMemoryPool}
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.network.KafkaChannel.ChannelMuteEvent
import org.apache.kafka.common.network.{ChannelBuilder, ChannelBuilders, KafkaChannel, ListenerName, Selectable, Send, Selector => KSelector}
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time}
import org.slf4j.event.Level

import scala.collection._
import JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Buffer}
import scala.util.control.ControlThrowable

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time, val credentialProvider: CredentialProvider) extends Logging with KafkaMetricsGroup {

  private val maxQueuedRequests = config.queuedMaxRequests

  private val logContext = new LogContext(s"[SocketServer brokerId=${config.brokerId}] ")
  this.logIdent = logContext.logPrefix

  private val memoryPoolSensor = metrics.sensor("MemoryPoolUtilization")
  private val memoryPoolDepletedPercentMetricName = metrics.metricName("MemoryPoolAvgDepletedPercent", "socket-server-metrics")
  private val memoryPoolDepletedTimeMetricName = metrics.metricName("MemoryPoolDepletedTimeTotal", "socket-server-metrics")
  memoryPoolSensor.add(new Meter(TimeUnit.MILLISECONDS, memoryPoolDepletedPercentMetricName, memoryPoolDepletedTimeMetricName))
  private val memoryPool = if (config.queuedMaxBytes > 0) new SimpleMemoryPool(config.queuedMaxBytes, config.socketRequestMaxBytes, false, memoryPoolSensor) else MemoryPool.NONE
  val requestChannel = new RequestChannel(maxQueuedRequests)
  private val processors = new ConcurrentHashMap[Int, Processor]()
  private var nextProcessorId = 0

  private[network] val acceptors = new ConcurrentHashMap[EndPoint, Acceptor]()
  private var connectionQuotas: ConnectionQuotas = _
  private var stoppedProcessingRequests = false

  /**
   * Start the socket server. Acceptors for all the listeners are started. Processors
   * are started if `startupProcessors` is true. If not, processors are only started when
   * [[kafka.network.SocketServer#startProcessors()]] is invoked. Delayed starting of processors
   * is used to delay processing client connections until server is fully initialized, e.g.
   * to ensure that all credentials have been loaded before authentications are performed.
   * Acceptors are always started during `startup` so that the bound port is known when this
   * method completes even when ephemeral ports are used. Incoming connections on this server
   * are processed when processors start up and invoke [[org.apache.kafka.common.network.Selector#poll]].
   *
   * @param startupProcessors Flag indicating whether `Processor`s must be started.
   */
  def startup(startupProcessors: Boolean = true) {
    this.synchronized {
      connectionQuotas = new ConnectionQuotas(config.maxConnectionsPerIp, config.maxConnectionsPerIpOverrides)
      //创建acceptor和processor
      createAcceptorAndProcessors(config.numNetworkThreads, config.listeners)
      if (startupProcessors) {
        //启动
        startProcessors()
      }
    }

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {

        def value = SocketServer.this.synchronized {
          val ioWaitRatioMetricNames = processors.values.asScala.map { p =>
            metrics.metricName("io-wait-ratio", "socket-server-metrics", p.metricTags)
          }
          ioWaitRatioMetricNames.map { metricName =>
            Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
          }.sum / processors.size
        }
      }
    )
    newGauge("MemoryPoolAvailable",
      new Gauge[Long] {
        def value = memoryPool.availableMemory()
      }
    )
    newGauge("MemoryPoolUsed",
      new Gauge[Long] {
        def value = memoryPool.size() - memoryPool.availableMemory()
      }
    )
    info("Started " + acceptors.size + " acceptor threads")
  }

  /**
   * Starts processors of all the acceptors of this server if they have not already been started.
   * This method is used for delayed starting of processors if [[kafka.network.SocketServer#startup]]
   * was invoked with `startupProcessors=false`.
   */
  def startProcessors(): Unit = synchronized {
    acceptors.values.asScala.foreach { _.startProcessors() }
    info(s"Started processors for ${acceptors.size} acceptors")
  }

  private def endpoints = config.listeners.map(l => l.listenerName -> l).toMap


  /**
   * acceptor -> processor -> handler
   * @param processorsPerListener
   * @param endpoints
   */
  private def createAcceptorAndProcessors(processorsPerListener: Int,
                                          endpoints: Seq[EndPoint]): Unit = synchronized {

    val sendBufferSize = config.socketSendBufferBytes
    val recvBufferSize = config.socketReceiveBufferBytes
    val brokerId = config.brokerId
    //遍历每个point
    endpoints.foreach { endpoint =>
      val listenerName = endpoint.listenerName
      val securityProtocol = endpoint.securityProtocol

      //创建acceptor
      val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId, connectionQuotas)
      //添加processor
      addProcessors(acceptor, endpoint, processorsPerListener)
      //启动accetor对于的线程
      KafkaThread.nonDaemon(s"kafka-socket-acceptor-$listenerName-$securityProtocol-${endpoint.port}", acceptor).start()
      //等待acceptor启动完成
      acceptor.awaitStartup()
      //记录端点对应的acceptor
      acceptors.put(endpoint, acceptor)
    }
  }

  private def addProcessors(acceptor: Acceptor, endpoint: EndPoint, newProcessorsPerListener: Int): Unit = synchronized {
    val listenerName = endpoint.listenerName
    val securityProtocol = endpoint.securityProtocol
    val listenerProcessors = new ArrayBuffer[Processor]()

    for (_ <- 0 until newProcessorsPerListener) {
      val processor = newProcessor(nextProcessorId, connectionQuotas, listenerName, securityProtocol, memoryPool)
      listenerProcessors += processor
      requestChannel.addProcessor(processor)
      nextProcessorId += 1
    }
    listenerProcessors.foreach(p => processors.put(p.id, p))
    acceptor.addProcessors(listenerProcessors)
  }

  /**
    * Stop processing requests and new connections.
    */
  def stopProcessingRequests() = {
    info("Stopping socket server request processors")
    this.synchronized {
      acceptors.asScala.values.foreach(_.shutdown())
      processors.asScala.values.foreach(_.shutdown())
      requestChannel.clear()
      stoppedProcessingRequests = true
    }
    info("Stopped socket server request processors")
  }

  def resizeThreadPool(oldNumNetworkThreads: Int, newNumNetworkThreads: Int): Unit = synchronized {
    info(s"Resizing network thread pool size for each listener from $oldNumNetworkThreads to $newNumNetworkThreads")
    if (newNumNetworkThreads > oldNumNetworkThreads) {
      acceptors.asScala.foreach { case (endpoint, acceptor) =>
        addProcessors(acceptor, endpoint, newNumNetworkThreads - oldNumNetworkThreads)
      }
    } else if (newNumNetworkThreads < oldNumNetworkThreads)
      acceptors.asScala.values.foreach(_.removeProcessors(oldNumNetworkThreads - newNumNetworkThreads, requestChannel))
  }

  /**
    * Shutdown the socket server. If still processing requests, shutdown
    * acceptors and processors first.
    */
  def shutdown() = {
    info("Shutting down socket server")
    this.synchronized {
      if (!stoppedProcessingRequests)
        stopProcessingRequests()
      requestChannel.shutdown()
    }
    info("Shutdown completed")
  }

  def boundPort(listenerName: ListenerName): Int = {
    try {
      acceptors.get(endpoints(listenerName)).serverChannel.socket.getLocalPort
    } catch {
      case e: Exception =>
        throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  def addListeners(listenersAdded: Seq[EndPoint]): Unit = synchronized {
    info(s"Adding listeners for endpoints $listenersAdded")
    createAcceptorAndProcessors(config.numNetworkThreads, listenersAdded)
    startProcessors()
  }

  def removeListeners(listenersRemoved: Seq[EndPoint]): Unit = synchronized {
    info(s"Removing listeners for endpoints $listenersRemoved")
    listenersRemoved.foreach { endpoint =>
      acceptors.asScala.remove(endpoint).foreach(_.shutdown())
    }
  }

  def updateMaxConnectionsPerIp(maxConnectionsPerIp: Int): Unit = {
    info(s"Updating maxConnectionsPerIp: $maxConnectionsPerIp")
    connectionQuotas.updateMaxConnectionsPerIp(maxConnectionsPerIp)
  }

  def updateMaxConnectionsPerIpOverride(maxConnectionsPerIpOverrides: Map[String, Int]): Unit = {
    info(s"Updating maxConnectionsPerIpOverrides: ${maxConnectionsPerIpOverrides.map { case (k, v) => s"$k=$v" }.mkString(",")}")
    connectionQuotas.updateMaxConnectionsPerIpOverride(maxConnectionsPerIpOverrides)
  }

  /* `protected` for test usage */
  protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                      securityProtocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      config.failedAuthenticationDelayMs,
      listenerName,
      securityProtocol,
      config,
      metrics,
      credentialProvider,
      memoryPool,
      logContext
    )
  }

  /* For test usage */
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  /* For test usage */
  private[network] def processor(index: Int): Processor = processors.get(index)

}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  private val startupLatch = new CountDownLatch(1)

  // `shutdown()` is invoked before `startupComplete` and `shutdownComplete` if an exception is thrown in the constructor
  // (e.g. if the address is already in use). We want `shutdown` to proceed in such cases, so we first assign an open
  // latch and then replace it in `startupComplete()`.
  @volatile private var shutdownLatch = new CountDownLatch(0)

  private val alive = new AtomicBoolean(true)

  def wakeup(): Unit

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    if (alive.getAndSet(false))
      wakeup()
    shutdownLatch.await()
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete(): Unit = {
    // Replace the open latch with a closed one
    shutdownLatch = new CountDownLatch(1)
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete(): Unit = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning: Boolean = alive.get

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(channel: SocketChannel): Unit = {
    if (channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      CoreUtils.swallow(channel.socket().close(), this, Level.ERROR)
      CoreUtils.swallow(channel.close(), this, Level.ERROR)
    }
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 * 用来处理链接和配置新链接的方法实现
 */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  //构建nio selector
  private val nioSelector = NSelector.open()
  //打开channel
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)
  private val processors = new ArrayBuffer[Processor]()
  private val processorsStarted = new AtomicBoolean

  //记录processor
  private[network] def addProcessors(newProcessors: Buffer[Processor]): Unit = synchronized {
    processors ++= newProcessors
    if (processorsStarted.get)
      startProcessors(newProcessors)
  }

  private[network] def startProcessors(): Unit = synchronized {
    if (!processorsStarted.getAndSet(true)) {
      startProcessors(processors)
    }
  }

  //启动processor
  private def startProcessors(processors: Seq[Processor]): Unit = synchronized {
    processors.foreach { processor =>
      KafkaThread.nonDaemon(s"kafka-network-thread-$brokerId-${endPoint.listenerName}-${endPoint.securityProtocol}-${processor.id}",
        processor).start()
    }
  }

  private[network] def removeProcessors(removeCount: Int, requestChannel: RequestChannel): Unit = synchronized {
    // Shutdown `removeCount` processors. Remove them from the processor list first so that no more
    // connections are assigned. Shutdown the removed processors, closing the selector and its connections.
    // The processors are then removed from `requestChannel` and any pending responses to these processors are dropped.
    val toRemove = processors.takeRight(removeCount)
    processors.remove(processors.size - removeCount, removeCount)
    toRemove.foreach(_.shutdown())
    toRemove.foreach(processor => requestChannel.removeProcessor(processor.id))
  }

  override def shutdown(): Unit = {
    super.shutdown()
    synchronized {
      processors.foreach(_.shutdown())
    }
  }

  /**
   * Accept loop that checks for new connection attempts
   *
   *
   * runnable的run方法实现，主要是用来循环检查和配置新连接
   */
  def run() {
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    //当chanel注册到seletor就表示的启动完成。让我们前面的线程继续处理自己的内容
    startupComplete()
    try {
      var currentProcessor = 0
      while (isRunning) {
        try {
          //等待连接
          val ready = nioSelector.select(500)
          if (ready > 0) {
            //获取所有的连接key
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            //遍历
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                iter.remove()
                //判断key是不是accept事件
                if (key.isAcceptable) {
                  val processor = synchronized {
                    currentProcessor = currentProcessor % processors.size
                    //选择对应的processor
                    processors(currentProcessor)
                  }
                  //进行accept的处理
                  accept(key, processor)
                } else
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")

                // round robin to the next processor thread, mod(numProcessors) will be done later
                currentProcessor = currentProcessor + 1
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket and selector.")
      CoreUtils.swallow(serverChannel.close(), this, Level.ERROR)
      CoreUtils.swallow(nioSelector.close(), this, Level.ERROR)
      shutdownComplete()
    }
  }

  /*
   * Create a server socket to listen for connections on.
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if (host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      serverChannel.socket().setReceiveBufferSize(recvBufferSize)

    try {
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * Accept a new connection
   */
  def accept(key: SelectionKey, processor: Processor) {
    //获取server socket channel
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    //获取accept后的channel
    val socketChannel = serverSocketChannel.accept()
    try {
      //增加连接数
      connectionQuotas.inc(socketChannel.socket().getInetAddress)
      //配置连接后的channel信息
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)
      if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
        socketChannel.socket().setSendBufferSize(sendBufferSize)

      debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))
      //processor处理channel
      processor.accept(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = nioSelector.wakeup()

}

private[kafka] object Processor {
  val IdlePercentMetricName = "IdlePercent"
  val NetworkProcessorMetricTag = "networkProcessor"
  val ListenerMetricTag = "listener"
}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               failedAuthenticationDelayMs: Int,
                               listenerName: ListenerName,
                               securityProtocol: SecurityProtocol,
                               config: KafkaConfig,
                               metrics: Metrics,
                               credentialProvider: CredentialProvider,
                               memoryPool: MemoryPool,
                               logContext: LogContext) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  import Processor._
  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote, index) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort, Integer.parseInt(index))
        }
      }
      case _ => None
    }
  }

  private[network] case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int, index: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort-$index"
  }

  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  private val responseQueue = new LinkedBlockingDeque[RequestChannel.Response]()

  private[kafka] val metricTags = mutable.LinkedHashMap(
    ListenerMetricTag -> listenerName.value,
    NetworkProcessorMetricTag -> id.toString
  ).asJava

  newGauge(IdlePercentMetricName,
    new Gauge[Double] {
      def value = {
        Option(metrics.metric(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags)))
          .fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
      }
    },
    // for compatibility, only add a networkProcessor tag to the Yammer Metrics alias (the equivalent Selector metric
    // also includes the listener name)
    Map(NetworkProcessorMetricTag -> id.toString)
  )

  private val selector = createSelector(
    ChannelBuilders.serverChannelBuilder(listenerName,
      listenerName == config.interBrokerListenerName,
      securityProtocol,
      config,
      credentialProvider.credentialCache,
      credentialProvider.tokenCache))
  // Visible to override for testing
  protected[network] def createSelector(channelBuilder: ChannelBuilder): KSelector = {
    channelBuilder match {
      case reconfigurable: Reconfigurable => config.addReconfigurable(reconfigurable)
      case _ =>
    }
    new KSelector(
      maxRequestSize,
      connectionsMaxIdleMs,
      failedAuthenticationDelayMs,
      metrics,
      time,
      "socket-server",
      metricTags,
      false,
      true,
      channelBuilder,
      memoryPool,
      logContext)
  }

  // Connection ids have the format `localAddr:localPort-remoteAddr:remotePort-index`. The index is a
  // non-negative incrementing value that ensures that even if remotePort is reused after a connection is
  // closed, connection ids are not reused while requests from the closed connection are being processed.
  private var nextConnectionIndex = 0

  /**
   * run方法的执行逻辑
   *
   * 这里我们可以发现，proccesor里面做的事情是非常多的
   * 因为他有非常多的职责，因为他是用来处理socket对应的读写事件的。
   * 所以都要被他处理。
   * processCompletedReceives这一步就非常的值得我们去看，因为他会将请求转发给[[kafka.server.KafkaApis]]
   */
  override def run() {
    //表示启动完成
    startupComplete()
    try {
      while (isRunning) {
        try {
          // setup any new connections that have been queued up
          //配置新连接
          configureNewConnections()
          // register any new responses for writing
          //处理响应 - 根据不同的请求进行不同 的处理
          processNewResponses()
          //读取socket消息写到completeReceive里
          poll()
          //进行接收完成的处理 - 发送到requestQueue里
          processCompletedReceives()
          //进行发送完成的处理 - 回调onComplete函数
          processCompletedSends()
          //处理断开连接
          processDisconnected()
        } catch {
          // We catch all the throwables here to prevent the processor thread from exiting. We do this because
          // letting a processor exit might cause a bigger impact on the broker. This behavior might need to be
          // reviewed if we see an exception that needs the entire broker to stop. Usually the exceptions thrown would
          // be either associated with a specific socket channel or a bad request. These exceptions are caught and
          // processed by the individual methods above which close the failing channel and continue processing other
          // channels. So this catch block should only ever see ControlThrowables.
          case e: Throwable => processException("Processor got uncaught exception.", e)
        }
      }
    } finally {
      debug("Closing selector - processor " + id)
      CoreUtils.swallow(closeAll(), this, Level.ERROR)
      shutdownComplete()
    }
  }

  private def processException(errorMessage: String, throwable: Throwable) {
    throwable match {
      case e: ControlThrowable => throw e
      case e => error(errorMessage, e)
    }
  }

  private def processChannelException(channelId: String, errorMessage: String, throwable: Throwable) {
    if (openOrClosingChannel(channelId).isDefined) {
      error(s"Closing socket for $channelId because of error", throwable)
      close(channelId)
    }
    processException(errorMessage, throwable)
  }

  private def processNewResponses() {
    var currentResponse: RequestChannel.Response = null
    while ({currentResponse = dequeueResponse(); currentResponse != null}) {
      val channelId = currentResponse.request.context.connectionId
      try {
        currentResponse match {
          case response: NoOpResponse =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            updateRequestMetrics(response)
            trace("Socket server received empty response to send, registering for read: " + response)
            // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
            // it will be unmuted immediately. If the channel has been throttled, it will be unmuted only if the
            // throttling delay has already passed by now.
            handleChannelMuteEvent(channelId, ChannelMuteEvent.RESPONSE_SENT)
            tryUnmuteChannel(channelId)

          case response: SendResponse =>
            sendResponse(response, response.responseSend)
          case response: CloseConnectionResponse =>
            updateRequestMetrics(response)
            trace("Closing socket connection actively according to the response code.")
            close(channelId)
          case _: StartThrottlingResponse =>
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_STARTED)
          case _: EndThrottlingResponse =>
            // Try unmuting the channel. The channel will be unmuted only if the response has already been sent out to
            // the client.
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_ENDED)
            tryUnmuteChannel(channelId)
          case _ =>
            throw new IllegalArgumentException(s"Unknown response type: ${currentResponse.getClass}")
        }
      } catch {
        case e: Throwable =>
          processChannelException(channelId, s"Exception while processing response for $channelId", e)
      }
    }
  }

  /* `protected` for test usage */
  protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send) {
    val connectionId = response.request.context.connectionId
    trace(s"Socket server received response to send to $connectionId, registering for write and sending data: $response")
    // `channel` can be None if the connection was closed remotely or if selector closed it for being idle for too long
    if (channel(connectionId).isEmpty) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $connectionId")
      response.request.updateRequestMetrics(0L, response)
    }
    // Invoke send for closingChannel as well so that the send is failed and the channel closed properly and
    // removed from the Selector after discarding any pending staged receives.
    // `openOrClosingChannel` can be None if the selector closed the connection because it was idle for too long
    if (openOrClosingChannel(connectionId).isDefined) {
      selector.send(responseSend)
      inflightResponses += (connectionId -> response)
    }
  }

  private def poll() {
    //获取请求
    try selector.poll(300)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        // The exception is not re-thrown and any completed sends/receives/connections/disconnections
        // from this poll will be processed.
        error(s"Processor $id poll failed", e)
    }
  }

  private def processCompletedReceives() {

    //遍历接收完成的请求进行
    selector.completedReceives.asScala.foreach { receive =>
      try {
        //通过connectid找到channel（open/close）
        openOrClosingChannel(receive.source) match {
          case Some(channel) =>
            //解析header
            val header = RequestHeader.parse(receive.payload)
            val connectionId = receive.source
            //构建请求上细纹
            val context = new RequestContext(header, connectionId, channel.socketAddress,
              channel.principal, listenerName, securityProtocol)
            //构建request对象
            val req = new RequestChannel.Request(processor = id, context = context,
              startTimeNanos = time.nanoseconds, memoryPool, receive.payload, requestChannel.metrics)
            //发送到requstQueue中，很关键，他会被监听器处理
            requestChannel.sendRequest(req)
            //设置为mute
            selector.mute(connectionId)
            //处理mute事件
            handleChannelMuteEvent(connectionId, ChannelMuteEvent.REQUEST_RECEIVED)
          case None =>
            // This should never happen since completed receives are processed immediately after `poll()`
            throw new IllegalStateException(s"Channel ${receive.source} removed from selector before processing completed receive")
        }
      } catch {
        // note that even though we got an exception, we can assume that receive.source is valid.
        // Issues with constructing a valid receive object were handled earlier
        case e: Throwable =>
          processChannelException(receive.source, s"Exception while processing request from ${receive.source}", e)
      }
    }
  }


  /**
   *
   * 这里其实目的是为了调用完成发送的回调事件[[onComplete]]
   *
   * 这个事件主要发生在处理完成逻辑后将将数据写入到channel，然后提交的事件。
   * 所以他调用的内容跟具体的onComplete方法有关系
   */
  private def processCompletedSends() {
    selector.completedSends.asScala.foreach { send =>
      try {
        val response = inflightResponses.remove(send.destination).getOrElse {
          throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
        }
        updateRequestMetrics(response)

        // Invoke send completion callback
        //回调response的complete函数
        response.onComplete.foreach(onComplete => onComplete(send))

        // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
        // it will be unmuted immediately. If the channel has been throttled, it will unmuted only if the throttling
        // delay has already passed by now.
        //处理mute时间
        handleChannelMuteEvent(send.destination, ChannelMuteEvent.RESPONSE_SENT)
        //解开mute
        tryUnmuteChannel(send.destination)
      } catch {
        case e: Throwable => processChannelException(send.destination,
          s"Exception while processing completed send to ${send.destination}", e)
      }
    }
  }

  private def updateRequestMetrics(response: RequestChannel.Response): Unit = {
    val request = response.request
    val networkThreadTimeNanos = openOrClosingChannel(request.context.connectionId).fold(0L)(_.getAndResetNetworkThreadTimeNanos())
    request.updateRequestMetrics(networkThreadTimeNanos, response)
  }

  private def processDisconnected() {
    selector.disconnected.keySet.asScala.foreach { connectionId =>
      try {
        val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
          throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
        }.remoteHost
        //移除正在处理的列表
        inflightResponses.remove(connectionId).foreach(updateRequestMetrics)
        // the channel has been closed by the selector but the quotas still need to be updated
        connectionQuotas.dec(InetAddress.getByName(remoteHost))
      } catch {
        case e: Throwable => processException(s"Exception while processing disconnection of $connectionId", e)
      }
    }
  }

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   * The channel will be immediately removed from the selector's `channels` or `closingChannels`
   * and no further disconnect notifications will be sent for this channel by the selector.
   * If responses are pending for the channel, they are dropped and metrics is updated.
   * If the channel has already been removed from selector, no action is taken.
   */
  private def close(connectionId: String): Unit = {
    openOrClosingChannel(connectionId).foreach { channel =>
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(address)
      selector.close(connectionId)

      inflightResponses.remove(connectionId).foreach(response => updateRequestMetrics(response))
    }
  }

  /**
   * Queue up a new connection for reading
   *
   * 跟acceptor是类似的，看
   * run方法
   */
  def accept(socketChannel: SocketChannel) {
    //记录到连接中
    newConnections.add(socketChannel)
    //唤醒等待的processor
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   */
  private def configureNewConnections() {
    while (!newConnections.isEmpty) {
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        selector.register(connectionId(channel.socket), channel)
      } catch {
        // We explicitly catch all exceptions and close the socket to avoid a socket leak.
        case e: Throwable =>
          val remoteAddress = channel.socket.getRemoteSocketAddress
          // need to close the channel here to avoid a socket leak.
          close(channel)
          processException(s"Processor $id closed connection from $remoteAddress", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll() {
    selector.channels.asScala.foreach { channel =>
      close(channel.id)
    }
    selector.close()
    removeMetric(IdlePercentMetricName, Map(NetworkProcessorMetricTag -> id.toString))
  }

  // 'protected` to allow override for testing
  protected[network] def connectionId(socket: Socket): String = {
    val localHost = socket.getLocalAddress.getHostAddress
    val localPort = socket.getLocalPort
    val remoteHost = socket.getInetAddress.getHostAddress
    val remotePort = socket.getPort
    val connId = ConnectionId(localHost, localPort, remoteHost, remotePort, nextConnectionIndex).toString
    nextConnectionIndex = if (nextConnectionIndex == Int.MaxValue) 0 else nextConnectionIndex + 1
    connId
  }

  private[network] def enqueueResponse(response: RequestChannel.Response): Unit = {
    responseQueue.put(response)
    wakeup()
  }

  private def dequeueResponse(): RequestChannel.Response = {
    val response = responseQueue.poll()
    if (response != null)
      response.request.responseDequeueTimeNanos = Time.SYSTEM.nanoseconds
    response
  }

  private[network] def responseQueueSize = responseQueue.size

  // Only for testing
  private[network] def inflightResponseCount: Int = inflightResponses.size

  // Visible for testing
  // Only methods that are safe to call on a disconnected channel should be invoked on 'openOrClosingChannel'.
  private[network] def openOrClosingChannel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId)).orElse(Option(selector.closingChannel(connectionId)))

  // Indicate the specified channel that the specified channel mute-related event has happened so that it can change its
  // mute state.
  private def handleChannelMuteEvent(connectionId: String, event: ChannelMuteEvent): Unit = {
    openOrClosingChannel(connectionId).foreach(c => c.handleChannelMuteEvent(event))
  }

  private def tryUnmuteChannel(connectionId: String) = {
    openOrClosingChannel(connectionId).foreach(c => selector.unmute(c.id))
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  // Visible for testing
  private[network] def numStagedReceives(connectionId: String): Int =
    openOrClosingChannel(connectionId).map(c => selector.numStagedReceives(c)).getOrElse(0)

  /**
   * Wakeup the thread for selection.
   */
  override def wakeup() = selector.wakeup()

  override def shutdown(): Unit = {
    super.shutdown()
    removeMetric("IdlePercent", Map("networkProcessor" -> id.toString))
  }

}

class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

  @volatile private var defaultMaxConnectionsPerIp = defaultMax
  @volatile private var maxConnectionsPerIpOverrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  private val counts = mutable.Map[InetAddress, Int]()

  def inc(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      val max = maxConnectionsPerIpOverrides.getOrElse(address, defaultMaxConnectionsPerIp)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  def updateMaxConnectionsPerIp(maxConnectionsPerIp: Int): Unit = {
    defaultMaxConnectionsPerIp = maxConnectionsPerIp
  }

  def updateMaxConnectionsPerIpOverride(overrideQuotas: Map[String, Int]): Unit = {
    maxConnectionsPerIpOverrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  }

  def dec(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
