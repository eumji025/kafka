/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import kafka.server.QuotaType._
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.server.quota.ClientQuotaCallback
import org.apache.kafka.common.utils.Time

/**
 * 限速类型
 * quota表示的是 配额
 */
object QuotaType  {
  //fetch，表示的是副本拉取kafka日志
  case object Fetch extends QuotaType
  //表示生产
  case object Produce extends QuotaType
  //表示的是request类型
  case object Request extends QuotaType
  //表示的是leader的副本
  case object LeaderReplication extends QuotaType
  //表示的是follower的副本
  case object FollowerReplication extends QuotaType
  //alterLog还不知道是干嘛的
  case object AlterLogDirsReplication extends QuotaType
}
sealed trait QuotaType

object QuotaFactory extends Logging {

  object UnboundedQuota extends ReplicaQuota {
    override def isThrottled(topicPartition: TopicPartition): Boolean = false
    override def isQuotaExceeded: Boolean = false
    def record(value: Long): Unit = ()
  }

  case class QuotaManagers(fetch: ClientQuotaManager,
                           produce: ClientQuotaManager,
                           request: ClientRequestQuotaManager,
                           leader: ReplicationQuotaManager,
                           follower: ReplicationQuotaManager,
                           alterLogDirs: ReplicationQuotaManager,
                           clientQuotaCallback: Option[ClientQuotaCallback]) {
    def shutdown() {
      fetch.shutdown
      produce.shutdown
      request.shutdown
      clientQuotaCallback.foreach(_.close())
    }
  }

  def instantiate(cfg: KafkaConfig, metrics: Metrics, time: Time, threadNamePrefix: String): QuotaManagers = {

    val clientQuotaCallback = Option(cfg.getConfiguredInstance(KafkaConfig.ClientQuotaCallbackClassProp,
      classOf[ClientQuotaCallback]))

    //构建限速的管理器，主要还是为了防止机器负载过高，挂机
    //非常有用，目前只看过fetch的相关内容，主要就是防止同步的操作占用大量的资源，所以对速率进行控制，具体可以通过配置进行限制
    QuotaManagers(
      new ClientQuotaManager(clientFetchConfig(cfg), metrics, Fetch, time, threadNamePrefix, clientQuotaCallback),
      new ClientQuotaManager(clientProduceConfig(cfg), metrics, Produce, time, threadNamePrefix, clientQuotaCallback),
      new ClientRequestQuotaManager(clientRequestConfig(cfg), metrics, time, threadNamePrefix, clientQuotaCallback),
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, LeaderReplication, time),
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, FollowerReplication, time),
      new ReplicationQuotaManager(alterLogDirsReplicationConfig(cfg), metrics, AlterLogDirsReplication, time),
      clientQuotaCallback
    )
  }

  def clientProduceConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    if (cfg.producerQuotaBytesPerSecondDefault != Long.MaxValue)
      warn(s"${KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp} has been deprecated in 0.11.0.0 and will be removed in a future release. Use dynamic quota defaults instead.")
    ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.producerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )
  }

  def clientFetchConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    if (cfg.consumerQuotaBytesPerSecondDefault != Long.MaxValue)
      warn(s"${KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp} has been deprecated in 0.11.0.0 and will be removed in a future release. Use dynamic quota defaults instead.")
    ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.consumerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )
  }

  def clientRequestConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    ClientQuotaManagerConfig(
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )
  }

  def replicationConfig(cfg: KafkaConfig): ReplicationQuotaManagerConfig = {
    ReplicationQuotaManagerConfig(
      numQuotaSamples = cfg.numReplicationQuotaSamples,
      quotaWindowSizeSeconds = cfg.replicationQuotaWindowSizeSeconds
    )
  }

  def alterLogDirsReplicationConfig(cfg: KafkaConfig): ReplicationQuotaManagerConfig = {
    ReplicationQuotaManagerConfig(
      numQuotaSamples = cfg.numAlterLogDirsReplicationQuotaSamples,
      quotaWindowSizeSeconds = cfg.alterLogDirsReplicationQuotaWindowSizeSeconds
    )
  }

}
