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
package kafka.utils.timer

import kafka.utils.nonthreadsafe

import java.util.concurrent.DelayQueue
import java.util.concurrent.atomic.AtomicInteger

/**
  * Hierarchical（分层） Timing Wheels
  *
  *
  * 时间轮，目的是为了使用更加高效的手段来实现高吞吐的队列。 他和普通的队列是有区别的、
  * 其实现使用过类似于时钟的环形队列（可定义个数和每个格子对应的时间区间）其设计是基于当前的时间相对超时时间来计算的（假设一圈有10格，每格的时间区间是200ms,每个格子上都存放着一个列表）。那么一轮的时间就是2000s，
  * 当超时时间大于当前时间的2000ms，那么就无法在当前轮存在，因为超过了一个周期（你可能会想是不是可以通过取模来）但是时间轮的操作并不是这样的
  * 本身是通过在自己内部存放个一个下级时间轮（无限递归）
  * note: 需要注意的是，下级的一个格子的周期对应的是上一级全部给的时间区间也就是2000ms（这就使得当前的轮可以容纳更多的数据，其道理就是当上层轮完一圈之后，将下面的一个格子往上浮，暂时不讲怎么实现）
  *
  * image link： https://segmentfault.com/img/remote/1460000005349031
  *
  * 时间轮拨动 {@link SystemTimer}
  *
  *
  *
  *
  *
  * A simple timing wheel is a circular（圆形） list of buckets of timer tasks. Let u be the time unit.
  * A timing wheel with size n has n buckets and can hold timer tasks in n * u time interval（间隔）.
  * Each bucket holds timer tasks that fall into the corresponding（相应的） time range. At the beginning,
  * the first bucket holds tasks for [0, u), the second bucket holds tasks for [u, 2u), …,
  * the n-th bucket for [u * (n -1), u * n). Every interval of time unit u, the timer ticks and
  * moved to the next bucket then expire all timer tasks in it. So, the timer never insert a task
  * into the bucket for the current time since it is already expired. The timer immediately runs
  * the expired task. The emptied bucket is then available for the next round, so if the current
  * bucket is for the time t, it becomes the bucket for [t + u * n, t + (n + 1) * u) after a tick.
  * A timing wheel has O(1) cost for insert/delete (start-timer/stop-timer) whereas priority queue
  * based timers, such as java.util.concurrent.DelayQueue and java.util.Timer, have O(log n)
  * insert/delete cost.
  *
  * A major drawback of a simple timing wheel is that it assumes that a timer request is within
  * the time interval of n * u from the current time. If a timer request is out of this interval,
  * it is an overflow. A hierarchical timing wheel deals with such overflows. It is a hierarchically
  * organized timing wheels. The lowest level has the finest time resolution. As moving up the
  * hierarchy, time resolutions become coarser. If the resolution of a wheel at one level is u and
  * the size is n, the resolution of the next level should be n * u. At each level overflows are
  * delegated to the wheel in one level higher. When the wheel in the higher level ticks, it reinsert
  * timer tasks to the lower level. An overflow wheel can be created on-demand. When a bucket in an
  * overflow bucket expires, all tasks in it are reinserted into the timer recursively. The tasks
  * are then moved to the finer grain wheels or be executed. The insert (start-timer) cost is O(m)
  * where m is the number of wheels, which is usually very small compared to the number of requests
  * in the system, and the delete (stop-timer) cost is still O(1).
  *
  * Example
  * Let's say that u is 1 and n is 3. If the start time is c,
  * then the buckets at different levels are:
  *
  * level    buckets
  * 1        [c,c]   [c+1,c+1]  [c+2,c+2]
  * 2        [c,c+2] [c+3,c+5]  [c+6,c+8]
  * 3        [c,c+8] [c+9,c+17] [c+18,c+26]
  *
  * The bucket expiration is at the time of bucket beginning.
  * So at time = c+1, buckets [c,c], [c,c+2] and [c,c+8] are expired.
  * Level 1's clock moves to c+1, and [c+3,c+3] is created.
  * Level 2 and level3's clock stay at c since their clocks move in unit of 3 and 9, respectively.
  * So, no new buckets are created in level 2 and 3.
  *
  * Note that bucket [c,c+2] in level 2 won't receive any task since that range is already covered in level 1.
  * The same is true for the bucket [c,c+8] in level 3 since its range is covered in level 2.
  * This is a bit wasteful, but simplifies the implementation.
  *
  * 1        [c+1,c+1]  [c+2,c+2]  [c+3,c+3]
  * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
  * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
  *
  * At time = c+2, [c+1,c+1] is newly expired.
  * Level 1 moves to c+2, and [c+4,c+4] is created,
  *
  * 1        [c+2,c+2]  [c+3,c+3]  [c+4,c+4]
  * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
  * 3        [c,c+8]    [c+9,c+17] [c+18,c+18]
  *
  * At time = c+3, [c+2,c+2] is newly expired.
  * Level 2 moves to c+3, and [c+5,c+5] and [c+9,c+11] are created.
  * Level 3 stay at c.
  *
  * 1        [c+3,c+3]  [c+4,c+4]  [c+5,c+5]
  * 2        [c+3,c+5]  [c+6,c+8]  [c+9,c+11]
  * 3        [c,c+8]    [c+9,c+17] [c+8,c+11]
  *
  * The hierarchical timing wheels works especially well when operations are completed before they time out.
  * Even when everything times out, it still has advantageous when there are many items in the timer.
  * Its insert cost (including reinsert) and delete cost are O(m) and O(1), respectively while priority
  * queue based timers takes O(log N) for both insert and delete where N is the number of items in the queue.
  *
  * This class is not thread-safe. There should not be any add calls while advanceClock is executing.
  * It is caller's responsibility to enforce it. Simultaneous add calls are thread-safe.
  */
@nonthreadsafe
private[timer] class TimingWheel(tickMs: Long, wheelSize: Int, startMs: Long, taskCounter: AtomicInteger, queue: DelayQueue[TimerTaskList]) {

  //周期（当前时间轮的总时长）
  private[this] val interval = tickMs * wheelSize
  //构建一个槽位列表
  private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) }
  //当前时间
  private[this] var currentTime = startMs - (startMs % tickMs) // rounding down to multiple of tickMs

  // overflowWheel can potentially be updated and read by two concurrent threads through add().
  // Therefore, it needs to be volatile due to the issue of Double-Checked Locking pattern with JVM
  @volatile private[this] var overflowWheel: TimingWheel = null

  private[this] def addOverflowWheel(): Unit = {
    synchronized {
      if (overflowWheel == null) {
        overflowWheel = new TimingWheel(
          tickMs = interval, //一个槽位的时间
          wheelSize = wheelSize, //槽位的数量
          startMs = currentTime, //开始的时间
          taskCounter = taskCounter, //任务数量
          queue //任务队列
        )
      }
    }
  }

  /**
    * 记录延时任务的方法
    * 1.如果是超时的任务，或者已取消任务则不需要添加
    * 2.对于正常的任务，如果当前层的周期大于超时的周期，都会记录到时间轮的制定位置
    * 然后根据槽位设置对应的位置，并计入延时队列
    * 否则需要进行下一轮的时间轮add尝试（递归）
    *
    * @param timerTaskEntry
    * @return
    */
  def add(timerTaskEntry: TimerTaskEntry): Boolean = {
    val expiration = timerTaskEntry.expirationMs

    if (timerTaskEntry.cancelled) {
      //如果任务被取消
      // Cancelled
      false
    } else if (expiration < currentTime + tickMs) {
      //过期
      // Already expired
      false
    } else if (expiration < currentTime + interval) {
      //在当前周期里[比如当前是2号位，则有可能命中1号位]
      // Put in its own bucket
      val virtualId = expiration / tickMs
      //余除取到位置
      val bucket = buckets((virtualId % wheelSize.toLong).toInt)
      bucket.add(timerTaskEntry)

      // Set the bucket expiration time
      if (bucket.setExpiration(virtualId * tickMs)) {
        //设置过期时间
        // The bucket needs to be enqueued because it was an expired bucket
        // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
        // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
        // will pass in the same value and hence return false, thus the bucket with the same expiration will not
        // be enqueued multiple times.
        queue.offer(bucket) //记录到延时队列中
      }
      true
    } else {
      //超过第一个轮的范围，则需要加入到下层的环形队列
      // Out of the interval. Put it into the parent timer
      if (overflowWheel == null) addOverflowWheel()
      overflowWheel.add(timerTaskEntry)
    }
  }

  // Try to advance the clock 调整时间
  def advanceClock(timeMs: Long): Unit = {
    if (timeMs >= currentTime + tickMs) {
      currentTime = timeMs - (timeMs % tickMs)

      // Try to advance the clock of the overflow wheel if present
      if (overflowWheel != null) overflowWheel.advanceClock(currentTime)
    }
  }
}
