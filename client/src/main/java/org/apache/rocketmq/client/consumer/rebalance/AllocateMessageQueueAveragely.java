/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();


    /**
     * 平均分配，默认
     * @param consumerGroup current consumer group
     * @param currentCID current consumer id
     * @param mqAll message queue set in current topic
     * @param cidAll consumer set in current consumer group
     * @return
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        /**
         *  consumerGroup:消费者组名
         *  currentCID：当前客户端is
         *  mqAll:某个topic对应的messagequeue
         *  cidAll:消费者组下的客户端id
         *
         */

        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }
        //计算自己在消费者列表中的位置
        int index = cidAll.indexOf(currentCID);
        /**
         * messagequeue的个数 模 消费者组下客户端的个数
         */
        //计算平均分配后 多出来的数量 余数
        int mod = mqAll.size() % cidAll.size();

        /**
         *  计算每个客户端能分到的messagequeue
         *  1、如果messagequeue的大小 < 客户端的大小 averageSize = 1
         *  2、否则 如果余数 > 0 并且当前客户端id在客户端列表的顺序 > 余数 则当前客户端id分配到的messagequeue 为 商 + 1，如果客户端列表的顺序 < 余数 ，则分配到的messagequeue为商
         *
         */
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());
        // //计算分配给自己的队列列表中的起始位置
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        // 计算range，即分配给自己几个队列。
        // 由于计算出的averageSize最小为1，当队列数量是小于消费者数量，多出来的消费者应该分配为0，所
        //以取(mqAll.size() - startIndex)
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        ////从起始位置开始，循环range次，把对应的队列分配给自己  分配当前客户端应得的messagequeue
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
