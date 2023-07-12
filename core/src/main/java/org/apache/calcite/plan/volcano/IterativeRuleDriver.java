/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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
package org.apache.calcite.plan.volcano;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

/***
 * The algorithm executes repeatedly. The exact rules
 * that may be fired varies.
 *
 * <p>The planner iterates over the rule matches presented
 * by the rule queue until the rule queue becomes empty.
 */
class IterativeRuleDriver implements RuleDriver {

  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  private final VolcanoPlanner planner;
  private final IterativeRuleQueue ruleQueue;  // 这个ruleQueue是根据传入参数planner来构建出来的

  IterativeRuleDriver(VolcanoPlanner planner) {
    this.planner = planner;
    ruleQueue = new IterativeRuleQueue(planner);
  }

  @Override public IterativeRuleQueue getRuleQueue() {
    return ruleQueue;
  }

  @Override public void drive() {
    // 这里是一个死循环,只有等到超时，才会跳出这个匹配
    while (true) {
      assert planner.root != null : "RelSubset must not be null at this point";
      LOGGER.debug("Best cost before rule match: {}", planner.root.bestCost);

      // VolcanoRuleMatch是一个VolcanoRuleCall的子类
      // 从rules列表中给pop出一个来,这个ruleQueue里面的rules就是从planner里面拿到的
      VolcanoRuleMatch match = ruleQueue.popMatch();
      // 明天看看这里为啥每次pop出来的都是有用的,就用testSwapInnerJoin这个例子来入手
      // 意思是出来的rule，调用这个rule的match函数，为啥里面  call.rel(0) 这种类型转换都是对的
      if (match == null) {
        break;
      }

      assert match.getRule().matches(match);
      try {
        match.onMatch();
      } catch (VolcanoTimeoutException e) {
        LOGGER.warn("Volcano planning times out, cancels the subsequent optimization.");
        planner.canonize();
        break;
      }

      // The root may have been merged with another
      // subset. Find the new root subset.
      planner.canonize();
    }

  }

  @Override public void onProduce(RelNode rel, RelSubset subset) {
  }

  @Override public void onSetMerged(RelSet set) {
  }

  @Override public void clear() {
    ruleQueue.clear();
  }
}
