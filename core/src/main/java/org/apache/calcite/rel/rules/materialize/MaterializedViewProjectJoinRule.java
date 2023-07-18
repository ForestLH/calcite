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
package org.apache.calcite.rel.rules.materialize;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

import org.immutables.value.Value;

// TODO 这里为什么他可以匹配上到这个rule，我要是想实现一个自己的rule，发生在Join->Filter，这种该如何做
/** Rule that matches Project on Join. */
@Value.Enclosing
public class MaterializedViewProjectJoinRule
    extends MaterializedViewJoinRule<MaterializedViewProjectJoinRule.Config> {

  private MaterializedViewProjectJoinRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public MaterializedViewProjectJoinRule(RelBuilderFactory relBuilderFactory,
      boolean generateUnionRewriting, HepProgram unionRewritingPullProgram,
      boolean fastBailOut) {
    this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withGenerateUnionRewriting(generateUnionRewriting)
        .withUnionRewritingPullProgram(unionRewritingPullProgram)
        .withFastBailOut(fastBailOut)
        .as(Config.class));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Join join = call.rel(1);
    perform(call, project, join);
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends MaterializedViewRule.Config {
    Config DEFAULT = ImmutableMaterializedViewProjectJoinRule.Config.builder()
        .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
        .withOperandSupplier(b0 ->
            b0.operand(Project.class).oneInput(b1 ->
                b1.operand(Join.class).anyInputs()))
        .withDescription("MaterializedViewJoinRule(Project-Join)")
        .withGenerateUnionRewriting(true)
        .withUnionRewritingPullProgram(null)
        .withFastBailOut(true)
        .build();

    @Override default MaterializedViewProjectJoinRule toRule() {
      return new MaterializedViewProjectJoinRule(this);
    }
  }
}
