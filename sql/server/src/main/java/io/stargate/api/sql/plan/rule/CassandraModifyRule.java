/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.api.sql.plan.rule;

import io.stargate.api.sql.plan.rel.CassandraModify;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableModify;

public class CassandraModifyRule extends RelOptRule {
  public CassandraModifyRule() {
    super(operand(LogicalTableModify.class, any()), "CassandraModifyRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalTableModify modify = call.rel(0);
    RelOptCluster cluster = modify.getCluster();
    RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);
    RelNode input = convert(modify.getInput(), traitSet);
    call.transformTo(CassandraModify.create(modify, input, traitSet));
  }
}