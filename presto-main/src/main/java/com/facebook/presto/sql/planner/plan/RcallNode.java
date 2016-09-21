/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

@Immutable
public class RcallNode
        extends PlanNode
{
    private final PlanNode source;
    private final String rProgram;
    private final List<Symbol> params;
    private final Symbol outputSymbol;

    public RcallNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("rProgram") String rProgram,
            @JsonProperty("params") List<Symbol> params,
            @JsonProperty("outputSymbol") Symbol outputSymbol)
    {
        super(id);
        this.source = source;
        this.rProgram = rProgram;
        this.params = params;
        this.outputSymbol = outputSymbol;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
        outputSymbols.addAll(source.getOutputSymbols());
        outputSymbols.add(outputSymbol);
        return outputSymbols.build();
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public String getrProgram()
    {
        return rProgram;
    }

    @JsonProperty
    public List<Symbol> getParams()
    {
        return params;
    }

    @JsonProperty
    public Symbol getOutputSymbol()
    {
        return outputSymbol;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitRcall(this, context);
    }
}
