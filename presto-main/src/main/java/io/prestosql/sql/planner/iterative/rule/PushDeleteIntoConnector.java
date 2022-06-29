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
package io.prestosql.sql.planner.iterative.rule;

import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.TableDeleteNode;
import io.prestosql.sql.planner.plan.TableFinishNode;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.plan.Patterns.delete;
import static io.prestosql.sql.planner.plan.Patterns.exchange;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableFinish;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

public class PushDeleteIntoConnector
        implements Rule<TableFinishNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<TableFinishNode> PATTERN_WITHOUT_PROJECT =
            tableFinish().with(source().matching(
                    exchange().with(source().matching(
                            delete().with(source().matching(
                                    tableScan().capturedAs(TABLE_SCAN)))))));

    private static final Pattern<TableFinishNode> PATTERN_WITH_EXCAHNGE =
            tableFinish().with(source().matching(
                    exchange().with(source().matching(
                            delete().with(source().matching(
                                    project().with(source().matching(
                                            tableScan().capturedAs(TABLE_SCAN)))))))));

    private final Metadata metadata;
    private final boolean withExchange;

    public PushDeleteIntoConnector(Metadata metadata, boolean withExchange)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.withExchange = withExchange;
    }

    @Override
    public Pattern<TableFinishNode> getPattern()
    {
        if (withExchange) {
            return PATTERN_WITH_EXCAHNGE;
        }

        return PATTERN_WITHOUT_PROJECT;
    }

    @Override
    public Result apply(TableFinishNode node, Captures captures, Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        return Optional.of(tableScan.getTable())
                .map(newHandle -> new TableDeleteNode(
                        context.getIdAllocator().getNextId(),
                        newHandle,
                        getOnlyElement(node.getOutputSymbols())))
                .map(Result::ofPlanNode)
                .orElseGet(Result::empty);
    }
}
