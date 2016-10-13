package com.facebook.presto.row;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkState;

public class PageToRowOperatorFactory
        implements OperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final MetadataManager testMetadataManager;
    private final List<RowOperatorFactory> rowOperatorFactories;
    private final List<Type> inputTypes;
    private final List<Type> outputTypes;
    private final ListenableFuture<?> future;

    public PageToRowOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            MetadataManager testMetadataManager,
            List<RowOperatorFactory> rowOperatorFactories,
            List<Type> inputTypes,
            List<Type> outputTypes,
            ListenableFuture<?> future)
    {
        this.operatorId = operatorId;
        this.planNodeId = planNodeId;
        this.testMetadataManager = testMetadataManager;
        this.rowOperatorFactories = rowOperatorFactories;
        this.inputTypes = inputTypes;
        this.outputTypes = outputTypes;
        this.future = future;
    }

    @Override
    public List<Type> getTypes()
    {
        return outputTypes;
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        ImmutableList.Builder<RowOperator> rowOperators = ImmutableList.builder();
        for (RowOperatorFactory rowOperatorFactory : rowOperatorFactories) {
            rowOperators.add(rowOperatorFactory.createOperator(driverContext));
        }
        return new PageToRowOperator(
                driverContext.addOperatorContext(operatorId, planNodeId, PageToRowOperator.class.getSimpleName()),
                rowOperators.build(),
                outputTypes,
                future);
    }

    @Override
    public void close()
    {
        for (RowOperatorFactory rowOperatorFactory : rowOperatorFactories) {
            rowOperatorFactory.close();
        }
    }

    @Override
    public OperatorFactory duplicate()
    {
        return new PageToRowOperatorFactory(operatorId, planNodeId, testMetadataManager, rowOperatorFactories, inputTypes, outputTypes, future);
    }

    private static class PageToRowOperator
            implements Operator
    {
        private final OperatorContext operatorContext;

        private final RowOperator operator0;
        private final RowOperator operator1;
        private final List<Type> outputTypes;
        private final ListenableFuture<?> future;
        private boolean finishing;
        private final PageBuilder pageBuilder;
        private final Type col0Type;
        private final Type col1Type;
        private final Type col2Type;


        public PageToRowOperator(
                OperatorContext operatorContext,
                List<RowOperator> rowOperators,
                List<Type> outputTypes,
                ListenableFuture<?> future)
        {
            checkState(outputTypes.equals(ImmutableList.of(VARCHAR, BIGINT, BIGINT, VARCHAR, BIGINT, BIGINT, VARCHAR, BIGINT, BIGINT)));
            this.operatorContext = operatorContext;
            this.outputTypes = outputTypes;
            this.future = future;
            this.pageBuilder = new PageBuilder(outputTypes);

            checkState(rowOperators.size() == 2);

            operator0 = rowOperators.get(0);
            operator1 = rowOperators.get(1);
            col0Type = outputTypes.get(0);
            col1Type = outputTypes.get(1);
            col2Type = outputTypes.get(2);
        }

        @Override
        public ListenableFuture<?> isBlocked()
        {
            return future;
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public List<Type> getTypes()
        {
            return outputTypes;
        }

        @Override
        public void finish()
        {
            finishing = true;
        }

        @Override
        public boolean isFinished()
        {
            return finishing && pageBuilder.isEmpty();
        }

        @Override
        public boolean needsInput()
        {
            return true;
        }

        @Override
        public void addInput(Page page)
        {
            operator0.prepare();
            operator1.prepare();

            TestRowObject rowObject = new TestRowObject();
            for (int position = 0; position < page.getPositionCount(); position++) {
                rowObject.col0 = col0Type.getSlice(page.getBlock(0), position);
                rowObject.col1 = col1Type.getLong(page.getBlock(1), position);
                rowObject.col2 = col2Type.getLong(page.getBlock(2), position);

                // TODO can this loop be native java?
                operator0.addInput(rowObject);
                RowObject output0 = operator0.getOutput();
                while (output0 != null) {
                    operator1.addInput(output0);
                    RowObject output1 = operator1.getOutput();
                    while (output1 != null) {
                        write(output1);
                        output1 = operator1.getOutput();
                    }
                    output0 = operator0.getOutput();
                }
            }
        }

        private void write(RowObject rowObject)
        {
            rowObject.appendTo(pageBuilder);
        }

        @Override
        public Page getOutput()
        {
            if (pageBuilder.isFull() || finishing) {
                Page page = pageBuilder.build();
                pageBuilder.reset();
                return page;
            }
            return null;
        }
    }
}
