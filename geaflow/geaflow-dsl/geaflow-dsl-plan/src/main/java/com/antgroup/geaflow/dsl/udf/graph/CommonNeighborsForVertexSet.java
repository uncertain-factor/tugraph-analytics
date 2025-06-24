
package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.HashSet;

@Description(name = "common_neighbors_for_vertex_set", description = "built-in udga for CommonNeighborsForVertexSet")
public class CommonNeighborsForVertexSet implements AlgorithmUserFunction<Object, Tuple<Boolean, Boolean>> {
    private AlgorithmRuntimeContext<Object, Tuple<Boolean, Boolean>> context;
    HashSet<Object> A = new HashSet<>(); // 存储A集合
    HashSet<Object> B = new HashSet<>(); // 存储B集合

    @Override
    public void init(AlgorithmRuntimeContext<Object, Tuple<Boolean, Boolean>> context, Object[] params) {
        this.context = context;
        Boolean sep = false;

        //params 形如(3, '|', 2, 5),'|'前属于集合A,'|'后属于集合B,遍历params，将A集合和B集合的顶点分别存储到对应的HashSet中
        for (Object param : params) {
            if (sep) {
                B.add(TypeCastUtil.cast(param, context.getGraphSchema().getIdType()));
            } else {
                if (param.equals("|")) {
                    sep = true;
                } else {
                    A.add(TypeCastUtil.cast(param, context.getGraphSchema().getIdType()));
                }
            }
        }

        if(!sep)
        {
            throw new IllegalArgumentException("Invalid parameters, usage: common_neighbors_for_vertex_set(id_a1, id_a2, ..., '|', id_b1, id_b2, ...)");
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Tuple<Boolean, Boolean>> messages) {
        if (context.getCurrentIterationId() == 1L) {
            // 第一轮：A和B集合中的顶点向邻居发送标识消息
            Tuple<Boolean, Boolean> f = new Tuple<>(false, false);
            // 根据自身顶点属于A集合还是B集合来将f置为True
            if(A.contains(vertex.getId())) {
                f.setF0(true);
            } else if (B.contains(vertex.getId())) {
                f.setF1(true);
            }
            sendMessageToNeighbors(context.loadEdges(EdgeDirection.BOTH), f);
        } else if (context.getCurrentIterationId() == 2L) {
            // 第二轮：检查是否同时收到A和B的消息
            Tuple<Boolean, Boolean> received = new Tuple<>(false, false);
            // 解析收到的其他顶点的消息
            while(messages.hasNext()) {
                Tuple<Boolean, Boolean> message = messages.next();
                if (message.getF0()) {
                    received.setF0(true);
                }
                if (message.getF1()) {
                    received.setF1(true);
                }
            }
            // 如果同时收到A和B的消息，则为共同邻居
            if (received.getF0() && received.getF1()) {
                context.take(ObjectRow.create(vertex.getId()));
            }
        }
    }

    private void sendMessageToNeighbors(List<RowEdge> edges, Tuple<Boolean, Boolean> message) {
        for (RowEdge rowEdge : edges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
                new TableField("id", graphSchema.getIdType(), false));
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
    }
}