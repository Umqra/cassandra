/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.transform;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;

/// This test written by analogy with RTTransformationsTest.java
public final class PartitionConcatTests
{
    private static final String KEYSPACE = "PartitionConcatTests";
    private static final String TABLE = "table";

    private final int nowInSec = FBUtilities.nowInSeconds();

    private CFMetaData metadata;
    private DecoratedKey key;

    @Before
    public void setUp()
    {
        metadata =
        CFMetaData.Builder
        .create(KEYSPACE, TABLE)
        .addPartitionKey("pk", UTF8Type.instance)
        .addClusteringColumn("ck0", UTF8Type.instance)
        .addClusteringColumn("ck1", UTF8Type.instance)
        .addClusteringColumn("ck2", UTF8Type.instance)
        .withPartitioner(Murmur3Partitioner.instance)
        .build();
        key = Murmur3Partitioner.instance.decorateKey(bytes("key"));
    }

    @Test
    public void testConcatPartitionWithThriftLimit()
    {
        DataLimits.Counter counter = DataLimits.thriftLimits(1, Integer.MAX_VALUE).newCounter(0, false, false, true);
        FilteredPartitions iterator1 = FilteredPartitions.filter(iter(true, row(1, "a", "1", "")), new Filter(0, true));
        FilteredPartitions iterator2 = FilteredPartitions.filter(iter(false, row(1, "a", "1", "")), new Filter(0, true));
        FilteredPartitions iterator3 = FilteredPartitions.filter(iter(false, row(1, "a", "1", "")), new Filter(0, true));
        int count = 0;
        try (PartitionIterator results = PartitionIterators.concat(Arrays.asList(Transformation.apply(iterator1, counter), iterator2, iterator3)))
        {
            while (results.hasNext())
            {
                try (RowIterator ignored = results.next())
                {
                    count++;
                }
            }
        }
        assertEquals(3, count);
    }

    private Row row(long timestamp, Object... clusteringValues)
    {
        ByteBuffer[] clusteringByteBuffers = new ByteBuffer[clusteringValues.length];
        for (int i = 0; i < clusteringValues.length; i++)
            clusteringByteBuffers[i] = decompose(metadata.clusteringColumns().get(i).type, clusteringValues[i]);

        return BTreeRow.noCellLiveRow(Clustering.make(clusteringByteBuffers), LivenessInfo.create(timestamp, nowInSec));
    }

    @SuppressWarnings("unchecked")
    private static <T> ByteBuffer decompose(AbstractType<?> type, T value)
    {
        return ((AbstractType<T>) type).decompose(value);
    }

    private UnfilteredPartitionIterator iter(boolean isReversedOrder, Unfiltered... unfiltereds)
    {
        Iterator<Unfiltered> iterator = Iterators.forArray(unfiltereds);

        UnfilteredRowIterator rowIter =
        new AbstractUnfilteredRowIterator(metadata,
                                          key,
                                          DeletionTime.LIVE,
                                          metadata.partitionColumns(),
                                          Rows.EMPTY_STATIC_ROW,
                                          isReversedOrder,
                                          EncodingStats.NO_STATS)
        {
            protected Unfiltered computeNext()
            {
                return iterator.hasNext() ? iterator.next() : endOfData();
            }
        };

        return new SingletonUnfilteredPartitionIterator(rowIter, false);
    }
}
