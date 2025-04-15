/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.runtime.operator.groupby;

import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Iterator;


public class OneLongKeyGroupIdGenerator implements GroupIdGenerator {
  private final Long2IntOpenHashMap _groupIdMap;
  private final int _numGroupsLimit;

  private int _numGroups = 0;
  private int _nullGroupId = INVALID_ID;

  public OneLongKeyGroupIdGenerator(int numGroupsLimit, int initialCapacity) {
    _groupIdMap = new Long2IntOpenHashMap(initialCapacity);
    _groupIdMap.defaultReturnValue(INVALID_ID);
    _numGroupsLimit = numGroupsLimit;
  }

  @Override
  public int getGroupId(Object key) {
    if (_numGroups < _numGroupsLimit) {
      if (key == null) {
        if (_nullGroupId == INVALID_ID) {
          _nullGroupId = _numGroups++;
        }
        return _nullGroupId;
      }
      int groupId = _groupIdMap.computeIfAbsent((long) key, k -> _numGroups);
      if (groupId == _numGroups) {
        _numGroups++;
      }
      return groupId;
    } else {
      if (key == null) {
        return _nullGroupId;
      }
      return _groupIdMap.get((long) key);
    }
  }

  @Override
  public int getNumGroups() {
    return _numGroups;
  }

  @Override
  public Iterator<GroupKey> getGroupKeyIterator(int numColumns) {
    return new Iterator<GroupKey>() {
      final ObjectIterator<Long2IntMap.Entry> _entryIterator = _groupIdMap.long2IntEntrySet().fastIterator();
      boolean _returnNull = _nullGroupId != INVALID_ID;

      @Override
      public boolean hasNext() {
        return _returnNull || _entryIterator.hasNext();
      }

      @Override
      public GroupKey next() {
        Object[] row = new Object[numColumns];
        if (_returnNull) {
          _returnNull = false;
          return new GroupKey(_nullGroupId, row);
        }
        Long2IntMap.Entry entry = _entryIterator.next();
        row[0] = entry.getLongKey();
        return new GroupKey(entry.getIntValue(), row);
      }
    };
  }
}
