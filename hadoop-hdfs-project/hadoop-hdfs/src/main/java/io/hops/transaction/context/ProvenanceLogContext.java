/*
 * Copyright 2018 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.transaction.context;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.dal.ProvenanceLogDataAccess;
import io.hops.metadata.hdfs.entity.ProvenanceLogEntry;
import io.hops.transaction.lock.TransactionLocks;
import java.util.concurrent.TimeUnit;

public class ProvenanceLogContext extends BaseEntityContext<ProvenanceLogContext.DBKey, ProvenanceLogEntry> {

  private static final long PROVENANCE_CACHE_EXPIRATION_TIME = 5000;
  private static final long PROVENANCE_CACHE_MAX_SIZE = 1000;
  //TODO Alex - add cache usage
  private static final Cache<CacheKey, ProvenanceLogEntry> CACHE;

  static {
    CACHE = Caffeine.newBuilder()
      .expireAfterWrite(PROVENANCE_CACHE_EXPIRATION_TIME, TimeUnit.MILLISECONDS)
      .maximumSize(PROVENANCE_CACHE_MAX_SIZE)
      .build();
  }
  private final ProvenanceLogDataAccess<ProvenanceLogEntry> dataAccess;

  class CacheKey {

    private int inodeId;
    private int userId;
    private int appId;

    public CacheKey(int inodeId, int userId, int appId) {
      this.inodeId = inodeId;
      this.userId = userId;
      this.appId = appId;
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 79 * hash + this.inodeId;
      hash = 79 * hash + this.userId;
      hash = 79 * hash + this.appId;
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final CacheKey other = (CacheKey) obj;
      if (this.inodeId != other.inodeId) {
        return false;
      }
      if (this.userId != other.userId) {
        return false;
      }
      if (this.appId != other.appId) {
        return false;
      }
      return true;
    }
  }

  class DBKey {

    private int inodeId;
    private int userId;
    private int appId;
    private long timestamp;

    public DBKey(int inodeId, int userId, int appId, long timestamp) {
      this.inodeId = inodeId;
      this.userId = userId;
      this.appId = appId;
      this.timestamp = timestamp;
    }

    private CacheKey getCacheKey() {
      return new CacheKey(inodeId, userId, appId);
    }
    
    @Override
    public int hashCode() {
      int hash = 7;
      hash = 71 * hash + this.inodeId;
      hash = 71 * hash + this.userId;
      hash = 71 * hash + this.appId;
      hash = 71 * hash + (int) (this.timestamp ^ (this.timestamp >>> 32));
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final DBKey other = (DBKey) obj;
      if (this.inodeId != other.inodeId) {
        return false;
      }
      if (this.userId != other.userId) {
        return false;
      }
      if (this.appId != other.appId) {
        return false;
      }
      if (this.timestamp != other.timestamp) {
        return false;
      }
      return true;
    }

  }

  public ProvenanceLogContext(ProvenanceLogDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(ProvenanceLogEntry logEntry)
    throws TransactionContextException {
    DBKey dbKey = getKey(logEntry);
    CacheKey cacheKey = dbKey.getCacheKey();
    if (get(dbKey) != null) {
      throw new RuntimeException("Conflicting logical time in the "
        + "ProvenanceLogEntry");
    }
    
    super.add(logEntry);
    log("provenance-log-added", "inodeId", logEntry.getInodeId(), "userId",
      logEntry.getUserId(), "appId", logEntry.getAppId(), "operation",
      logEntry.getOperation());
  }

  @Override
  DBKey getKey(ProvenanceLogEntry logEntry) {
    return new DBKey(logEntry.getInodeId(), logEntry.getUserId(),
      logEntry.getAppId(), logEntry.getLogicalTime());
  }

  @Override
  public void prepare(TransactionLocks tlm)
    throws TransactionContextException, StorageException {
    dataAccess.addAll(getAdded());
  }

}
