/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Preconditions;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.FileProvXAttrBufferEntry;
import io.hops.metadata.hdfs.entity.FileProvenanceEntry;
import io.hops.metadata.hdfs.entity.INodeMetadataLogEntry;
import io.hops.security.GroupNotFoundException;
import io.hops.security.UserNotFoundException;
import io.hops.security.UsersGroups;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Optional;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.security.UserGroupInformation;

public abstract class INodeWithAdditionalFields extends INode {
  
  /** The inode id */
  final protected long id;
  /**
   * The inode name is in java UTF8 encoding;
   * The name in HdfsFileStatus should keep the same encoding as this.
   * if this encoding is changed, implicitly getFileInfo and listStatus in
   * clientProtocol are changed; The decoding at the client
   * side should change accordingly.
   */
  private byte[] name = null;
  
  private FsPermission permission;
  /** The last modification time */
  protected  long modificationTime = 0L;
  /** The last access time */
  protected long accessTime = 0L;
  
  private String userName;
  private String groupName;
  
  private int userId;
  private int groupId;
  
  long header = 0L;
  protected Long partitionId;
  
  /** An array {@link Feature}s. */
  private static final Feature[] EMPTY_FEATURE = new Feature[0];
  protected Feature[] features = EMPTY_FEATURE;
  
  private INodeWithAdditionalFields(INode parent, long id, byte[] name,
      PermissionStatus permission, long modificationTime, long accessTime, boolean inTree)
      throws IOException {
    super(parent);
    this.id = id;
    this.name = name;
    this.permission = permission.getPermission();
    this.userName = permission.getUserName();
    this.userId = getUserIDDB(userName);
    this.groupName = permission.getGroupName();
    this.groupId = getGroupIDDB(groupName);
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
    this.inTree = inTree;
  }

  INodeWithAdditionalFields(long id, byte[] name, PermissionStatus permission,
      long modificationTime, long accessTime, boolean inTree) throws IOException {
    this(null, id, name, permission, modificationTime, accessTime, inTree);
  }
  
  INodeWithAdditionalFields(INodeWithAdditionalFields other) throws IOException {
    this(other.getParent(), other.getId(), other.getLocalNameBytes(),
        other.getPermissionStatus(), other.modificationTime, other.accessTime, other.inTree);
    this.header = other.getHeader();
    this.partitionId = other.getPartitionId();
    this.parentId = other.getParentId();
    this.logicalTime = other.getLogicalTime();
  }
  
  INodeWithAdditionalFields(long id, String name, PermissionStatus permissions)
    throws IOException {
    this(id, name, permissions, false);
  }
  
  INodeWithAdditionalFields(long id, String name, PermissionStatus permissions, boolean inTree)
    throws IOException {
    this(null, id, DFSUtil.string2Bytes(name), permissions, 0L, 0L, inTree);
  }
  
  INodeWithAdditionalFields(long id, PermissionStatus permissions, long modificationTime, long accessTime)
    throws IOException {
    this(id, permissions, modificationTime, accessTime, false);
  }
  
  INodeWithAdditionalFields(long id, PermissionStatus permissions, long modificationTime,
      long accessTime, boolean inTree) throws IOException {
    this(id, null, permissions, modificationTime, accessTime, inTree);
  }
  
  /** Get the inode id */
  public long getId() {
    return this.id;
  }
  
  /**
   * Get local file name
   *
   * @return null if the local name is null;
   *          otherwise, return the local name byte array
   */
  @Override
  public final byte[] getLocalNameBytes() {
    return name;
  }
  
  @Override
  public final void setLocalName(byte[] name)
    throws StorageException, TransactionContextException {
    setLocalNameNoPersistance(name);
    save();
  }
  
  /** Clone the {@link FsPermission}. */
  final void clonePermission(INodeWithAdditionalFields that)
    throws StorageException, TransactionContextException {
    this.permission = that.permission;
    save();
  }
  
  @Override
  public final PermissionStatus getPermissionStatus() throws IOException {
    return new PermissionStatus(getUserName(), getGroupName(), getFsPermission());
  }
  
  /**
   * Set user
   */
  protected final void setUser(String user) throws IOException {
    setUserNoPersistance(user);
    save();
  }
  
  /**
   * Get user name
   */
  public final String getUserName() throws IOException {
    return userName;
  }
  
  public final int getUserID() {
    return userId;
  }
  
  public final int getGroupID() {
    return groupId;
  }
  
  /**
   * Get group name
   */
  public final String getGroupName() throws IOException {
    return groupName;
  }
  
  protected final void setGroup(String group) throws IOException {
    setGroupNoPersistance(group);
    save();
  }
  
  protected final short getFsPermissionShort() {
    return permission.toShort();
  }
  
  @Override
  void setPermission(FsPermission permission)
    throws StorageException, TransactionContextException {
    setPermissionNoPersistance(permission);
    save();
  }
  
  /**
   * Get last modification time of inode.
   *
   * @return access time
   */
  public long getModificationTime() {
    return this.modificationTime;
  }
  
  /**
   * Update modification time if it is larger than the current value.
   */
  @Override
  public final void updateModificationTime(long mtime)
      throws QuotaExceededException, StorageException, TransactionContextException {
    Preconditions.checkState(isDirectory());
    if (mtime <= modificationTime) {
      return;
    }
    setModificationTime(mtime);
  }
  
  public final void setModificationTime(long modificationTime)
    throws StorageException, TransactionContextException {
    setModificationTimeNoPersistance(modificationTime);
    save();
  }
  
  final void setModificationTimeForce(long modificationTime)
    throws StorageException, TransactionContextException {
    setModificationTimeForceNoPersistence(modificationTime);
    save();
  }
  
  final void cloneModificationTime(INodeWithAdditionalFields that)
    throws StorageException, TransactionContextException {
    this.modificationTime = that.modificationTime;
    save();
  }
  
  /**
   * Get access time of inode.
   *
   * @return access time
   */
  public final long getAccessTime() {
    return accessTime;
  }
  
  public final void setAccessTime(long accessTime)
    throws StorageException, TransactionContextException {
    setAccessTimeNoPersistance(accessTime);
    save();
  }
  /**
   * Get the {@link FsPermission}
   */
  public final FsPermission getFsPermission() {
    return permission;
  }
  
  /**
   * Set user
   */
  public void setUserNoPersistance(String user) throws IOException {
    this.userName = user;
  }
  
  /**
   * Set group
   */
  public void setGroupNoPersistance(String group) throws IOException {
    this.groupName = group;
  }
  
  public final long getHeader() {
    return header;
  }
  
  public final void setHeaderNoPersistance(long header) {
    this.header = header;
  }
  
  public final void setHasBlocksNoPersistance(boolean hasBlocks)
    throws StorageException, TransactionContextException {
    header = HeaderFormat.HAS_BLOCKS.BITS.combine((hasBlocks) ? 1 : 0, header);
  }
  
  public boolean hasBlocks() {
    return HeaderFormat.hasBlocks(header);
  }
  
  public final Long getPartitionId() {
    return partitionId;
  }
  
  public final void setPartitionIdNoPersistance(long partitionId) {
    this.partitionId = partitionId;
  }
  
  public final void setPartitionId(Long partitionId)
    throws StorageException, TransactionContextException {
    setPartitionIdNoPersistance(partitionId);
    save();
  }
  
  private int logicalTime;
  
  public void logMetadataEvent(INodeMetadataLogEntry.Operation operation)
      throws StorageException, TransactionContextException {
    if(isUnderConstruction()){
      return;
    }
  
    long datasetId;
    if(this instanceof INodeDirectory && ((INodeDirectory)this).isMetaEnabled()) {
      datasetId = getId();
    } else if(isPathMetaEnabled()) {
      datasetId = getMetaEnabledParent().getId();
    } else  {
      return;
    }
    if(getPartitionId() == null){
      throw new RuntimeException("Trying to log metadata for an inode that " +
        "wasn't commited to the database");
    }
    long inodeId = getId();
    int logicalTime = incrementLogicalTime();
    save();
  
    INodeMetadataLogEntry logEntry = new INodeMetadataLogEntry(datasetId, inodeId,
      getPartitionId(), getParentId(), getLocalName(), logicalTime, operation);
    EntityManager.add(logEntry);
  }
  
  final static String PROV_PROJECTS = "Projects";
  final static int PROV_PROJECT = 4;
  final static int PROV_DATASET = 3;
  final static int PROV_PARENT_P1 = 2;
  final static int PROV_PARENT_P2 = 1;
  final static int PROV_PARENT_DIRECT = 0;
  
  public INodeDirectory[] provenanceDirs(INode inode) {
    INodeDirectory[] provDirs = new INodeDirectory[]{null, null, null, null, null, null};
    LinkedList<INodeDirectory> aux = new LinkedList<>();
    
    try {
      if(isRoot()) {
        return provDirs;
      }
      INodeDirectory current = inode.getParent();
      provDirs[PROV_PARENT_DIRECT] = current;
      
      //save the top 4 parents (we need lvl 2,3 dirs) - not root(0) or Projects(1)
      while (!current.isRoot()) {
        current = current.getParent();
        aux.add(current);
        if(aux.size() > PROV_PROJECT + 2) {
          aux.removeFirst();
        }
      }
      if(aux.size() <= 1) {
        return provDirs;
      }
      
      //drop root
      aux.removeLast();
      
      if(aux.isEmpty() && PROV_PROJECTS.equals(provDirs[PROV_PARENT_DIRECT].getLocalName())) {
        //project
        return provDirs;
      }
      
      //check and drop Projects folder
      if (!PROV_PROJECTS.equals(aux.removeLast().getLocalName())) { 
        return provDirs;
      }
      if(aux.isEmpty()) {
        //dataset
        provDirs[PROV_PROJECT] = provDirs[PROV_PARENT_DIRECT];
        provDirs[PROV_DATASET] = (INodeDirectory)inode;
        return provDirs;
      }
      //check project as parent
      provDirs[PROV_PROJECT] = aux.removeLast();
      //check dataset as parent
      provDirs[PROV_DATASET] = aux.isEmpty() ? provDirs[PROV_PARENT_DIRECT] : aux.removeLast();
      provDirs[PROV_PARENT_P1] = aux.isEmpty() ? null : aux.removeLast();
      provDirs[PROV_PARENT_P2] = aux.isEmpty() ? null : aux.removeLast();
      return provDirs;
    } catch (IOException ex) {
      throw new RuntimeException("provenance log error3", ex);
    }
  }
  
  private boolean isProject(INodeDirectory[] parents) {
    return parents[PROV_PARENT_DIRECT] != null
      && PROV_PROJECTS.equals(parents[PROV_PARENT_DIRECT].getLocalName());
  }
  
  private boolean isDatasetLvl(INodeDirectory[] parents) {
    return parents[PROV_PARENT_DIRECT] != null && parents[PROV_PROJECT] != null
      && parents[PROV_PARENT_DIRECT].equals(parents[PROV_PROJECT])
      && isDirectory();
      //on create a dataset has not yet been meta enabled
      //&& ((INodeDirectory) this).isMetaEnabled();
  }

  private boolean partOfDataset(INodeDirectory[] parents) {
    return parents[PROV_DATASET] != null;
  }
  
  @Override
  public void logProvenanceEvent(FileProvenanceEntry.Operation op) {
    INodeDirectory[] parents = provenanceDirs(this);
//    if( isProject(parents) || isDatasetLvl(parents) || partOfDataset(parents)) {
    if(isDatasetLvl(parents) || partOfDataset(parents)) {
      switch(parents[PROV_DATASET].getMetaStatus()) {
        case DISABLED:
        case META_ENABLED:
          return;
        case MIN_PROV_ENABLED:
          switch(op) {
            case ACCESS_DATA:
            case MODIFY_DATA:
            case METADATA:
            case OTHER:
              return;
          }
      }
      //if we are here we are tracking
      Optional<XAttr> xattr = Optional.empty();
      logProvenanceEvent(parents, op, xattr);
    }
  }
  
  @Override
  public void logProvenanceEvent(FileProvenanceEntry.Operation op, XAttr xattr) {
    if(XAttr.NameSpace.PROVENANCE.equals(xattr.getNameSpace())) {
      INodeDirectory[] parents = provenanceDirs(this);
      if(isDatasetLvl(parents) || partOfDataset(parents)) {
        switch(parents[PROV_DATASET].getMetaStatus()) {
          case DISABLED:
          case META_ENABLED:
            return;
        }
        //if we are here we are tracking
        logProvenanceEvent(parents, op, Optional.of(xattr));
      }
    }
  }
  
  private void logProvenanceEvent(INodeDirectory[] parents, FileProvenanceEntry.Operation op, 
    Optional<XAttr> xattr) {
    UserGroupInformation ugi;
    int remoteUserId;
    String remoteUserName;
    try {
      ugi = NameNode.getRemoteUser();
      remoteUserId = UsersGroups.getUserID(ugi.getUserName());
      remoteUserName = ugi.getUserName();
    } catch (IOException ex) {
      throw new RuntimeException("provenance log error1", ex);
    }
    String appId = ugi.getApplicationId();
    if(appId == null) {
       appId = "notls";
    }
    
    long timestamp = System.currentTimeMillis();
    String p1Name = parents[PROV_PARENT_P1] != null ? parents[PROV_PARENT_P1].getLocalName() : "";
    String p2Name = parents[PROV_PARENT_P2] != null ? parents[PROV_PARENT_P2].getLocalName() : "";
    
    try {
      if (xattr.isPresent()) {
        byte xattrNamespace = xattr.get().getNameSpace().getId();
        String xattrName = xattr.get().getName();
        byte[] xattrValue = xattr.get().getValue();
        FileProvXAttrBufferEntry xattrEntry 
          = new FileProvXAttrBufferEntry(id, xattrNamespace, xattrName, logicalTime, xattrValue);
        EntityManager.add(xattrEntry);
      }
      String xattrName = xattr.isPresent() ? xattr.get().getName() : "";
  
      FileProvenanceEntry ple = new FileProvenanceEntry(id, op, logicalTime, timestamp, appId, remoteUserId,
        partitionId,
        parents[PROV_PROJECT].getId(), parents[PROV_DATASET].getId(), parents[PROV_PARENT_DIRECT].getId(),
        getLocalName(), parents[PROV_PROJECT].getLocalName(), parents[PROV_DATASET].getLocalName(),
        p1Name, p2Name, parents[PROV_PARENT_DIRECT].getLocalName(),
        remoteUserName, xattrName, logicalTime, timestamp, parents[PROV_DATASET].getLogicalTime());

      EntityManager.add(ple);
    } catch (IOException ex) {
      throw new RuntimeException("provenance log error3", ex);
    }
  }
  
  public final int getLogicalTime() {
    return logicalTime;
  }
  
  public final void setLogicalTimeNoPersistance(Integer logicalTime) {
    this.logicalTime = logicalTime;
  }
  
  public final int incrementLogicalTime(){
    return ++logicalTime;
  }
  
  /**
   * Set local file name
   */
  public final void setLocalNameNoPersistance(byte[] name) {
    this.name = name;
  }
  
  public void setLocalNameNoPersistance(String name) {
    this.name = DFSUtil.string2Bytes(name);
  }
  
  /**
   * Set the {@link FsPermission} of this {@link INode}
   */
  private void setPermissionNoPersistance(FsPermission permission) {
    this.permission = permission;
  }
  
  /**
   * Set last modification time of inode.
   */
  public void setModificationTimeNoPersistance(long modtime) {
    this.modificationTime = modtime;
  }
  
  /**
   * Always set the modification time of inode.
   */
  protected void setModificationTimeForceNoPersistence(long modificationTime) {
    this.modificationTime = modificationTime;
  }
  
  /**
   * Set last access time of inode.
   */
  public void setAccessTimeNoPersistance(long atime) {
    accessTime = atime;
  }
  
  @Override
  XAttrFeature getXAttrFeature() {
    return getFeature(XAttrFeature.class);
  }
  
  @Override
  public void removeXAttrFeature() {
    XAttrFeature f = getXAttrFeature();
    Preconditions.checkNotNull(f);
    removeFeature(f);
  }
  
  @Override
  public void addXAttrFeature(XAttrFeature f) {
    XAttrFeature f1 = getXAttrFeature();
    Preconditions.checkState(f1 == null, "Duplicated XAttrFeature");
    
    addFeature(f);
  }
  
  public void setUserID(int userId) throws IOException {
    setUserIDNoPersistence(userId);
    save();
  }

  public void setUserIDNoPersistence(int userId) {
    this.userId = userId;
  }

  public void setGroupID(int groupId) throws IOException {
    setGroupIDNoPersistence(groupId);
    save();
  }

  public void setGroupIDNoPersistence(int groupId) {
    this.groupId = groupId;
  }
  
  public void addFeature(Feature f) {
    int size = features.length;
    Feature[] arr = new Feature[size + 1];
    if (size != 0) {
      System.arraycopy(features, 0, arr, 0, size);
    }
    arr[size] = f;
    features = arr;
  }
  
  protected void removeFeature(Feature f) {
    int size = features.length;
    Preconditions.checkState(size > 0, "Feature "
      + f.getClass().getSimpleName() + " not found.");
    if (size == 1) {
      Preconditions.checkState(features[0] == f, "Feature "
        + f.getClass().getSimpleName() + " not found.");
      features = EMPTY_FEATURE;
      return;
    }
    
    Feature[] arr = new Feature[size - 1];
    int j = 0;
    boolean overflow = false;
    for (Feature f1 : features) {
      if (f1 != f) {
        if (j == size - 1) {
          overflow = true;
          break;
        } else {
          arr[j++] = f1;
        }
      }
    }
    
    Preconditions.checkState(!overflow && j == size - 1, "Feature "
      + f.getClass().getSimpleName() + " not found.");
    features = arr;
  }
  
  protected <T extends Feature> T getFeature(Class<? extends Feature> clazz) {
    for (Feature f : features) {
      if (f.getClass() == clazz) {
        @SuppressWarnings("unchecked")
        T ret = (T) f;
        return ret;
      }
    }
    return null;
  }
  
  private int getUserIDDB(String name) throws IOException {
    if(name == null){
      return 0;
    }
    int userID = 0;
    try{
      userID = UsersGroups.getUserID(name);
    } catch (UserNotFoundException e ){
      return 0;
    }
    return userID;
  }

  private int getGroupIDDB(String name) throws IOException {
    if(name == null){
      return 0;
    }
    int groupID = 0;
    try{
      groupID = UsersGroups.getGroupID(name);
    } catch (GroupNotFoundException e ){
      return 0;
    }
    return groupID;
  }
}
