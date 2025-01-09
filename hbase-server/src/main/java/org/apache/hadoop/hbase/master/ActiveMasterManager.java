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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZNodeClearer;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskGroup;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Handles everything on master-side related to master election. Keeps track of currently active
 * master and registered backup masters.
 * <p>
 * Listens and responds to ZooKeeper notifications on the master znodes, both
 * <code>nodeCreated</code> and <code>nodeDeleted</code>.
 * <p>
 * Contains blocking methods which will hold up backup masters, waiting for the active master to
 * fail.
 * <p>
 * This class is instantiated in the HMaster constructor and the method
 * #blockUntilBecomingActiveMaster() is called to wait until becoming the active master of the
 * cluster.
 */
@InterfaceAudience.Private
public class ActiveMasterManager extends ZKListener {
  private static final Logger LOG = LoggerFactory.getLogger(ActiveMasterManager.class);

  final AtomicBoolean clusterHasActiveMaster = new AtomicBoolean(false);
  final AtomicBoolean clusterShutDown = new AtomicBoolean(false);

  // This server's information. Package-private for child implementations.
  int infoPort;
  final ServerName sn;
  final Server master;

  // Active master's server name. Invalidated anytime active master changes (based on ZK
  // notifications) and lazily fetched on-demand.
  // ServerName is immutable, so we don't need heavy synchronization around it.
  volatile ServerName activeMasterServerName;
  // Registered backup masters. List is kept up to date based on ZK change notifications to
  // backup znode.
  private volatile ImmutableList<ServerName> backupMasters;

  /**
   * @param watcher ZK watcher
   * @param sn      ServerName
   * @param master  In an instance of a Master.
   */
  ActiveMasterManager(ZKWatcher watcher, ServerName sn, Server master)
    throws InterruptedIOException {
    super(watcher);
    watcher.registerListener(this);
    this.sn = sn;
    this.master = master;
    // KLRD: 从ZK获取所有backup master信息
    updateBackupMasters();
  }

  // will be set after jetty server is started
  public void setInfoPort(int infoPort) {
    this.infoPort = infoPort;
  }

  @Override
  public void nodeCreated(String path) {
    handle(path);
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(watcher.getZNodePaths().backupMasterAddressesZNode)) {
      try {
        updateBackupMasters();
      } catch (InterruptedIOException ioe) {
        LOG.error("Error updating backup masters", ioe);
      }
    }
  }

  @Override
  public void nodeDeleted(String path) {
    // We need to keep track of the cluster's shutdown status while
    // we wait on the current master. We consider that, if the cluster
    // was already in a "shutdown" state when we started, that this master
    // is part of a new cluster that was started shortly after the old cluster
    // shut down, so that state is now irrelevant. This means that the shutdown
    // state must be set while we wait on the active master in order
    // to shutdown this master. See HBASE-8519.
    if (path.equals(watcher.getZNodePaths().clusterStateZNode) && !master.isStopped()) {
      clusterShutDown.set(true);
    }
    handle(path);
  }

  void handle(final String path) {
    if (path.equals(watcher.getZNodePaths().masterAddressZNode) && !master.isStopped()) {
      handleMasterNodeChange();
    }
  }

  private void updateBackupMasters() throws InterruptedIOException {
    /**
     * KLRD:
     *  1.HBase集群关于master的active和backup的分配
     *  2.ZK上有两个znode：
     *    /hbase/master         状态为active的HMaster
     *    /hbase/backup-master  状态为backup的HMaster
     *  3.在$HBase_HOME/conf/backup-masters中指定的就是backup master
     *  4.一般不需要指定哪个机器是active master，
     *    在哪个机器上执行start-hbase.sh，那么这个节点就是active master
     *  5.即使是active master，启动时也会到/hbase/backup-master下创建一个子znode代表自己
     *    然后再创建名为/hbase/master的znode，将自己信息写入该znode中
     *    如果成功自己信息写入/hbase/master，那么表示自己成为active master，
     *    然后再从/hbase/backup-masters将自己的znode删除
     */
    backupMasters =
      ImmutableList.copyOf(MasterAddressTracker.getBackupMastersAndRenewWatch(watcher));
  }

  /**
   * Fetches the active master's ServerName from zookeeper.
   */
  private void fetchAndSetActiveMasterServerName() {
    LOG.debug("Attempting to fetch active master sn from zk");
    try {
      activeMasterServerName = MasterAddressTracker.getMasterAddress(watcher);
    } catch (IOException | KeeperException e) {
      // Log and ignore for now and re-fetch later if needed.
      LOG.error("Error fetching active master information", e);
    }
  }

  public Optional<ServerName> getActiveMasterServerName() {
    if (!clusterHasActiveMaster.get()) {
      return Optional.empty();
    }
    if (activeMasterServerName == null) {
      fetchAndSetActiveMasterServerName();
    }
    // It could still be null, but return whatever we have.
    return Optional.ofNullable(activeMasterServerName);
  }

  public int getActiveMasterInfoPort() {
    try {
      return MasterAddressTracker.getMasterInfoPort(watcher);
    } catch (Exception e) {
      LOG.warn("Failed to get active master's info port.", e);
      return 0;
    }
  }

  public int getBackupMasterInfoPort(final ServerName sn) {
    try {
      return MasterAddressTracker.getBackupMasterInfoPort(watcher, sn);
    } catch (Exception e) {
      LOG.warn("Failed to get backup master: " + sn + "'s info port.", e);
      return 0;
    }
  }

  /**
   * Handle a change in the master node. Doesn't matter whether this was called from a nodeCreated
   * or nodeDeleted event because there are no guarantees that the current state of the master node
   * matches the event at the time of our next ZK request.
   * <p>
   * Uses the watchAndCheckExists method which watches the master address node regardless of whether
   * it exists or not. If it does exist (there is an active master), it returns true. Otherwise it
   * returns false.
   * <p>
   * A watcher is set which guarantees that this method will get called again if there is another
   * change in the master node.
   */
  private void handleMasterNodeChange() {
    // Watch the node and check if it exists.
    try {
      synchronized (clusterHasActiveMaster) {
        if (ZKUtil.watchAndCheckExists(watcher, watcher.getZNodePaths().masterAddressZNode)) {
          // A master node exists, there is an active master
          LOG.trace("A master is now available");
          clusterHasActiveMaster.set(true);
        } else {
          // Node is no longer there, cluster does not have an active master
          LOG.debug("No master available. Notifying waiting threads");
          clusterHasActiveMaster.set(false);
          // Notify any thread waiting to become the active master
          clusterHasActiveMaster.notifyAll();
        }
        // Reset the active master sn. Will be re-fetched later if needed.
        // We don't want to make a synchronous RPC under a monitor.
        activeMasterServerName = null;
      }
    } catch (KeeperException ke) {
      master.abort("Received an unexpected KeeperException, aborting", ke);
    }
  }

  /**
   * Block until becoming the active master. Method blocks until there is not another active master
   * and our attempt to become the new active master is successful. This also makes sure that we are
   * watching the master znode so will be notified if another master dies.
   * @param checkInterval    the interval to check if the master is stopped
   * @param startupTaskGroup the task group for master startup to track the progress
   * @return True if no issue becoming active master else false if another master was running or if
   *         some other problem (zookeeper, stop flag has been set on this Master)
   */
  boolean blockUntilBecomingActiveMaster(int checkInterval, TaskGroup startupTaskGroup) {
    MonitoredTask blockUntilActive =
      startupTaskGroup.addTask("Blocking until becoming active master");
    // KLRD: 自己的backup znode，如果自己成功竞选为Active，需要将这个Backup znode删除
    String backupZNode = ZNodePaths
      .joinZNode(this.watcher.getZNodePaths().backupMasterAddressesZNode, this.sn.toString());
    // KLRD: 一直尝试注册成为Active Master
    while (!(master.isAborted() || master.isStopped())) {
      blockUntilActive.setStatus("Trying to register in ZK as active master");
      // Try to become the active master, watch if there is another master.
      // Write out our ServerName as versioned bytes.
      try {
        // KLRD 如果当前机器成功创建/hbase/master znode，则表示该机器成为active master
        if (
          MasterAddressTracker.setMasterAddress(this.watcher,
            this.watcher.getZNodePaths().masterAddressZNode, this.sn, infoPort)
        ) {

          // If we were a backup master before, delete our ZNode from the backup
          // master directory since we are the active now)
          // KLRD: 竞选active成功，删除代表自己的backup znode
          if (ZKUtil.checkExists(this.watcher, backupZNode) != -1) {
            LOG.info("Deleting ZNode for " + backupZNode + " from backup master directory");
            ZKUtil.deleteNodeFailSilent(this.watcher, backupZNode);
          }
          // Save the znode in a file, this will allow to check if we crash in the launch scripts
          // KLRD: 将信息持久化到磁盘，通过环境变量：HBASE_ZNODE_FILE设置
          ZNodeClearer.writeMyEphemeralNodeOnDisk(this.sn.toString());

          // We are the master, return
          blockUntilActive.setStatus("Successfully registered as active master.");
          this.clusterHasActiveMaster.set(true); // KLRD 当前集群已有active master
          activeMasterServerName = sn;
          LOG.info("Registered as active master=" + this.sn);
          return true;
        }

        // KLRD: 上面逻辑是分布式竞争选举active master
        //       下面逻辑是自己没有成为active，则获取当前active master，并注册监听，等待重新竞选

        // Invalidate the active master name so that subsequent requests do not get any stale
        // master information. Will be re-fetched if needed.
        activeMasterServerName = null;
        // There is another active master running elsewhere or this is a restart
        // and the master ephemeral node has not expired yet.
        this.clusterHasActiveMaster.set(true);

        String msg;
        byte[] bytes =
          ZKUtil.getDataAndWatch(this.watcher, this.watcher.getZNodePaths().masterAddressZNode);
        if (bytes == null) {
          msg = ("A master was detected, but went down before its address "
            + "could be read.  Attempting to become the next active master");
        } else {
          ServerName currentMaster;
          try {
            currentMaster = ProtobufUtil.parseServerNameFrom(bytes);
          } catch (DeserializationException e) {
            LOG.warn("Failed parse", e);
            // Hopefully next time around we won't fail the parse. Dangerous.
            continue;
          }
          // KLRD: 若从ZK获取的当前Active Master是自己（自己是backup），这是一种异常情况
          //  从ZK删除当前active znode（/hbase/master）相关信息，让集群重新竞选active Master
          if (ServerName.isSameAddress(currentMaster, this.sn)) {
            msg = ("Current master has this master's address, " + currentMaster
              + "; master was restarted? Deleting node.");
            // Hurry along the expiration of the znode.
            ZKUtil.deleteNode(this.watcher, this.watcher.getZNodePaths().masterAddressZNode);

            // We may have failed to delete the znode at the previous step, but
            // we delete the file anyway: a second attempt to delete the znode is likely to fail
            // again.
            ZNodeClearer.deleteMyEphemeralNodeOnDisk();
          } else {
            msg = "Another master is the active master, " + currentMaster
              + "; waiting to become the next active master";
          }
        }
        LOG.info(msg);
        blockUntilActive.setStatus(msg);
      } catch (KeeperException ke) {
        master.abort("Received an unexpected KeeperException, aborting", ke);
        return false;
      }
      synchronized (this.clusterHasActiveMaster) {
        while (clusterHasActiveMaster.get() && !master.isStopped()) {
          try {
            clusterHasActiveMaster.wait(checkInterval);
          } catch (InterruptedException e) {
            // We expect to be interrupted when a master dies,
            // will fall out if so
            LOG.debug("Interrupted waiting for master to die", e);
          }
        }
        if (clusterShutDown.get()) {
          this.master.stop("Cluster went down before this master became active");
        }
      }
    }
    return false;
  }

  /** Returns True if cluster has an active master. */
  boolean hasActiveMaster() {
    try {
      if (ZKUtil.checkExists(watcher, watcher.getZNodePaths().masterAddressZNode) >= 0) {
        return true;
      }
    } catch (KeeperException ke) {
      LOG.info("Received an unexpected KeeperException when checking " + "isActiveMaster : " + ke);
    }
    return false;
  }

  public void stop() {
    try {
      synchronized (clusterHasActiveMaster) {
        // Master is already stopped, wake up the manager
        // thread so that it can shutdown soon.
        clusterHasActiveMaster.notifyAll();
      }
      // If our address is in ZK, delete it on our way out
      ServerName activeMaster = null;
      try {
        activeMaster = MasterAddressTracker.getMasterAddress(this.watcher);
      } catch (IOException e) {
        LOG.warn("Failed get of master address: " + e.toString());
      }
      if (activeMaster != null && activeMaster.equals(this.sn)) {
        ZKUtil.deleteNode(watcher, watcher.getZNodePaths().masterAddressZNode);
        // We may have failed to delete the znode at the previous step, but
        // we delete the file anyway: a second attempt to delete the znode is likely to fail again.
        ZNodeClearer.deleteMyEphemeralNodeOnDisk();
      }
    } catch (KeeperException e) {
      LOG.debug(this.watcher.prefix("Failed delete of our master address node; " + e.getMessage()));
    }
  }

  /** Returns list of registered backup masters. */
  public List<ServerName> getBackupMasters() {
    return backupMasters;
  }
}
